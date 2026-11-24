"""共享维度数据处理：事件属性取值、维度分组与聚合度量计算。

实时窗口（``app.pipeline.compute_aggregation``）与 Cube 预计算
（``app.pipeline.precompute_cube``）过去各自把「事件属性解析 → 按维度取值 →
拼展示键分组 → 聚合度量」写了一遍，两边对缺键、问号和零值的处理只靠隐式
约定保持一致。本模块把这套维度数据处理收拢为唯一实现，两处计算复用同一套
代码，保证对同一条事件读出的维度值、分组键和问号/缺键判定完全相同。

设计要点：

- :func:`parse_event_attrs` 只解析事件自身携带的 ``event_attrs`` 字段，
  解析失败按既有口径回退为空属性，并保留解析错误说明；
- :func:`extract_dimension_values` 区分三种取值状态：
  ``present``（字段存在且读到值，数字 0 也属于 present）、
  ``missing``（事件属性里根本没有该键）、
  ``question``（字段存在但值确实是问号 "?"）。
  缺键与显式问号在分组展示键上都按历史口径渲染成 "?"，但核对证据分别记录，
  不会把输出展示键反过来当成分组证据；
- :func:`group_events_by_dimensions` 在共享读取阶段一次性整理维度并分组，
  返回每个事件实际被读取的字段和所在分组，供审计落库；
- :func:`aggregate_measure` 承载 COUNT/SUM/AVG/MAX/MIN/COUNT_DISTINCT 等
  度量计算，异常回退口径与历史实现保持一致。
"""
from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from typing import Any

# 缺键与显式问号在展示键中的统一渲染值（历史口径，不可变更）
DISPLAY_QUESTION = "?"
# 无维度窗口的固定分组键
NO_DIMENSION_KEY = "__all__"


@dataclass
class DimensionFieldRead:
    """单个维度字段在一条事件上的实际读取证据。"""

    dimension: str
    status: str  # present / missing / question
    raw_value: Any = None
    display_value: str = DISPLAY_QUESTION


@dataclass
class EventDimensionRead:
    """一条事件在一次维度计算中的读取明细。"""

    event_id: int | None
    event_name: str
    batch_id: str | None
    group_key: str
    field_reads: list[DimensionFieldRead] = field(default_factory=list)
    parse_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_name": self.event_name,
            "batch_id": self.batch_id,
            "group_key": self.group_key,
            "parse_error": self.parse_error,
            "field_reads": [
                {
                    "dimension": fr.dimension,
                    "status": fr.status,
                    "raw_value": fr.raw_value,
                    "display_value": fr.display_value,
                }
                for fr in self.field_reads
            ],
        }


@dataclass
class DimensionGrouping:
    """一次共享维度整理的产物：分组事件 + 每条事件的读取证据。"""

    dimensions: list[str]
    groups: dict[str, list[dict[str, Any]]]
    reads: list[EventDimensionRead]
    attrs_by_event_id: dict[int, dict[str, Any]]
    parse_errors: dict[int, str]


def parse_dimensions(raw: Any) -> list[str]:
    """解析窗口/Cube 配置里的维度组合，兼容 JSON 字符串与已解析列表。"""
    try:
        dims = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(dims, list):
        return []
    return [str(d) for d in dims]


def parse_event_attrs(event: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    """解析单条事件的 ``event_attrs``。

    返回（属性字典, 解析错误说明）。解析失败或属性不是 JSON 对象时按历史
    口径回退为空字典，错误说明仅用于审计，不影响分组展示键（仍渲染为 "?"）。
    """
    raw = event.get("event_attrs", "{}")
    if isinstance(raw, dict):
        return raw, None
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError) as exc:
        return {}, f"event_attrs 解析失败: {exc}"
    if not isinstance(parsed, dict):
        return {}, "event_attrs 不是 JSON 对象"
    return parsed, None


def extract_dimension_values(
    attrs: dict[str, Any], dimensions: list[str],
) -> list[DimensionFieldRead]:
    """按维度顺序从事件属性中取值，并标注每个字段的实际读取状态。

    与历史口径一致：分组展示值统一走 ``str(...)``，缺键渲染为 "?"。
    区别在于缺键（missing）与显式问号（question）分别记录证据，
    数字 0 等假值不会被误判为缺键。
    """
    reads: list[DimensionFieldRead] = []
    for dim in dimensions:
        if dim in attrs:
            raw_value = attrs[dim]
            if raw_value == DISPLAY_QUESTION:
                reads.append(DimensionFieldRead(
                    dimension=dim, status="question",
                    raw_value=raw_value, display_value=DISPLAY_QUESTION,
                ))
            else:
                reads.append(DimensionFieldRead(
                    dimension=dim, status="present",
                    raw_value=raw_value, display_value=str(raw_value),
                ))
        else:
            reads.append(DimensionFieldRead(
                dimension=dim, status="missing",
                raw_value=None, display_value=DISPLAY_QUESTION,
            ))
    return reads


def build_group_key(field_reads: list[DimensionFieldRead]) -> str:
    """按维度顺序拼接分组展示键，与历史 ``"|".join(...)`` 口径一致。"""
    return "|".join(fr.display_value for fr in field_reads)


def group_events_by_dimensions(
    events: list[dict[str, Any]], dimensions: list[str],
) -> DimensionGrouping:
    """共享读取阶段：解析属性、逐事件取维度值并完成分组。

    实时窗口与 Cube 都通过本函数整理维度，确保同一条事件在两处得到完全
    相同的分组键。返回的 :class:`EventDimensionRead` 列表记录每条事件实际
    被读取的字段、读取状态、所属分组和批次，可直接落库用于核对。
    """
    groups: dict[str, list[dict[str, Any]]] = {}
    reads: list[EventDimensionRead] = []
    attrs_by_event_id: dict[int, dict[str, Any]] = {}
    parse_errors: dict[int, str] = {}

    if not dimensions:
        # 无维度：保持历史固定分组键，不产生字段读取证据
        groups[NO_DIMENSION_KEY] = list(events)
        for evt in events:
            attrs, parse_error = parse_event_attrs(evt)
            event_id = evt.get("id")
            if event_id is not None:
                attrs_by_event_id[event_id] = attrs
                if parse_error:
                    parse_errors[event_id] = parse_error
            reads.append(EventDimensionRead(
                event_id=event_id,
                event_name=evt.get("event_name", ""),
                batch_id=evt.get("batch_id"),
                group_key=NO_DIMENSION_KEY,
                field_reads=[],
                parse_error=parse_error,
            ))
        return DimensionGrouping(
            dimensions=[], groups=groups, reads=reads,
            attrs_by_event_id=attrs_by_event_id, parse_errors=parse_errors,
        )

    for evt in events:
        attrs, parse_error = parse_event_attrs(evt)
        event_id = evt.get("id")
        if event_id is not None:
            attrs_by_event_id[event_id] = attrs
            if parse_error:
                parse_errors[event_id] = parse_error
        field_reads = extract_dimension_values(attrs, dimensions)
        group_key = build_group_key(field_reads)
        groups.setdefault(group_key, []).append(evt)
        reads.append(EventDimensionRead(
            event_id=event_id,
            event_name=evt.get("event_name", ""),
            batch_id=evt.get("batch_id"),
            group_key=group_key,
            field_reads=field_reads,
            parse_error=parse_error,
        ))

    return DimensionGrouping(
        dimensions=list(dimensions), groups=groups, reads=reads,
        attrs_by_event_id=attrs_by_event_id, parse_errors=parse_errors,
    )


def summarize_group_evidence(
    reads: list[EventDimensionRead], dimensions: list[str],
) -> dict[str, dict[str, Any]]:
    """汇总每个分组下各维度的读取证据，供计算审计展示。

    输出只来自事件字段的实际读取记录，不读取输出展示键，因此可以核对
    「没有键的记录（missing）」与「确实填了问号的记录（question）」。
    """
    summary: dict[str, dict[str, Any]] = {}
    for read in reads:
        group = summary.setdefault(read.group_key, {
            "group_key": read.group_key,
            "event_count": 0,
            "dimensions": {dim: {"present": [], "missing": [], "question": []}
                           for dim in dimensions},
        })
        group["event_count"] += 1
        for fr in read.field_reads:
            bucket = group["dimensions"][fr.dimension][fr.status]
            if read.event_id is not None and read.event_id not in bucket:
                bucket.append(read.event_id)
    return summary


def _measure_attrs(event: dict[str, Any]) -> Any:
    """度量计算用的属性读取：与历史实现保持同构。

    历史 SUM/AVG/MAX/MIN/COUNT_DISTINCT 在度量阶段对每条事件重新
    ``json.loads(event_attrs)``，坏 JSON 会抛异常并被外层捕获后回退为
    COUNT。这里保留同样的抛错行为；分组用的容错解析见
    :func:`parse_event_attrs`，两条路径互不影响历史口径。
    """
    raw = event.get("event_attrs", "{}")
    if isinstance(raw, str):
        return json.loads(raw)
    return raw if raw is not None else {}


def aggregate_measure(
    group_events: list[dict[str, Any]],
    agg_func: str,
    attrs_by_event_id: dict[int, dict[str, Any]] | None = None,
) -> float:
    """对一个分组内的事件执行度量聚合。

    异常回退口径与历史实现完全一致：SUM/AVG/MAX/MIN/COUNT_DISTINCT 任一
    事件取值失败（含坏 JSON、amount 不可转 float）时回退为分组事件数 COUNT。
    ``attrs_by_event_id`` 仅为兼容旧签名保留，度量取值仍按历史方式从事件
    原始 event_attrs 读取，保证数值与重构前逐位一致。
    """
    func = (agg_func or "COUNT").upper().strip()

    if func == "COUNT":
        return float(len(group_events))

    if func in ("SUM", "总金额", "总量"):
        try:
            return sum(float(_measure_attrs(e).get("amount", 0)) for e in group_events)
        except Exception:
            return float(len(group_events))

    if func in ("AVG", "均值"):
        try:
            vals = [float(_measure_attrs(e).get("amount", 0)) for e in group_events]
            return statistics.mean(vals) if vals else 0.0
        except Exception:
            return float(len(group_events))

    if func in ("MAX", "最大值"):
        try:
            vals = [float(_measure_attrs(e).get("amount", 0)) for e in group_events]
            return max(vals) if vals else 0.0
        except Exception:
            return float(len(group_events))

    if func in ("MIN", "最小值"):
        try:
            vals = [float(_measure_attrs(e).get("amount", 0)) for e in group_events]
            return min(vals) if vals else 0.0
        except Exception:
            return float(len(group_events))

    if func == "COUNT_DISTINCT":
        try:
            vals = {_measure_attrs(e).get("user_id", "") for e in group_events}
            return float(len(vals))
        except Exception:
            return float(len(group_events))

    return float(len(group_events))


def cube_dimension_values(
    group_events: list[dict[str, Any]],
    dimensions: list[str],
    attrs_by_event_id: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    """构造 Cube cell 的 dimension_values（取分组首条事件的原始维度值）。

    与历史实现一致取分组内第一条事件的值；缺键/解析失败回退为 "?"。
    值来源仍是事件属性本身，而非输出展示键。
    """
    first = group_events[0]
    attrs = attrs_by_event_id.get(first.get("id"), {})
    if not attrs:
        attrs, _ = parse_event_attrs(first)
    return {dim: attrs.get(dim, DISPLAY_QUESTION) for dim in dimensions}
