"""维度计算审计：记录实时窗口与 Cube 每次新计算实际读取的维度数据。

两处计算（``compute_aggregation`` / ``precompute_cube``）在共享读取阶段
整理维度时，会把每条事件实际读取了哪些字段、字段状态（present/missing/
question）、归属分组和批次落到这里。核对时只能依据这些源事件读取记录，
不能把输出里的展示键（dimension_key / dimension_values）反过来当成分组
证据。

表结构（DDL 见 app.database.DDL_STATEMENTS）：

- ``dimension_compute_runs``：一次计算一行，记录入口（实时窗口/Cube）、
  配置快照、实际读取的事件批次与事件范围；
- ``dimension_event_reads``：一次计算中每条事件一行（无维度计算不落字段
  证据，只落运行头），记录逐字段读取状态与分组归属。

已有的聚合结果（aggregation_results / cube_cells）显示与数值照常从原表
读取；只有能关联到运行头的结果才提供来源核对，关联不到的旧组保留原值并
标注「来源无法确认」。
"""
from __future__ import annotations

import json
from typing import Any, Sequence

from app.database import db

# 计算入口标识
ENGINE_REALTIME_WINDOW = "realtime_window"
ENGINE_CUBE = "cube"

# 旧结果没有对应的新计算审计运行
PROVENANCE_UNCONFIRMED = "source_unconfirmed"
PROVENANCE_CONFIRMED = "source_confirmed"


def record_compute_run(
    *, engine: str, owner_id: int, dimensions: Sequence[str], agg_func: str,
    event_ids: Sequence[int], batch_ids: Sequence[str | None],
    event_query: str, event_params: Sequence[Any],
    group_evidence: dict[str, dict[str, Any]],
    window_start: str | None = None, window_end: str | None = None,
    extra: dict[str, Any] | None = None,
) -> int:
    """落一次维度计算运行头，返回 run id。"""
    payload: dict[str, Any] = {
        "engine": engine,
        "owner_id": owner_id,
        "dimensions_json": json.dumps(list(dimensions), ensure_ascii=False),
        "agg_function": agg_func,
        "window_start": window_start,
        "window_end": window_end,
        "event_count": len(event_ids),
        "event_ids_json": json.dumps(list(event_ids), ensure_ascii=False),
        "batch_ids_json": json.dumps(
            sorted({b for b in batch_ids if b}), ensure_ascii=False),
        "event_query": event_query,
        "event_params_json": json.dumps(list(event_params), ensure_ascii=False),
        "group_evidence_json": json.dumps(group_evidence, ensure_ascii=False),
        "extra_json": json.dumps(extra or {}, ensure_ascii=False),
    }
    return db.insert("dimension_compute_runs", payload)


def record_event_reads(run_id: int, reads: list[dict[str, Any]]) -> int:
    """落一次计算中每条事件的维度读取明细。"""
    rows = [
        (
            run_id,
            read["event_id"],
            read["event_name"],
            read["batch_id"],
            read["group_key"],
            json.dumps(read["field_reads"], ensure_ascii=False),
            read.get("parse_error"),
        )
        for read in reads
    ]
    if not rows:
        return 0
    db.executemany(
        "INSERT INTO dimension_event_reads"
        "(run_id,event_id,event_name,batch_id,group_key,field_reads_json,parse_error) "
        "VALUES(?,?,?,?,?,?,?)",
        rows,
    )
    return len(rows)


def get_run(run_id: int) -> dict[str, Any] | None:
    return db.fetchone(
        "SELECT * FROM dimension_compute_runs WHERE id=?", [run_id])


def latest_run(engine: str, owner_id: int) -> dict[str, Any] | None:
    return db.fetchone(
        "SELECT * FROM dimension_compute_runs WHERE engine=? AND owner_id=? "
        "ORDER BY id DESC LIMIT 1",
        [engine, owner_id],
    )


def list_runs(
    engine: str, owner_id: int, limit: int = 20,
) -> list[dict[str, Any]]:
    return db.fetchall(
        "SELECT * FROM dimension_compute_runs WHERE engine=? AND owner_id=? "
        "ORDER BY id DESC LIMIT ?",
        [engine, owner_id, limit],
    )


def list_event_reads(run_id: int) -> list[dict[str, Any]]:
    return db.fetchall(
        "SELECT * FROM dimension_event_reads WHERE run_id=? ORDER BY id",
        [run_id],
    )


def _decode_run(run: dict[str, Any]) -> dict[str, Any]:
    decoded = dict(run)
    for key in ("dimensions_json", "event_ids_json", "batch_ids_json",
                "event_params_json", "group_evidence_json", "extra_json"):
        try:
            decoded[key.removesuffix("_json")] = json.loads(run.get(key) or "null")
        except (json.JSONDecodeError, TypeError):
            decoded[key.removesuffix("_json")] = None
    return decoded


def run_detail(run_id: int) -> dict[str, Any] | None:
    """运行头 + 逐事件读取明细，供核对实际读取的字段与事件。"""
    run = get_run(run_id)
    if run is None:
        return None
    decoded = _decode_run(run)
    reads = list_event_reads(run_id)
    for read in reads:
        try:
            read["field_reads"] = json.loads(read.get("field_reads_json") or "[]")
        except (json.JSONDecodeError, TypeError):
            read["field_reads"] = []
    decoded["event_reads"] = reads
    return decoded


def provenance_for_group(
    engine: str, owner_id: int, group_key: str,
) -> dict[str, Any]:
    """为一条已存结果（展示键）核对来源。

    只在最近一次同入口计算的源事件读取证据里查找该分组，明确区分缺键
    （missing）与显式问号（question）。找不到对应运行时保留原值并标注来源
    无法确认，绝不根据展示键内容反推成员。
    """
    run = latest_run(engine, owner_id)
    if run is None:
        return {
            "provenance": PROVENANCE_UNCONFIRMED,
            "group_key": group_key,
            "message": "该组为旧计算结果，仅保留展示键，无新计算审计记录，成员来源无法确认",
            "run_id": None,
        }

    decoded = _decode_run(run)
    evidence = (decoded.get("group_evidence") or {}).get(group_key)
    if evidence is None:
        return {
            "provenance": PROVENANCE_UNCONFIRMED,
            "group_key": group_key,
            "run_id": run["id"],
            "message": "最近一次新计算未产出该展示键分组，成员来源无法确认（展示键不作为分组证据）",
        }

    missing_events: list[Any] = []
    question_events: list[Any] = []
    for dim, buckets in (evidence.get("dimensions") or {}).items():
        missing_events.extend(buckets.get("missing", []))
        question_events.extend(buckets.get("question", []))

    return {
        "provenance": PROVENANCE_CONFIRMED,
        "group_key": group_key,
        "run_id": run["id"],
        "engine": engine,
        "dimensions": decoded.get("dimensions") or [],
        "event_count": evidence.get("event_count", 0),
        "missing_key_event_ids": sorted(set(missing_events)),
        "explicit_question_event_ids": sorted(set(question_events)),
        "batch_ids": decoded.get("batch_ids") or [],
        "event_query": run.get("event_query"),
        "message": "来源已按源事件字段读取记录核对，未使用输出展示键反推",
    }
