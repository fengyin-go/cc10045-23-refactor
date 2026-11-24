"""多维数据集构建领域服务：定义多维Cube并执行预计算。

真实数据流：定义Cube（事实表/维度组合/指标计算）
→ pipeline.precompute_cube 从 events 表按维度组合聚合
→ 写入 cube_cells → 下游仪表盘查询加速。
"""
from __future__ import annotations

import json
from typing import Any

from app import audit
from app import dimensions as dimlib
from app.database import db
from app.pipeline import precompute_cube
from app.services.base import BaseModuleService


class CubeBuilderService(BaseModuleService):
    MODULE = "cube_builder"
    ENTITY = "Cube定义"
    OPERATION_TERM = "预计算"
    LIST_FIELDS = ["Cube名称", "事实表", "维度组合", "指标计算", "预计算状态"]
    REQUIRED_FIELDS = ["Cube名称", "事实表", "维度组合", "指标计算"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用Cube": "已启用", "停用Cube": "已停用"}
    NEGATIVE_ACTIONS = []

    def run_precomputation(self, cube_id: int) -> dict[str, Any]:
        """执行Cube预计算。"""
        cube = db.fetchone("SELECT * FROM cubes WHERE id=?", [cube_id])
        if cube is None:
            return {"ok": False, "message": f"Cube {cube_id} 不存在"}

        db.update("cubes", {"precomputation_status": "计算中"}, "id=?", [cube_id])
        cell_count = precompute_cube(dict(cube))

        return {
            "ok": True,
            "cube_name": cube["cube_name"],
            "cells_computed": cell_count,
            "status": "已启用",
        }

    def cube_cells(self, cube_id: int, limit: int = 100) -> list[dict[str, Any]]:
        """获取Cube预计算结果。"""
        rows = db.fetchall(
            "SELECT * FROM cube_cells WHERE cube_id=? ORDER BY measure_value DESC LIMIT ?",
            [cube_id, limit],
        )
        for r in rows:
            try:
                r["dimension_values"] = json.loads(r["dimension_values"]) if isinstance(
                    r["dimension_values"], str) else r["dimension_values"]
            except json.JSONDecodeError:
                pass
        return rows

    # ================================================================
    # 维度来源核对（新计算审计；旧 cell 保留原值并标注来源无法确认）
    # ================================================================

    def list_dimension_runs(self, cube_id: int, limit: int = 20) -> list[dict[str, Any]]:
        """列出 Cube 历次预计算的维度读取运行（实际读取的批次/事件范围）。"""
        return audit.list_runs(audit.ENGINE_CUBE, cube_id, limit)

    def dimension_run_detail(self, run_id: int) -> dict[str, Any] | None:
        """单次 Cube 预计算的字段读取明细：实际读了哪些事件、哪些维度字段。"""
        return audit.run_detail(run_id)

    def cell_provenance(self, cube_id: int, dimension_key: str) -> dict[str, Any]:
        """核对一个已存 Cube cell（展示键）的来源。

        dimension_key 可传拼接展示键（如 ``华东|?``）或 dimension_values
        的 JSON 串。核对只走新计算的源事件读取证据，不把展示键反推为分组
        依据；旧 cell 只有展示键时保留原值并标注来源无法确认。
        """
        stored_cell = self._find_cell(cube_id, dimension_key)
        lookup_key = dimension_key
        if stored_cell is not None:
            dims = self._cube_dimensions(cube_id)
            raw_vals = stored_cell.get("dimension_values")
            if isinstance(raw_vals, str):
                try:
                    raw_vals = json.loads(raw_vals)
                except json.JSONDecodeError:
                    raw_vals = {}
            if dims and isinstance(raw_vals, dict):
                # 与共享分组口径一致：缺键渲染为 "?"
                lookup_key = "|".join(
                    str(raw_vals.get(d, "?")) for d in dims)

        provenance = audit.provenance_for_group(
            audit.ENGINE_CUBE, cube_id, lookup_key)
        provenance["requested_key"] = dimension_key
        provenance["stored_cell"] = stored_cell
        if stored_cell is not None:
            provenance["stored_value"] = stored_cell.get("measure_value")
        return provenance

    def _find_cell(self, cube_id: int, dimension_key: str) -> dict[str, Any] | None:
        cells = db.fetchall("SELECT * FROM cube_cells WHERE cube_id=?", [cube_id])
        for cell in cells:
            raw = cell.get("dimension_values")
            parsed = raw
            if isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    parsed = {}
            if raw == dimension_key or json.dumps(parsed, ensure_ascii=False) == dimension_key:
                cell["dimension_values"] = parsed
                return cell
            dims = self._cube_dimensions(cube_id)
            if dims and isinstance(parsed, dict):
                joined = "|".join(str(parsed.get(d, "?")) for d in dims)
                if joined == dimension_key:
                    cell["dimension_values"] = parsed
                    return cell
        return None

    def _cube_dimensions(self, cube_id: int) -> list[str]:
        cube = db.fetchone("SELECT dimensions FROM cubes WHERE id=?", [cube_id])
        if cube is None:
            return []
        return dimlib.parse_dimensions(cube.get("dimensions", "[]"))

    def aggregate_by_dimension(self, cube_id: int, dimension: str) -> list[dict[str, Any]]:
        """按指定维度汇总Cube数据。"""
        rows = db.fetchall(
            "SELECT dimension_values, SUM(measure_value) as total FROM cube_cells WHERE cube_id=? GROUP BY 1",
            [cube_id],
        )
        result: list[dict[str, Any]] = []
        for r in rows:
            try:
                dims = json.loads(r["dimension_values"]) if isinstance(r["dimension_values"], str) else {}
                val = dims.get(dimension, "unknown")
            except (json.JSONDecodeError, TypeError):
                val = "parse_error"
            result.append({"dimension_value": val, "total": r["total"]})
        return sorted(result, key=lambda x: -x["total"])

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_cubes"] = db.count("cubes")
        snapshot["total_cells"] = db.count("cube_cells")
        snapshot["enabled_cubes"] = db.count("cubes", "precomputation_status='已启用'")
        return snapshot