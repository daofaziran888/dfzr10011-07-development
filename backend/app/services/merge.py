"""结果聚合归并业务规则：状态流转、字段校验与筛选口径都收在这里。

真正的归并快照流程在 services/mergeflow.py；这里只负责任务登记和任务级筛选。
"""
from __future__ import annotations

from typing import Any

from app.services import mergeflow
from app.store import store

MODULE = "merge"
REQUIRED_FIELDS = ["任务编号", "采集计划", "分片总数"]
STATUS_ORDER = ["待归并", "归并中", "已完成", "归并失败"]
ACTION_RULES = {"启动归并": "待归并", "重试归并": "归并中", "归档任务": "归并失败"}
NEGATIVE_ACTIONS = []


class MergeService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("任务编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        plan = mergeflow.engine.plan(str(values.get("plan_id") or "")) or mergeflow.engine.plan_by_name(
            str(values.get("采集计划") or "")
        )
        if plan is not None:
            entry["plan_id"] = plan["plan_id"]
            entry["分片总数"] = plan.get("分片总数", entry.get("分片总数"))
        elif values.get("plan_id"):
            entry["plan_id"] = str(values["plan_id"])
        entry["已归并数"] = 0
        entry["数据总量"] = 0
        entry["归并策略"] = values.get("归并策略") or "按分片顺序归并"
        entry["输出路径"] = values.get("输出路径") or f"/data/merge/{entry['任务编号']}"
        entry["任务状态"] = STATUS_ORDER[0]
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"聚合任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于结果聚合归并可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["任务状态"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"聚合任务已{action}"
