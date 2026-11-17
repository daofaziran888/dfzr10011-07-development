"""任务分片调度业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services import mergeflow
from app.store import store

MODULE = "shard"
REQUIRED_FIELDS = ["分片编号", "采集计划", "分片范围"]
STATUS_ORDER = ["待分配", "执行中", "已完成", "执行失败"]
ACTION_RULES = {"分配节点": "执行中", "重试分片": "执行中", "标记完成": "已完成"}
NEGATIVE_ACTIONS = []


class ShardService:
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
            rows = [row for row in rows if keyword in str(row.get("分片编号", ""))]
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
        entry["status"] = STATUS_ORDER[0]
        entry["分片状态"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        plan = mergeflow.engine.plan(str(values.get("plan_id") or "")) or mergeflow.engine.plan_by_name(
            str(values.get("采集计划") or "")
        )
        if plan is not None:
            entry["plan_id"] = plan["plan_id"]
        elif values.get("plan_id"):
            entry["plan_id"] = str(values["plan_id"])
        seq = values.get("分片序号")
        if seq in (None, ""):
            existing = [row for row in rows if str(row.get("plan_id")) == str(entry.get("plan_id"))]
            seq = max((int(row.get("分片序号") or 0) for row in existing), default=0) + 1
        entry["分片序号"] = int(seq)
        entry["记录数"] = int(values.get("记录数") or 0)
        entry["空分片"] = bool(values.get("空分片")) or entry["记录数"] == 0
        entry["完成时间"] = ""
        entry["采集计划"] = (plan or {}).get("采集计划", entry["采集计划"])
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"分片任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于任务分片调度可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["分片状态"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        if target == "已完成":
            entry["完成时间"] = entry.get("完成时间") or datetime.now().strftime("%Y-%m-%d %H:%M")
            if "空分片" not in entry:
                entry["空分片"] = int(entry.get("记录数") or 0) == 0
        return entry, f"分片任务已{action}"
