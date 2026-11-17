"""结果聚合归并的核心流程：分片完成水位、归并闸门、归并快照与归档清单。

这里刻意把「水位」和「快照」拆开：

* 水位是从分片完成记录里连续推导出来的，缺口之后的分片即使完成了也不会抬高水位；
* 快照一旦生成就固定住一个水位，已归并数和输出数据总量都从同一份分片视图计算，
  不会出现两个数字来自不同刷新时刻的情况；
* 同一聚合任务只会有一个归并批次，重复启动只会复用批次，快照按版本追加，旧版本
  保留原始水位和原始数据口径。

所有写入都先完整构造、统一校验，再一次性提交；校验失败或模拟写入异常时不会留下
「有归档条目、没有归并结果」的半快照。
"""
from __future__ import annotations

import copy
import threading
from datetime import datetime
from typing import Any, Iterable

from app.seed_mergeflow import PLANS

MODULE_SHARD = "shard"
MODULE_MERGE = "merge"
MODULE_STORAGE = "storage"

POLICY_FREEZE = "freeze"
POLICY_EXTEND = "extend"

POLICY_LABELS = {
    POLICY_FREEZE: "冻结启动时水位",
    POLICY_EXTEND: "延长到最后确认水位",
}

SNAPSHOT_OK = "已生成"
SNAPSHOT_FAILED = "生成失败"


class SnapshotValidationError(RuntimeError):
    """归并闸门校验不通过：不生成快照、不写归档条目。"""


class SnapshotWriteError(RuntimeError):
    """写快照时发生异常：整批回滚，避免半快照。"""


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _is_completed(row: dict[str, Any]) -> bool:
    return str(row.get("分片状态") or row.get("status") or "") == "已完成"


def _seq(row: dict[str, Any], fallback: int = 0) -> int:
    return _as_int(row.get("分片序号"), fallback)


def _records(row: dict[str, Any]) -> int:
    return max(_as_int(row.get("记录数"), 0), 0)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "是", "开启"}



def _range_records(value: str) -> int:
    """从「3201-4000」这样的分片范围推算记录量，推不出来就按 0 处理。"""
    text = value.replace("－", "-").replace("–", "-")
    if "-" not in text:
        return 0
    head, _, tail = text.partition("-")
    start, end = _as_int(head, -1), _as_int(tail, -1)
    if start < 0 or end < start:
        return 0
    return end - start + 1


def normalize_policy(value: Any) -> str:
    """把前端或接口传来的几种写法统一成 freeze / extend。"""
    text = str(value or "").strip().lower()
    if text in {"extend", "延长", "延长到最后确认水位", "延长至最后确认水位"}:
        return POLICY_EXTEND
    if text in {"freeze", "冻结", "冻结启动时水位", "冻结启动水位"}:
        return POLICY_FREEZE
    return POLICY_FREEZE


class MergeFlowEngine:
    """归并链路的状态机与快照仓库（进程内存储，和 Store 一样可替换为数据库实现）。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._seeded = False
        self.batches: dict[int, dict[str, Any]] = {}
        self.snapshots: list[dict[str, Any]] = []
        self.merge_results: list[dict[str, Any]] = []
        self.archive_entries: list[dict[str, Any]] = []
        self.gates: list[dict[str, Any]] = []
        self.review_items: list[dict[str, Any]] = []
        self.pending_items: list[dict[str, Any]] = []
        self._snapshot_seq = 0
        self._batch_seq = 0
        self._archive_seq = 0
        self._result_seq = 0
        self._gate_seq = 0
        self._review_seq = 0
        self._pending_seq = 0

    # ------------------------------------------------------------------
    # 基础查询
    # ------------------------------------------------------------------
    def plans(self) -> list[dict[str, Any]]:
        self._ensure_seeded()
        return [dict(plan) for plan in PLANS]

    def plan(self, plan_id: str) -> dict[str, Any] | None:
        for plan in PLANS:
            if str(plan.get("plan_id")) == str(plan_id):
                return dict(plan)
        return None

    def plan_by_name(self, name: str) -> dict[str, Any] | None:
        for plan in PLANS:
            if str(plan.get("采集计划")) == str(name):
                return dict(plan)
        return None

    def _shard_rows(self, plan_id: str | None = None, rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        from app.store import store

        source = rows if rows is not None else store.rows(MODULE_SHARD)
        picked = [
            row
            for row in source
            if plan_id is None or str(row.get("plan_id") or "") == str(plan_id)
        ]
        return sorted(picked, key=lambda row: (_seq(row), _as_int(row.get("id"))))

    def summarize(self, plan_id: str, rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """按分片完成记录推导连续完成水位和同一水位下的输出数据总量。"""
        self._ensure_seeded()
        plan = self.plan(plan_id) or {}
        shards = self._shard_rows(plan_id, rows)
        total = _as_int(plan.get("分片总数"), len(shards))
        by_seq = {_seq(row): row for row in shards if _seq(row) > 0}
        completed_seqs = {seq for seq, row in by_seq.items() if _is_completed(row)}

        watermark = 0
        for seq in range(1, total + 1):
            if seq in completed_seqs:
                watermark = seq
            else:
                break

        at_watermark = [by_seq[seq] for seq in range(1, watermark + 1)]
        gaps = [seq for seq in range(1, total + 1) if seq not in completed_seqs]
        completed = [by_seq[seq] for seq in range(1, total + 1) if seq in completed_seqs]
        empty_shards = [
            row
            for row in completed
            if bool(row.get("空分片")) or _records(row) == 0
        ]
        output_total = sum(_records(row) for row in at_watermark)
        pending = max(total - watermark, 0)

        return {
            "plan_id": plan_id,
            "采集计划": plan.get("采集计划", ""),
            "分片总数": total,
            "分片记录数": len(shards),
            "已完成分片数": len(completed),
            "分片完成水位": watermark,
            "watermark": watermark,
            "已归并数": len(at_watermark),
            "merged_count": len(at_watermark),
            "输出数据总量": output_total,
            "output_total": output_total,
            "待完成分片数": pending,
            "下一批待归并数": pending,
            "空分片数": len(empty_shards),
            "空计划": bool(plan.get("空计划")) or total == 0,
            "缺口": gaps,
            "缺口分片": [by_seq[seq].get("分片编号", f"#{seq}") for seq in gaps if seq in by_seq],
            "完成记录": [self._shard_view(row) for row in at_watermark],
            "空分片": [self._shard_view(row) for row in empty_shards],
            "最后确认水位": watermark,
        }

    def _shard_view(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": row.get("id"),
            "分片编号": row.get("分片编号", ""),
            "采集计划": row.get("采集计划", ""),
            "plan_id": row.get("plan_id", ""),
            "分片序号": _seq(row),
            "分片范围": row.get("分片范围", ""),
            "分配节点": row.get("分配节点", ""),
            "分片状态": row.get("分片状态") or row.get("status", ""),
            "记录数": _records(row),
            "空分片": bool(row.get("空分片")) or _records(row) == 0,
            "完成时间": row.get("完成时间", ""),
        }

    # ------------------------------------------------------------------
    # 批次与快照
    # ------------------------------------------------------------------
    def batch_for(self, task_id: int | str) -> dict[str, Any] | None:
        self._ensure_seeded()
        return self.batches.get(_as_int(task_id))

    def latest_snapshot(self, task_id: int | str) -> dict[str, Any] | None:
        batch = self.batch_for(task_id)
        if not batch:
            return None
        candidates = [item for item in self.snapshots if item.get("batch_id") == batch["batch_id"]]
        return max(candidates, key=lambda item: _as_int(item.get("version")), default=None)

    def snapshots_for(self, **filters: Any) -> list[dict[str, Any]]:
        self._ensure_seeded()
        rows = list(self.snapshots)
        if filters.get("task_id") is not None:
            rows = [row for row in rows if _as_int(row.get("task_id")) == _as_int(filters["task_id"])]
        if filters.get("plan_id"):
            rows = [row for row in rows if str(row.get("plan_id")) == str(filters["plan_id"])]
        if filters.get("status"):
            rows = [row for row in rows if row.get("快照状态") == filters["status"]]
        return sorted(rows, key=lambda item: _as_int(item.get("id")), reverse=True)

    def find_snapshot(self, reference: str) -> dict[str, Any] | None:
        self._ensure_seeded()
        text = str(reference or "").strip()
        for row in self.snapshots:
            if text in {str(row.get("snapshot_id")), str(row.get("snapshot_version")), str(row.get("版本"))}:
                return row
        return None

    def _next_version(self, batch_id: str) -> int:
        versions = [item.get("version", 0) for item in self.snapshots if item.get("batch_id") == batch_id]
        return max(versions, default=0) + 1

    def _new_batch(self, task: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
        self._batch_seq += 1
        now = _now()
        batch = {
            "id": self._batch_seq,
            "batch_id": f"MB-{self._batch_seq:04d}",
            "批次号": f"MB-{self._batch_seq:04d}",
            "task_id": _as_int(task.get("id")),
            "任务编号": task.get("任务编号", ""),
            "plan_id": plan.get("plan_id", ""),
            "采集计划": plan.get("采集计划", ""),
            "分片总数": _as_int(plan.get("分片总数")),
            "status": "归并中",
            "批次状态": "归并中",
            "created_at": now,
            "创建时间": now,
            "updated_at": now,
            "snapshot_versions": [],
            "resume_from": self._first_gap(plan["plan_id"]),
            "last_watermark": 0,
            "failure_reason": "",
        }
        self.batches[batch["task_id"]] = batch
        return batch

    def _resolve_task(self, task_id: int | str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        from app.store import store

        task = store.find(MODULE_MERGE, _as_int(task_id))
        if task is None:
            return None, None
        plan_id = str(task.get("plan_id") or "")
        if not plan_id:
            by_name = self.plan_by_name(str(task.get("采集计划") or ""))
            plan_id = str(by_name.get("plan_id")) if by_name else ""
        plan = self.plan(plan_id)
        return task, plan

    def _build_snapshot(
        self,
        *,
        task: dict[str, Any],
        plan: dict[str, Any],
        batch: dict[str, Any],
        policy: str,
        start_view: list[dict[str, Any]],
        final_view: list[dict[str, Any]],
        resume_from: int = 0,
    ) -> dict[str, Any]:
        """从一份固定的分片视图生成快照、归并结果与归档条目的完整草稿。"""
        start_summary = self.summarize(plan["plan_id"], start_view)
        final_summary = self.summarize(plan["plan_id"], final_view)
        start_wm = start_summary["分片完成水位"]
        final_wm = final_summary["分片完成水位"]

        if policy == POLICY_EXTEND:
            chosen_summary = final_summary
            watermark = final_wm
            pending_count = max(final_summary["分片总数"] - final_wm, 0)
        else:
            chosen_summary = start_summary
            watermark = start_wm
            pending_count = max(start_summary["分片总数"] - start_wm, 0)

        # 水位、已归并数和输出总量全部来自 chosen_summary 的同一份视图。
        merged_count = chosen_summary["已归并数"]
        output_total = chosen_summary["输出数据总量"]
        total = chosen_summary["分片总数"]
        chosen_by_seq = {
            _seq(row): row
            for row in self._shard_rows(plan["plan_id"], start_view if policy == POLICY_FREEZE else final_view)
        }
        included = [chosen_by_seq[seq] for seq in range(1, watermark + 1) if seq in chosen_by_seq]

        now = _now()
        version = self._next_version(batch["batch_id"])
        snapshot_id = f"SNP-{self._snapshot_seq + 1:06d}"
        snapshot = {
            "id": self._snapshot_seq + 1,
            "snapshot_id": snapshot_id,
            "snapshot_version": f"{task.get('任务编号', '')}-V{version}",
            "版本": f"V{version}",
            "version": version,
            "batch_id": batch["batch_id"],
            "批次号": batch["batch_id"],
            "task_id": _as_int(task.get("id")),
            "任务编号": task.get("任务编号", ""),
            "plan_id": plan["plan_id"],
            "采集计划": plan.get("采集计划", ""),
            "分片总数": total,
            "分片完成水位": watermark,
            "watermark": watermark,
            "启动水位": start_wm,
            "start_watermark": start_wm,
            "最后确认水位": final_wm,
            "final_watermark": final_wm,
            "已归并数": merged_count,
            "merged_count": merged_count,
            "输出数据总量": output_total,
            "数据总量": output_total,
            "output_total": output_total,
            "下一批待归并数": pending_count,
            "next_pending_count": pending_count,
            "空分片数": chosen_summary["空分片数"],
            "缺口": chosen_summary["缺口"],
            "水位策略": POLICY_LABELS[policy],
            "policy": policy,
            "policy_label": POLICY_LABELS[policy],
            "续跑起点": resume_from,
            "快照状态": SNAPSHOT_OK,
            "status": SNAPSHOT_OK,
            "生成时间": now,
            "created_at": now,
            "源刷新时间": now,
            "归档条目数": len(included),
            "说明": (
                "按冻结的启动水位生成，生成期间新完成的分片计入下一批"
                if policy == POLICY_FREEZE
                else "已延长到最后确认水位，下一批只保留该水位之后的记录"
            ),
        }

        results: list[dict[str, Any]] = []
        for order, shard in enumerate(included, start=1):
            results.append({
                "id": self._result_seq + order,
                "snapshot_id": snapshot_id,
                "snapshot_version": snapshot["snapshot_version"],
                "batch_id": batch["batch_id"],
                "task_id": snapshot["task_id"],
                "plan_id": plan["plan_id"],
                "分片编号": shard.get("分片编号", ""),
                "分片序号": _seq(shard),
                "分片范围": shard.get("分片范围", ""),
                "记录数": _records(shard),
                "空分片": bool(shard.get("空分片")) or _records(shard) == 0,
                "归并结果": "已写入",
                "输出路径": f"{task.get('输出路径', '')}/shard-{_seq(shard):04d}",
                "水位": watermark,
            })

        archive_entries: list[dict[str, Any]] = []
        for order, shard in enumerate(included, start=1):
            archive_entries.append({
                "id": self._archive_seq + order,
                "archive_id": f"ARC-{self._archive_seq + order:06d}",
                "snapshot_id": snapshot_id,
                "snapshot_version": snapshot["snapshot_version"],
                "版本": snapshot["版本"],
                "batch_id": batch["batch_id"],
                "task_id": snapshot["task_id"],
                "任务编号": task.get("任务编号", ""),
                "plan_id": plan["plan_id"],
                "采集计划": plan.get("采集计划", ""),
                "分片编号": shard.get("分片编号", ""),
                "分片序号": _seq(shard),
                "分片范围": shard.get("分片范围", ""),
                "记录数": _records(shard),
                "输出路径": f"{task.get('输出路径', '')}/shard-{_seq(shard):04d}",
                "水位": watermark,
                "启动水位": start_wm,
                "最后确认水位": final_wm,
                "归档状态": "已归档",
                "生成时间": now,
            })

        # 闸门校验：任何一项对不上都不允许落库。
        checks = self._checks(snapshot, results, archive_entries, chosen_summary, plan)
        snapshot["校验项"] = checks
        snapshot["校验通过"] = all(item["结果"] == "通过" for item in checks)
        snapshot["归并闸门"] = "已通过" if snapshot["校验通过"] else "已拦截"
        if not snapshot["校验通过"]:
            failed = "、".join(item["校验项"] for item in checks if item["结果"] != "通过")
            raise SnapshotValidationError(f"归并闸门校验未通过：{failed}")

        snapshot["一致性"] = {
            "已归并数": merged_count,
            "归并结果条数": len(results),
            "输出数据总量": output_total,
            "归并结果记录合计": sum(item["记录数"] for item in results),
            "归档条目数": len(archive_entries),
            "是否同快照": True,
        }
        return {
            "snapshot": snapshot,
            "results": results,
            "archive_entries": archive_entries,
            "checks": checks,
        }

    def _checks(
        self,
        snapshot: dict[str, Any],
        results: list[dict[str, Any]],
        archive_entries: list[dict[str, Any]],
        summary: dict[str, Any],
        plan: dict[str, Any],
    ) -> list[dict[str, Any]]:
        checks: list[dict[str, Any]] = []

        def add(name: str, passed: bool, detail: str) -> None:
            checks.append({
                "校验项": name,
                "结果": "通过" if passed else "不通过",
                "说明": detail,
            })

        add(
            "分片总数与分片记录数一致",
            summary["分片总数"] == summary["分片记录数"],
            f"计划 {summary['分片总数']} 个，分片记录 {summary['分片记录数']} 条",
        )
        add(
            "分片完成水位连续",
            snapshot["分片完成水位"] <= summary["分片总数"] and snapshot["分片完成水位"] >= 0,
            f"水位 {snapshot['分片完成水位']}，缺口 {summary['缺口'] or '无'}",
        )
        add(
            "已归并数等于完成水位",
            snapshot["已归并数"] == snapshot["分片完成水位"],
            f"已归并 {snapshot['已归并数']}，水位 {snapshot['分片完成水位']}",
        )
        add(
            "输出数据总量与归并结果合计一致",
            snapshot["输出数据总量"] == sum(item["记录数"] for item in results),
            f"快照 {snapshot['输出数据总量']}，结果合计 {sum(item['记录数'] for item in results)}",
        )
        add(
            "归档条目数与已归并数一致",
            len(archive_entries) == snapshot["已归并数"],
            f"归档 {len(archive_entries)} 条，已归并 {snapshot['已归并数']}",
        )
        add(
            "空分片已单独标识且不产生记录",
            all(item["记录数"] == 0 for item in results if item["空分片"]),
            f"空分片 {summary['空分片数']} 个",
        )
        return checks

    def _commit_snapshot(self, draft: dict[str, Any]) -> None:
        """所有表一起提交，调用方要么看到完整快照，要么什么也看不到。"""
        snapshot = draft["snapshot"]
        results = draft["results"]
        archives = draft["archive_entries"]
        if len(results) != snapshot["已归并数"]:
            raise SnapshotWriteError("归并结果条数与已归并数不一致，整批回滚")
        if len(archives) != snapshot["已归并数"]:
            raise SnapshotWriteError("归档条目数与已归并数不一致，整批回滚")

        self._snapshot_seq += 1
        self._result_seq += len(results)
        self._archive_seq += len(archives)
        snapshot["id"] = self._snapshot_seq
        for row in results:
            row["snapshot_id"] = snapshot["snapshot_id"]
        for row in archives:
            row["snapshot_id"] = snapshot["snapshot_id"]
        self.snapshots.append(snapshot)
        self.merge_results.extend(results)
        self.archive_entries.extend(archives)
        self._refresh_gate(snapshot)
        self._refresh_reviews(snapshot, draft["checks"])
        self._refresh_pending(snapshot)
        self._tag_storage_policy(snapshot)
        # 任务行展示的已归并数 / 数据总量始终取自这份快照，避免和输出数据总量不同源。
        from app.store import store

        task = store.find(MODULE_MERGE, snapshot["task_id"])
        if task is not None:
            task["已归并数"] = snapshot["已归并数"]
            task["数据总量"] = snapshot["输出数据总量"]
            task["快照版本"] = snapshot["snapshot_version"]
        batch = self.batches.get(snapshot["task_id"])
        if batch:
            batch["snapshot_versions"].append(snapshot["snapshot_version"])
            batch["last_watermark"] = snapshot["分片完成水位"]
            batch["updated_at"] = _now()
            batch["status"] = "归并中"
            batch["批次状态"] = "归并中"
            batch["failure_reason"] = ""

    def _tag_storage_policy(self, snapshot: dict[str, Any]) -> None:
        from app.store import store

        for policy in store.rows(MODULE_STORAGE):
            if str(policy.get("plan_id")) == str(snapshot["plan_id"]):
                policy["归并快照版本"] = snapshot["snapshot_version"]
                policy["归并快照"] = snapshot["snapshot_version"]
                policy["快照水位"] = snapshot["分片完成水位"]
                policy["已归并数"] = snapshot["已归并数"]
                policy["数据总量"] = snapshot["输出数据总量"]

    def _refresh_gate(self, snapshot: dict[str, Any]) -> None:
        existing = next(
            (
                item
                for item in self.gates
                if str(item.get("plan_id")) == str(snapshot["plan_id"])
                and str(item.get("归并任务", "")) == str(snapshot["任务编号"])
            ),
            None,
        )
        now = _now()
        values = {
            "plan_id": snapshot["plan_id"],
            "采集计划": snapshot["采集计划"],
            "归并任务": snapshot["任务编号"],
            "归并批次": snapshot["batch_id"],
            "快照版本": snapshot["版本"],
            "snapshot_version": snapshot["snapshot_version"],
            "snapshot_id": snapshot["snapshot_id"],
            "分片总数": snapshot["分片总数"],
            "分片完成水位": snapshot["分片完成水位"],
            "已归并数": snapshot["已归并数"],
            "数据总量": snapshot["输出数据总量"],
            "输出数据总量": snapshot["输出数据总量"],
            "归档条目数": snapshot["归档条目数"],
            "闸门状态": snapshot["归并闸门"],
            "校验通过": snapshot["校验通过"],
            "更新时间": now,
        }
        if existing is None:
            self._gate_seq += 1
            values.update({"id": self._gate_seq, "闸门编号": f"GATE-{self._gate_seq:04d}"})
            self.gates.append(values)
        else:
            existing.update(values)

    def _refresh_reviews(self, snapshot: dict[str, Any], checks: list[dict[str, Any]]) -> None:
        for index, check in enumerate(checks, start=1):
            self._review_seq += 1
            self.review_items.append({
                "id": self._review_seq,
                "复核编号": f"REV-{self._review_seq:04d}",
                "snapshot_id": snapshot["snapshot_id"],
                "snapshot_version": snapshot["snapshot_version"],
                "版本": snapshot["版本"],
                "plan_id": snapshot["plan_id"],
                "采集计划": snapshot["采集计划"],
                "任务编号": snapshot["任务编号"],
                "复核项": check["校验项"],
                "校验结果": check["结果"],
                "复核依据": check["说明"],
                "复核状态": "待复核",
                "复核人": "",
                "创建时间": snapshot["生成时间"],
            })

    def _refresh_pending(self, snapshot: dict[str, Any]) -> None:
        existing = next(
            (
                item
                for item in self.pending_items
                if str(item.get("plan_id")) == str(snapshot["plan_id"])
                and str(item.get("任务编号", "")) == str(snapshot["任务编号"])
            ),
            None,
        )
        status = "已完成" if snapshot["下一批待归并数"] == 0 else "待处理"
        values = {
            "plan_id": snapshot["plan_id"],
            "采集计划": snapshot["采集计划"],
            "任务编号": snapshot["任务编号"],
            "快照版本": snapshot["版本"],
            "snapshot_version": snapshot["snapshot_version"],
            "当前水位": snapshot["分片完成水位"],
            "下一批待归并数": snapshot["下一批待归并数"],
            "缺口": snapshot["缺口"],
            "水位策略": snapshot["水位策略"],
            "待办状态": status,
            "更新时间": snapshot["生成时间"],
            "说明": snapshot["说明"],
        }
        if existing is None:
            self._pending_seq += 1
            values.update({"id": self._pending_seq, "待办编号": f"TODO-{self._pending_seq:04d}"})
            self.pending_items.append(values)
        else:
            existing.update(values)

    # ------------------------------------------------------------------
    # 对外动作
    # ------------------------------------------------------------------
    def generate(
        self,
        task_id: int | str,
        *,
        policy: Any = POLICY_FREEZE,
        concurrent_shards: Iterable[Any] | None = None,
        simulate_write_error: bool = False,
        resume: bool = False,
        force_new: bool = False,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str, str]:
        """生成快照。

        返回 (task, snapshot, message, status)；status 为 ok / conflict / failed。
        重复调用同一水位时返回已有快照，不会追加版本。
        """
        with self._lock:
            self._ensure_seeded()
            task, plan = self._resolve_task(task_id)
            if task is None:
                return None, None, f"聚合任务 {task_id} 不存在或已归档", "failed"
            if plan is None:
                return task, None, f"聚合任务 {task_id} 未关联有效的采集计划", "failed"

            mode = normalize_policy(policy)
            batch = self.batches.get(_as_int(task.get("id")))
            if batch is None:
                batch = self._new_batch(task, plan)
            elif batch["status"] == "已完成" and not force_new and not resume:
                latest = self.latest_snapshot(task["id"])
                return task, latest, f"归并批次 {batch['batch_id']} 已存在，沿用快照 {latest['版本'] if latest else '—'}", "conflict"

            # 失败重试从缺口继续：以最近快照的水位作为已确认的归并进度，
            # 只补归并水位之后的分片；缺口还没闭合时直接说明原因，不生成半截快照。
            latest = self.latest_snapshot(task["id"])
            resume_from = _as_int(batch.get("resume_from")) if resume else 0
            if resume and latest is not None:
                resume_from = max(resume_from, _as_int(latest.get("分片完成水位")) + 1)

            start_view = copy.deepcopy(self._shard_rows(plan["plan_id"]))
            start_summary = self.summarize(plan["plan_id"], start_view)

            # 模拟「快照生成过程中又有分片完成」：只改变活数据，不影响已捕获的启动视图。
            applied = self._apply_concurrent_completions(plan["plan_id"], concurrent_shards or [])
            final_view = copy.deepcopy(self._shard_rows(plan["plan_id"]))
            final_summary = self.summarize(plan["plan_id"], final_view)

            if resume and latest is not None and final_summary["分片完成水位"] <= _as_int(latest.get("分片完成水位")):
                if final_summary["缺口"]:
                    blocking = final_summary["缺口"][0]
                    message = f"归并失败：缺口分片 {blocking} 尚未完成，无法从缺口继续"
                    batch["status"] = "归并失败"
                    batch["批次状态"] = "归并失败"
                    batch["failure_reason"] = message
                    batch["resume_from"] = blocking
                    self._set_task_status(task, "归并失败")
                    self._refresh_gate_failure(plan, task, batch, message)
                    return task, None, message, "failed"
                # 水位已经和最近快照一致：没有可续跑的分片，直接复用快照。
                return (
                    task,
                    latest,
                    f"分片完成水位 {final_summary['分片完成水位']} 已与快照 {latest['版本']} 一致，没有可续跑的分片",
                    "conflict",
                )

            chosen_summary = final_summary if mode == POLICY_EXTEND else start_summary
            existing = next(
                (
                    item
                    for item in self.snapshots
                    if item.get("batch_id") == batch["batch_id"]
                    and _as_int(item.get("分片完成水位")) == chosen_summary["分片完成水位"]
                    and item.get("policy") == mode
                ),
                None,
            )
            if existing is not None and not force_new and not applied:
                # 重复启动或重试都不会创建第二个批次/第二份同水位快照。
                return task, existing, f"水位 {chosen_summary['分片完成水位']} 已有快照 {existing['版本']}，不重复生成", "conflict"

            try:
                draft = self._build_snapshot(
                    task=task,
                    plan=plan,
                    batch=batch,
                    policy=mode,
                    start_view=start_view,
                    final_view=final_view,
                    resume_from=resume_from,
                )
                if simulate_write_error:
                    # 在提交前抛出：快照、归并结果、归档条目、闸门、复核和待办全部不落库。
                    raise SnapshotWriteError("写入归档清单时发生异常，快照已整批回滚")
                self._commit_snapshot(draft)
            except SnapshotValidationError as exc:
                batch["status"] = "归并失败"
                batch["批次状态"] = "归并失败"
                batch["failure_reason"] = str(exc)
                batch["resume_from"] = self._first_gap(plan["plan_id"])
                self._set_task_status(task, "归并失败")
                self._refresh_gate_failure(plan, task, batch, str(exc))
                return task, None, str(exc), "failed"
            except SnapshotWriteError as exc:
                batch["status"] = "归并失败"
                batch["批次状态"] = "归并失败"
                batch["failure_reason"] = str(exc)
                batch["resume_from"] = self._first_gap(plan["plan_id"])
                self._set_task_status(task, "归并失败")
                self._refresh_gate_failure(plan, task, batch, str(exc), gate_status="写入失败")
                return task, None, str(exc), "failed"

            snapshot = draft["snapshot"]
            batch["resume_from"] = snapshot["分片完成水位"] + 1
            batch["resumed_from"] = resume_from
            self._set_task_status(task, "归并中")
            message = f"已生成归并快照 {snapshot['版本']}，水位 {snapshot['分片完成水位']}，已归并 {snapshot['已归并数']} 片，输出 {snapshot['输出数据总量']} 条"
            if mode == POLICY_FREEZE and final_summary["分片完成水位"] > snapshot["分片完成水位"]:
                message += "；生成期间有新分片完成，已按冻结水位归档，新增分片计入下一批"
                return task, snapshot, message, "conflict"
            if mode == POLICY_EXTEND and final_summary["分片完成水位"] > start_summary["分片完成水位"]:
                message += "；已延长到最后确认水位，下一批只保留该水位之后的记录"
                return task, snapshot, message, "conflict"
            return task, snapshot, message, "ok"

    def _first_gap(self, plan_id: str) -> int:
        summary = self.summarize(plan_id)
        gaps = summary["缺口"]
        return gaps[0] if gaps else summary["分片完成水位"] + 1

    def _set_task_status(self, task: dict[str, Any], status: str) -> None:
        task["status"] = status
        task["任务状态"] = status
        task["pending"] = status != "已完成"
        task["abnormal"] = status == "归并失败"

    def _apply_concurrent_completions(self, plan_id: str, specs: Iterable[Any]) -> bool:
        from app.store import store

        applied = False
        rows = store.rows(MODULE_SHARD)
        for spec in specs:
            records: int | None = None
            empty: bool | None = None
            seq = 0
            shard_no = ""
            if isinstance(spec, dict):
                seq = _as_int(spec.get("分片序号") or spec.get("seq"))
                shard_no = str(spec.get("分片编号") or spec.get("shard_no") or "")
                if "记录数" in spec or "records" in spec:
                    records = _as_int(spec.get("记录数", spec.get("records")), 0)
                if "空分片" in spec or "empty" in spec:
                    empty = _truthy(spec.get("空分片", spec.get("empty")))
            else:
                text = str(spec).strip()
                if ":" in text:
                    # 序号:记录数，用来模拟分片完成时带回的记录量
                    head, _, tail = text.partition(":")
                    seq = _as_int(head)
                    records = _as_int(tail)
                elif text.isdigit():
                    seq = _as_int(text)
                else:
                    shard_no = text
            target = None
            for row in rows:
                if str(row.get("plan_id")) != str(plan_id):
                    continue
                if shard_no and str(row.get("分片编号")) == shard_no:
                    target = row
                    break
                if seq and _seq(row) == seq:
                    target = row
                    break
            if target is None:
                continue
            target["分片状态"] = "已完成"
            target["status"] = "已完成"
            target["pending"] = False
            target["abnormal"] = False
            target["完成时间"] = target.get("完成时间") or _now()
            target["结束时间"] = target.get("结束时间") or _now()[:10]
            if records is not None:
                target["记录数"] = records
            if empty is not None:
                target["空分片"] = empty
            else:
                if _records(target) == 0:
                    # 没显式给记录数时按分片范围推算一个可复现的记录量，
                    # 空分片必须显式声明（记录数 0 或 empty）。
                    target["记录数"] = _range_records(str(target.get("分片范围") or ""))
                target["空分片"] = _records(target) == 0
            applied = True
        return applied

    def _refresh_gate_failure(
        self,
        plan: dict[str, Any],
        task: dict[str, Any],
        batch: dict[str, Any],
        reason: str,
        gate_status: str = "已拦截",
    ) -> None:
        existing = next(
            (
                item
                for item in self.gates
                if str(item.get("plan_id")) == str(plan["plan_id"])
                and str(item.get("归并任务", "")) == str(task.get("任务编号", ""))
            ),
            None,
        )
        values = {
            "plan_id": plan["plan_id"],
            "采集计划": plan.get("采集计划", ""),
            "归并任务": task.get("任务编号", ""),
            "归并批次": batch["batch_id"],
            "快照版本": "—",
            "分片总数": _as_int(plan.get("分片总数")),
            "分片完成水位": _as_int(batch.get("last_watermark")),
            "已归并数": _as_int(batch.get("last_watermark")),
            "数据总量": 0,
            "闸门状态": gate_status,
            "校验通过": False,
            "失败原因": reason,
            "更新时间": _now(),
        }
        if existing is None:
            self._gate_seq += 1
            values.update({"id": self._gate_seq, "闸门编号": f"GATE-{self._gate_seq:04d}"})
            self.gates.append(values)
        else:
            existing.update(values)

    def start(self, task_id: int | str, **kwargs: Any) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str, str]:
        return self.generate(task_id, resume=False, **kwargs)

    def retry(self, task_id: int | str, **kwargs: Any) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str, str]:
        return self.generate(task_id, resume=True, **kwargs)

    def archive_task(self, task_id: int | str) -> tuple[dict[str, Any] | None, str]:
        with self._lock:
            self._ensure_seeded()
            task, plan = self._resolve_task(task_id)
            if task is None:
                return None, f"聚合任务 {task_id} 不存在或已归档"
            snapshot = self.latest_snapshot(task["id"])
            if snapshot is None:
                return task, "尚未生成归并快照，不能归档；请先启动归并"
            batch = self.batch_for(task["id"])
            if batch:
                batch["status"] = "已完成"
                batch["批次状态"] = "已完成"
                batch["updated_at"] = _now()
            self._set_task_status(task, "已完成")
            self._refresh_gate(snapshot)
            gate = next((item for item in self.gates if str(item.get("plan_id")) == str(snapshot["plan_id"])), None)
            if gate:
                gate["闸门状态"] = "已归档"
            for item in self.review_items:
                if item.get("snapshot_id") == snapshot["snapshot_id"] and item.get("复核状态") == "待复核":
                    item["复核状态"] = "已复核"
                    item["复核人"] = "归并归档"
            return task, f"聚合任务已归档，归档清单固定读取快照 {snapshot['版本']}（水位 {snapshot['分片完成水位']}）"

    # ------------------------------------------------------------------
    # 存储模块读取口径
    # ------------------------------------------------------------------
    def gate_rows(self) -> list[dict[str, Any]]:
        self._ensure_seeded()
        self._sync_gate_pending_for_unsaved_tasks()
        return [dict(row) for row in self.gates]

    def pending_rows(self) -> list[dict[str, Any]]:
        self._ensure_seeded()
        self._sync_gate_pending_for_unsaved_tasks()
        # 分待办每次读取都按当前分片完成记录复核一遍：任何业务入口改了分片状态，
        # 这里都能立刻反映；已固定快照的任务仍以快照水位为准。
        for pending in self.pending_items:
            self._refresh_pending_live(pending)
        return [dict(row) for row in self.pending_items]

    def review_rows(self) -> list[dict[str, Any]]:
        self._ensure_seeded()
        return [dict(row) for row in self.review_items]

    def review_action(self, review_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        with self._lock:
            self._ensure_seeded()
            item = next((row for row in self.review_items if _as_int(row.get("id")) == _as_int(review_id)), None)
            if item is None:
                return None, f"复核项 {review_id} 不存在"
            if action not in {"复核通过", "标记异常", "重新打开"}:
                return None, f"动作「{action}」不属于复核清单可执行范围"
            mapping = {"复核通过": "已复核", "标记异常": "异常", "重新打开": "待复核"}
            item["复核状态"] = mapping[action]
            item["复核人"] = "值班管理员" if action != "重新打开" else ""
            return item, f"复核项已{action}"

    def archive_rows(self, snapshot_reference: str | None = None, plan_id: str | None = None) -> list[dict[str, Any]]:
        self._ensure_seeded()
        snapshot = None
        if snapshot_reference:
            snapshot = self.find_snapshot(snapshot_reference)
            if snapshot is None:
                return []
        rows = list(self.archive_entries)
        if snapshot is not None:
            rows = [row for row in rows if row.get("snapshot_id") == snapshot["snapshot_id"]]
        if plan_id:
            rows = [row for row in rows if str(row.get("plan_id")) == str(plan_id)]
        return [dict(row) for row in rows]

    def result_rows(self, snapshot_reference: str | None = None) -> list[dict[str, Any]]:
        self._ensure_seeded()
        rows = list(self.merge_results)
        if snapshot_reference:
            snapshot = self.find_snapshot(snapshot_reference)
            if snapshot is None:
                return []
            rows = [row for row in rows if row.get("snapshot_id") == snapshot["snapshot_id"]]
        return [dict(row) for row in rows]

    def manifest(self, snapshot_reference: str | None = None) -> tuple[dict[str, Any] | None, str]:
        """存储分层读取快照版本，返回该版本的归档清单。"""
        self._ensure_seeded()
        if not snapshot_reference:
            # 缺省取「最近一份有归档条目的快照」；空计划快照不抢占默认视图。
            candidates = [item for item in self.snapshots if _as_int(item.get("已归并数")) > 0]
            snapshot = max(candidates, key=lambda item: _as_int(item.get("id")), default=None)
            if snapshot is None:
                snapshot = max(self.snapshots, key=lambda item: _as_int(item.get("id")), default=None)
        else:
            snapshot = self.find_snapshot(snapshot_reference)
        if snapshot is None:
            return None, f"归并快照 {snapshot_reference or '—'} 不存在，无法生成归档清单"
        entries = [dict(row) for row in self.archive_entries if row.get("snapshot_id") == snapshot["snapshot_id"]]
        results = [dict(row) for row in self.merge_results if row.get("snapshot_id") == snapshot["snapshot_id"]]
        manifest = {
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot_version": snapshot["snapshot_version"],
            "版本": snapshot["版本"],
            "快照水位": snapshot["分片完成水位"],
            "启动水位": snapshot["启动水位"],
            "最后确认水位": snapshot["最后确认水位"],
            "policy": snapshot["policy"],
            "水位策略": snapshot["水位策略"],
            "plan_id": snapshot["plan_id"],
            "采集计划": snapshot["采集计划"],
            "任务编号": snapshot["任务编号"],
            "分片总数": snapshot["分片总数"],
            "已归并数": snapshot["已归并数"],
            "输出数据总量": snapshot["输出数据总量"],
            "归档条目数": len(entries),
            "归并结果条数": len(results),
            "归并结果合计": sum(_as_int(item.get("记录数")) for item in results),
            "缺口": snapshot["缺口"],
            "下一批待归并数": snapshot["下一批待归并数"],
            "归档状态": "完整" if len(entries) == len(results) == snapshot["已归并数"] else "不完整",
            "生成时间": snapshot["生成时间"],
            "items": entries,
            "results": results,
        }
        return manifest, "归档清单已按快照版本读取"

    def ensure_manifest(self, snapshot_reference: str) -> tuple[dict[str, Any] | None, str]:
        """存储分层按版本生成归档清单；幂等，已有条目直接复用。"""
        with self._lock:
            manifest, message = self.manifest(snapshot_reference)
            if manifest is None:
                return None, message
            if manifest["归档状态"] != "完整":
                return None, f"快照 {manifest['版本']} 的归档清单不完整，已阻止生成"
            return manifest, message

    def consistency(self) -> dict[str, Any]:
        """给归并页和自测用的总览：每一份快照的两个数字必须同源。"""
        self._ensure_seeded()
        rows = []
        for snapshot in self.snapshots:
            results = [item for item in self.merge_results if item.get("snapshot_id") == snapshot["snapshot_id"]]
            archives = [item for item in self.archive_entries if item.get("snapshot_id") == snapshot["snapshot_id"]]
            rows.append({
                "snapshot_id": snapshot["snapshot_id"],
                "snapshot_version": snapshot["snapshot_version"],
                "版本": snapshot["版本"],
                "采集计划": snapshot["采集计划"],
                "分片完成水位": snapshot["分片完成水位"],
                "已归并数": snapshot["已归并数"],
                "输出数据总量": snapshot["输出数据总量"],
                "归并结果条数": len(results),
                "结果记录合计": sum(_as_int(item.get("记录数")) for item in results),
                "归档条目数": len(archives),
                "一致": (
                    len(results) == snapshot["已归并数"]
                    and len(archives) == snapshot["已归并数"]
                    and sum(_as_int(item.get("记录数")) for item in results) == snapshot["输出数据总量"]
                ),
            })
        return {"items": rows, "total": len(rows)}

    # ------------------------------------------------------------------
    # 初始化与联动
    # ------------------------------------------------------------------
    def sync_task(self, task: dict[str, Any]) -> None:
        """业务入口新增聚合任务时，同步存储模块的闸门和待办。"""
        with self._lock:
            self._ensure_seeded()
            plan = self.plan(str(task.get("plan_id") or "")) or self.plan_by_name(str(task.get("采集计划") or ""))
            if plan is None:
                return
            existing = next(
                (
                    item
                    for item in self.gates
                    if str(item.get("plan_id")) == str(plan["plan_id"])
                    and str(item.get("归并任务", "")) == str(task.get("任务编号", ""))
                ),
                None,
            )
            if existing is None:
                summary = self.summarize(plan["plan_id"])
                self._gate_seq += 1
                self.gates.append({
                    "id": self._gate_seq,
                    "闸门编号": f"GATE-{self._gate_seq:04d}",
                    "plan_id": plan["plan_id"],
                    "采集计划": plan.get("采集计划", ""),
                    "归并任务": task.get("任务编号", ""),
                    "归并批次": "—",
                    "快照版本": "—",
                    "分片总数": summary["分片总数"],
                    "分片完成水位": summary["分片完成水位"],
                    "已归并数": 0,
                    "数据总量": 0,
                    "归档条目数": 0,
                    "闸门状态": "未启动",
                    "校验通过": False,
                    "更新时间": _now(),
                })
            existing_pending = next(
                (
                    item
                    for item in self.pending_items
                    if str(item.get("plan_id")) == str(plan["plan_id"])
                    and str(item.get("任务编号", "")) == str(task.get("任务编号", ""))
                ),
                None,
            )
            if existing_pending is None:
                summary = self.summarize(plan["plan_id"])
                self._pending_seq += 1
                self.pending_items.append({
                    "id": self._pending_seq,
                    "待办编号": f"TODO-{self._pending_seq:04d}",
                    "plan_id": plan["plan_id"],
                    "采集计划": plan.get("采集计划", ""),
                    "任务编号": task.get("任务编号", ""),
                    "快照版本": "—",
                    "当前水位": summary["分片完成水位"],
                    "下一批待归并数": summary["下一批待归并数"],
                    "缺口": summary["缺口"],
                    "水位策略": "—",
                    "待办状态": "待处理" if summary["下一批待归并数"] else "已完成",
                    "更新时间": _now(),
                    "说明": "尚未生成归并快照，按当前分片完成水位计算",
                })
            # 已登记的待办用当前分片记录复核一次，任何入口新增任务后都不会显示旧数字。
            for pending in self.pending_items:
                if str(pending.get("plan_id")) == str(plan["plan_id"]):
                    self._refresh_pending_live(pending)

    def on_shards_changed(self, plan_id: str | None = None) -> None:
        """分片完成后刷新存储模块的分待办口径；不改变已生成快照的依据。"""
        with self._lock:
            self._ensure_seeded()
            for pending in self.pending_items:
                if plan_id and str(pending.get("plan_id")) != str(plan_id):
                    continue
                self._refresh_pending_live(pending)

    def _refresh_pending_live(self, pending: dict[str, Any]) -> None:
        """按当前分片完成记录复核一条分待办；有快照的仍以快照水位为起点。"""
        plan_id = str(pending.get("plan_id") or "")
        if not plan_id or self.plan(plan_id) is None:
            return
        summary = self.summarize(plan_id)
        total = _as_int(summary.get("分片总数"))
        version = pending.get("快照版本")
        snapshot = None if version in (None, "", "—") else self.find_snapshot(str(version))
        if snapshot is None:
            pending["当前水位"] = summary["分片完成水位"]
            remaining = max(total - summary["分片完成水位"], 0)
        else:
            pending["当前水位"] = snapshot["分片完成水位"]
            # 冻结口径下新增分片要计入下一批；延长口径下只保留水位之后的记录。
            # 两种口径的剩余量都是「分片总数 - 快照水位」。
            remaining = max(total - _as_int(snapshot["分片完成水位"]), 0)
        pending["下一批待归并数"] = remaining
        pending["待办状态"] = "待处理" if remaining else "已完成"
        pending["缺口"] = summary["缺口"]

    def _sync_gate_pending_for_unsaved_tasks(self) -> None:
        from app.store import store

        for task in store.rows(MODULE_MERGE):
            if self.batch_for(task.get("id")) is None:
                self.sync_task(task)

    def _ensure_seeded(self) -> None:
        if self._seeded:
            return
        # 先置位，避免 summarize/plan 等调用递归回到这里。
        self._seeded = True
        self._seed_saved_results()
        from app.store import store

        for task in store.rows(MODULE_MERGE):
            self.sync_task(task)

    def _seed_saved_results(self) -> None:
        """预置一份已保存的归并结果，用来验证重进页面后旧记录仍保留原依据。"""
        from app.store import store

        for task in store.rows(MODULE_MERGE):
            plan_id = str(task.get("plan_id") or "")
            if not plan_id:
                continue
            status = str(task.get("status") or "")
            if status not in {"归并中", "已完成"}:
                continue
            summary = self.summarize(plan_id)
            if summary["分片完成水位"] <= 0 and not summary["空计划"]:
                continue
            batch = self._new_batch(task, self.plan(plan_id) or {"plan_id": plan_id, "采集计划": task.get("采集计划", "")})
            view = self._shard_rows(plan_id)
            draft = self._build_snapshot(
                task=task,
                plan=self.plan(plan_id) or {
                    "plan_id": plan_id,
                    "采集计划": task.get("采集计划", ""),
                    "分片总数": summary["分片总数"],
                },
                batch=batch,
                policy=POLICY_FREEZE,
                start_view=view,
                final_view=view,
            )
            self._commit_snapshot(draft)
            snapshot = draft["snapshot"]
            batch["status"] = "已完成" if status == "已完成" else "归并中"
            batch["批次状态"] = batch["status"]
            batch["last_watermark"] = snapshot["分片完成水位"]
            batch["resume_from"] = snapshot["分片完成水位"] + 1
            if status == "已完成":
                self._set_task_status(task, "已完成")
                gate = next((item for item in self.gates if str(item.get("plan_id")) == plan_id), None)
                if gate:
                    gate["闸门状态"] = "已归档"


engine = MergeFlowEngine()
