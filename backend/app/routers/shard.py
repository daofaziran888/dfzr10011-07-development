"""任务分片调度接口：维护分片任务，覆盖分配节点、重试分片、标记完成等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import mergeflow
from app.services.shard import ShardService

router = APIRouter(prefix="/api/shard", tags=["任务分片调度"])

service = ShardService()

LIST_FIELDS = ["分片编号", "采集计划", "分片范围", "分配节点", "开始时间", "结束时间", "重试次数", "分片状态"]
STATUSES = ["待分配", "执行中", "已完成", "执行失败"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按分片编号检索"),
    status: str | None = Query(default=None, description="待分配、执行中、已完成、执行失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按分片编号与状态过滤任务分片调度列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/completion")
def list_completion(plan_id: str | None = Query(default=None)) -> dict[str, Any]:
    """分片完成记录与空分片状态分开展示；完成水位只按连续完成的序号推进。"""
    plans = [mergeflow.engine.plan(plan_id)] if plan_id else mergeflow.engine.plans()
    plans = [plan for plan in plans if plan is not None]
    items = []
    for plan in plans:
        summary = mergeflow.engine.summarize(plan["plan_id"])
        items.append({
            "plan_id": plan["plan_id"],
            "采集计划": plan["采集计划"],
            "分片总数": summary["分片总数"],
            "分片完成水位": summary["分片完成水位"],
            "已完成分片数": summary["已完成分片数"],
            "空分片数": summary["空分片数"],
            "空计划": summary["空计划"],
            "缺口": summary["缺口"],
            "下一批待归并数": summary["下一批待归并数"],
            "完成记录": summary["完成记录"],
            "空分片": summary["空分片"],
        })
    if plan_id and not items:
        raise HTTPException(status_code=404, detail=f"采集计划 {plan_id} 不存在")
    return {"items": items, "total": len(items)}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条分片任务明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"分片任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条分片任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    # 分片登记也属于业务入口：同步刷新存储模块的分待办口径。
    if entry and entry.get("plan_id"):
        mergeflow.engine.on_shards_changed(str(entry["plan_id"]))
    return ActionResult(ok=True, message="分片任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条分片任务执行分配节点、重试分片、标记完成；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    # 分片完成后刷新存储模块的分待办口径；已生成快照的归档依据保持不变。
    if action == "标记完成" and entry.get("plan_id"):
        mergeflow.engine.on_shards_changed(str(entry["plan_id"]))
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出任务分片调度清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "shard", "total": total, "items": items}
