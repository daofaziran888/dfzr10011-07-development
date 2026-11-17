"""结果聚合归并接口：维护聚合任务，覆盖启动归并、重试归并、归档任务等动作。

归并链路的核心口径都在 services/mergeflow.py：
分片完成水位 → 归并闸门 → 同一水位快照（已归并数 / 输出数据总量）→ 归档清单。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.merge import MergeService
from app.services import mergeflow

router = APIRouter(prefix="/api/merge", tags=["结果聚合归并"])

service = MergeService()

LIST_FIELDS = ["任务编号", "采集计划", "分片总数", "已归并数", "归并策略", "输出路径", "数据总量", "任务状态"]
STATUSES = ["待归并", "归并中", "已完成", "归并失败"]


def _task_view(task: dict[str, Any]) -> dict[str, Any]:
    """把任务、当前分片水位和最近快照口径合成一行，供列表直接展示。"""
    row = dict(task)
    summary = mergeflow.engine.summarize(str(task.get("plan_id") or ""))
    snapshot = mergeflow.engine.latest_snapshot(task.get("id"))
    batch = mergeflow.engine.batch_for(task.get("id"))
    row.update({
        "分片完成水位": summary["分片完成水位"],
        "当前已归并数": summary["已归并数"],
        "当前输出数据总量": summary["输出数据总量"],
        "空分片数": summary["空分片数"],
        "缺口": summary["缺口"],
        "下一批待归并数": summary["下一批待归并数"],
        "归并批次": batch["batch_id"] if batch else "—",
        "快照版本": snapshot["snapshot_version"] if snapshot else "—",
        "快照水位": snapshot["分片完成水位"] if snapshot else "—",
        "快照已归并数": snapshot["已归并数"] if snapshot else "—",
        "快照数据总量": snapshot["输出数据总量"] if snapshot else "—",
        "快照时间": snapshot["生成时间"] if snapshot else "—",
    })
    return row


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="待归并、归并中、已完成、归并失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤结果聚合归并列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=[_task_view(item) for item in items], total=total, page=page, size=size)


@router.get("/progress")
def get_progress() -> dict[str, Any]:
    """按采集计划给出分片完成水位、已归并数、输出数据总量和下一批待归并数。"""
    items: list[dict[str, Any]] = []
    tasks = service.list_entries(page=1, size=10000)[0]
    for task in tasks:
        summary = mergeflow.engine.summarize(str(task.get("plan_id") or ""))
        snapshot = mergeflow.engine.latest_snapshot(task.get("id"))
        summary.update({
            "task_id": task.get("id"),
            "任务编号": task.get("任务编号"),
            "任务状态": task.get("status"),
            "快照版本": snapshot["snapshot_version"] if snapshot else "—",
            "快照水位": snapshot["分片完成水位"] if snapshot else None,
            "快照已归并数": snapshot["已归并数"] if snapshot else None,
            "快照输出数据总量": snapshot["输出数据总量"] if snapshot else None,
        })
        items.append(summary)
    return {"items": items, "total": len(items)}


@router.get("/watermark")
def get_watermark(plan_id: str | None = Query(default=None)) -> dict[str, Any]:
    """读取分片完成水位：只按连续完成的序号推进，缺口之后的分片不计入。"""
    if not plan_id:
        return {"items": [mergeflow.engine.summarize(plan["plan_id"]) for plan in mergeflow.engine.plans()]}
    plan = mergeflow.engine.plan(plan_id)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"采集计划 {plan_id} 不存在")
    return mergeflow.engine.summarize(plan_id)


@router.get("/completion")
def get_completion(plan_id: str | None = Query(default=None, description="采集计划编号")) -> dict[str, Any]:
    """分片完成记录与空分片状态分开返回，便于前端分别展示。"""
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
            "完成记录": summary["完成记录"],
            "空分片": summary["空分片"],
        })
    if plan_id and not items:
        raise HTTPException(status_code=404, detail=f"采集计划 {plan_id} 不存在")
    return {"items": items, "total": len(items)}


@router.get("/snapshots")
def list_snapshots(
    plan_id: str | None = Query(default=None),
    task_id: int | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    """归并快照列表：每个版本固定一个分片完成水位。"""
    rows = mergeflow.engine.snapshots_for(plan_id=plan_id, task_id=task_id, status=status)
    return {"items": rows, "total": len(rows)}


@router.get("/snapshots/{reference}")
def get_snapshot(reference: str) -> dict[str, Any]:
    """读取一份归并快照，连同它的归并结果、归档条目和闸门校验项。"""
    snapshot = mergeflow.engine.find_snapshot(reference)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"归并快照 {reference} 不存在")
    results = mergeflow.engine.result_rows(snapshot["snapshot_id"])
    archives = mergeflow.engine.archive_rows(snapshot["snapshot_id"])
    # 重新进入页面 / 刷新后重算：只按快照里固定的水位重算，不再读取活的分片状态。
    recomputed = [
        item
        for item in results
        if 0 < int(item.get("分片序号") or 0) <= int(snapshot["分片完成水位"])
    ]
    recomputed_count = len(recomputed)
    recomputed_total = sum(int(item.get("记录数") or 0) for item in recomputed)
    live_watermark = mergeflow.engine.summarize(str(snapshot["plan_id"]))["分片完成水位"]
    consistent = (
        recomputed_count == snapshot["已归并数"]
        and recomputed_total == snapshot["输出数据总量"]
        and len(results) == snapshot["已归并数"]
        and len(archives) == snapshot["已归并数"]
    )
    return {
        "snapshot": snapshot,
        "results": results,
        "archive_entries": archives,
        "consistency": {
            "已归并数": snapshot["已归并数"],
            "归并结果条数": len(results),
            "输出数据总量": snapshot["输出数据总量"],
            "归并结果记录合计": sum(int(item.get("记录数") or 0) for item in results),
            "归档条目数": len(archives),
            "重算已归并数": recomputed_count,
            "重算输出数据总量": recomputed_total,
            "一致": consistent,
        },
        "recompute": {
            "固定水位": snapshot["分片完成水位"],
            "重算已归并数": recomputed_count,
            "重算输出数据总量": recomputed_total,
            "当前分片完成水位": live_watermark,
            "水位漂移": live_watermark - int(snapshot["分片完成水位"]),
            "说明": "快照明细只按固定水位重算，当前分片进度不影响已保存结果",
        },
    }


@router.get("/results")
def list_results(snapshot: str | None = Query(default=None, description="快照编号或版本")) -> dict[str, Any]:
    """归并结果数据：可按快照版本读取，作为存储归档和下游复用依据。"""
    rows = mergeflow.engine.result_rows(snapshot)
    return {"items": rows, "total": len(rows), "snapshot": snapshot}


@router.get("/consistency")
def get_consistency() -> dict[str, Any]:
    """核对每份快照：已归并数、归并结果条数和输出数据总量是否同源一致。"""
    return mergeflow.engine.consistency()


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出结果聚合归并清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "merge", "total": total, "items": [_task_view(item) for item in items]}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条聚合任务明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"聚合任务 {entry_id} 不存在或已归档")
    return _task_view(entry)


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条聚合任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    if entry is not None:
        mergeflow.engine.sync_task(entry)
    return ActionResult(ok=True, message="聚合任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条聚合任务执行启动归并、重试归并、归档任务。

    启动/重试会落在 services/mergeflow 的水位快照流程上；重复启动复用同一批次，
    不会创建第二个归并批次。可选参数：
      policy=freeze|extend           生成期间有新分片完成时的水位口径
      concurrent_shards=[序号或编号]  模拟生成期间又有分片完成
      simulate_write_error=true      模拟写归档清单异常，整批回滚
    """
    action = str(payload.values.get("action") or "").strip()
    if action not in {"启动归并", "重试归并", "归档任务"}:
        entry, message = service.run_action(entry_id, action)
        if entry is None:
            return ActionResult(ok=False, message=message)
        return ActionResult(ok=True, message=message, entry=entry)

    if action == "归档任务":
        entry, message = mergeflow.engine.archive_task(entry_id)
        if entry is None:
            return ActionResult(ok=False, message=message)
        return ActionResult(ok=True, message=message, entry=_task_view(entry))

    policy = payload.values.get("policy") or payload.values.get("水位策略")
    concurrent = payload.values.get("concurrent_shards") or payload.values.get("并发完成分片") or []
    if isinstance(concurrent, (str, int)):
        concurrent = [item for item in str(concurrent).replace("，", ",").split(",") if item.strip()]
    simulate_error = bool(
        payload.values.get("simulate_write_error")
        or payload.values.get("模拟写入异常")
    )
    runner = mergeflow.engine.start if action == "启动归并" else mergeflow.engine.retry
    task, snapshot, message, status = runner(
        entry_id,
        policy=policy,
        concurrent_shards=concurrent,
        simulate_write_error=simulate_error,
        force_new=bool(payload.values.get("force_new")),
    )
    if task is None:
        return ActionResult(ok=False, message=message, status=status)
    data: dict[str, Any] = {"snapshot": snapshot} if snapshot else {}
    if snapshot:
        data["results"] = mergeflow.engine.result_rows(snapshot["snapshot_id"])
        data["archive_entries"] = mergeflow.engine.archive_rows(snapshot["snapshot_id"])
    return ActionResult(
        ok=status != "failed",
        message=message,
        entry=_task_view(task),
        status=status,
        data=data,
    )
