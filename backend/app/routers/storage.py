"""采集存储分层接口：存储策略、归并闸门、分待办、复核清单与归档清单。

存储分层不重新计算归并口径，只读取 mergeflow 中已固定的快照版本：
按版本生成归档清单，版本冻结在哪个分片完成水位，清单就展示到哪个水位。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import mergeflow
from app.services.storage import StorageService

router = APIRouter(prefix="/api/storage", tags=["采集存储分层"])

service = StorageService()

LIST_FIELDS = ["策略编号", "存储层级", "文件格式", "压缩算法", "保留天数", "迁移规则", "清理方式", "策略状态"]
STATUSES = ["草稿", "已生效", "已暂停", "已关闭"]


def _policy_view(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    snapshot = None
    for candidate in mergeflow.engine.snapshots_for(plan_id=str(row.get("plan_id") or "")):
        snapshot = candidate
        break
    item.update({
        "归并快照版本": snapshot["snapshot_version"] if snapshot else "—",
        "快照水位": snapshot["分片完成水位"] if snapshot else "—",
        "快照已归并数": snapshot["已归并数"] if snapshot else "—",
        "快照数据总量": snapshot["输出数据总量"] if snapshot else "—",
    })
    return item


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、已生效、已暂停、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按策略编号与状态过滤采集存储分层列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=[_policy_view(item) for item in items], total=total, page=page, size=size)


@router.get("/gates")
def list_gates(plan_id: str | None = Query(default=None), status: str | None = Query(default=None)) -> dict[str, Any]:
    """新增归并闸门：按采集计划展示分片总数、完成水位、已归并数和校验结论。"""
    rows = mergeflow.engine.gate_rows()
    if plan_id:
        rows = [row for row in rows if str(row.get("plan_id")) == plan_id]
    if status:
        rows = [row for row in rows if row.get("闸门状态") == status]
    return {"items": rows, "total": len(rows)}


@router.get("/gates/{gate_id}")
def get_gate(gate_id: int) -> dict[str, Any]:
    """读取单个归并闸门及其当前快照的归档口径。"""
    row = next((item for item in mergeflow.engine.gate_rows() if int(item.get("id", 0)) == gate_id), None)
    if row is None:
        raise HTTPException(status_code=404, detail=f"归并闸门 {gate_id} 不存在")
    manifest, message = mergeflow.engine.manifest(row.get("snapshot_id"))
    return {"gate": row, "manifest": manifest, "message": message}


@router.get("/pending")
def list_pending(plan_id: str | None = Query(default=None)) -> dict[str, Any]:
    """分待办：展示当前水位之后还需要归并的分片数量。"""
    rows = mergeflow.engine.pending_rows()
    if plan_id:
        rows = [row for row in rows if str(row.get("plan_id")) == plan_id]
    return {"items": rows, "total": len(rows)}


@router.get("/reviews")
def list_reviews(
    plan_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    """分复核清单：归并快照的每一项闸门校验都要落到复核项上。"""
    rows = mergeflow.engine.review_rows()
    if plan_id:
        rows = [row for row in rows if str(row.get("plan_id")) == plan_id]
    if status:
        rows = [row for row in rows if row.get("复核状态") == status]
    rows = sorted(rows, key=lambda item: int(item.get("id", 0)), reverse=True)
    return {"items": rows, "total": len(rows)}


@router.post("/reviews/{review_id}/actions", response_model=ActionResult)
def run_review_action(review_id: int, payload: EntryPayload) -> ActionResult:
    """对复核项执行复核通过、标记异常、重新打开。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = mergeflow.engine.review_action(review_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/snapshots")
def list_snapshots(plan_id: str | None = Query(default=None)) -> dict[str, Any]:
    """存储可读取的归并快照版本列表，旧版本不会被新版本覆盖。"""
    rows = mergeflow.engine.snapshots_for(plan_id=plan_id)
    items = [
        {
            "snapshot_id": row["snapshot_id"],
            "snapshot_version": row["snapshot_version"],
            "版本": row["版本"],
            "plan_id": row["plan_id"],
            "采集计划": row["采集计划"],
            "任务编号": row["任务编号"],
            "分片完成水位": row["分片完成水位"],
            "启动水位": row["启动水位"],
            "最后确认水位": row["最后确认水位"],
            "水位策略": row["水位策略"],
            "已归并数": row["已归并数"],
            "输出数据总量": row["输出数据总量"],
            "归档条目数": row["归档条目数"],
            "生成时间": row["生成时间"],
        }
        for row in rows
    ]
    return {"items": items, "total": len(items)}


@router.get("/manifest")
def get_manifest(
    snapshot: str | None = Query(default=None, description="快照编号或版本，缺省取最新快照"),
) -> dict[str, Any]:
    """读取指定快照版本的归档清单；启动水位与延长后的水位都按快照原样展示。"""
    manifest, message = mergeflow.engine.manifest(snapshot)
    if manifest is None:
        raise HTTPException(status_code=404, detail=message)
    return manifest


@router.post("/manifest", response_model=ActionResult)
def build_manifest(payload: EntryPayload) -> ActionResult:
    """按快照版本生成归档清单；重复生成幂等复用，不会产生第二份。"""
    reference = str(
        payload.values.get("snapshot")
        or payload.values.get("snapshot_version")
        or payload.values.get("快照版本")
        or ""
    ).strip()
    if not reference:
        latest = mergeflow.engine.snapshots_for()
        if not latest:
            return ActionResult(ok=False, message="当前没有归并快照，无法生成归档清单")
        reference = str(latest[0]["snapshot_version"])
    manifest, message = mergeflow.engine.ensure_manifest(reference)
    if manifest is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, data=manifest)


@router.get("/archives")
def list_archives(
    snapshot: str | None = Query(default=None),
    plan_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """归档清单明细：只返回指定快照版本下的条目，不复用其它版本的记录。"""
    items = mergeflow.engine.archive_rows(snapshot, plan_id)
    return {"items": items, "total": len(items), "snapshot": snapshot}


@router.get("/results")
def list_results(snapshot: str | None = Query(default=None)) -> dict[str, Any]:
    """存储侧可见的归并结果数据，按快照版本读取。"""
    items = mergeflow.engine.result_rows(snapshot)
    return {"items": items, "total": len(items), "snapshot": snapshot}


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出采集存储分层清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "storage", "total": total, "items": [_policy_view(item) for item in items]}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条存储策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"存储策略 {entry_id} 不存在或已归档")
    return _policy_view(entry)


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条存储策略，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="存储策略已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条存储策略执行启用策略、暂停策略、关闭策略；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
