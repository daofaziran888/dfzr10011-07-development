"""归并链路自测：水位、闸门、快照、归档清单和失败回滚。

运行：cd backend && python3 -m unittest discover -s tests -v
这里直接测服务层，不依赖 httpx / TestClient；每个用例开始前把示例数据复原。
"""
from __future__ import annotations

import copy
import unittest

from app.seed import SEED_ROWS
from app.services import mergeflow
from app.store import store

MODULE_MERGE = "merge"
MODULE_SHARD = "shard"
MODULE_STORAGE = "storage"


class MergeFlowTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._backup = copy.deepcopy(store._tables)
        for name in (MODULE_SHARD, MODULE_MERGE, MODULE_STORAGE):
            store._tables[name] = copy.deepcopy(SEED_ROWS[name])
        self.engine = mergeflow.MergeFlowEngine()

    def tearDown(self) -> None:
        store._tables = self._backup

    def _task(self, task_id: int) -> dict:
        return store.find(MODULE_MERGE, task_id)

    def test_watermark_stops_at_first_gap(self) -> None:
        summary = self.engine.summarize("PLAN-0002")
        self.assertEqual(summary["分片总数"], 6)
        self.assertEqual(summary["分片完成水位"], 4)
        self.assertEqual(summary["已归并数"], 4)
        self.assertEqual(summary["输出数据总量"], 4550)
        self.assertEqual(summary["缺口"], [5])
        self.assertEqual(summary["下一批待归并数"], 2)

    def test_empty_plan_and_empty_shard(self) -> None:
        empty_plan = self.engine.summarize("PLAN-0003")
        self.assertEqual(empty_plan["分片总数"], 0)
        self.assertEqual(empty_plan["分片完成水位"], 0)
        self.assertEqual(empty_plan["输出数据总量"], 0)
        self.assertTrue(empty_plan["空计划"])

        plan_one = self.engine.summarize("PLAN-0001")
        self.assertEqual(plan_one["分片完成水位"], 8)
        self.assertEqual(plan_one["空分片数"], 1)
        self.assertEqual(plan_one["输出数据总量"], 8480)
        empty = [row for row in plan_one["完成记录"] if row["空分片"]]
        self.assertEqual([row["分片编号"] for row in empty], ["SHAR-1008"])

    def test_freeze_keeps_start_watermark_and_pushes_rest_to_next_batch(self) -> None:
        task, snapshot, _, status = self.engine.start(
            4, policy="freeze", concurrent_shards=[5, 6]
        )
        self.assertIsNotNone(snapshot)
        self.assertEqual(status, "conflict")
        self.assertEqual(snapshot["启动水位"], 4)
        self.assertEqual(snapshot["最后确认水位"], 6)
        self.assertEqual(snapshot["分片完成水位"], 4)
        self.assertEqual(snapshot["已归并数"], 4)
        self.assertEqual(snapshot["输出数据总量"], 2600)
        self.assertEqual(snapshot["归档条目数"], 4)
        self.assertEqual(snapshot["下一批待归并数"], 6)

        archives = self.engine.archive_rows(snapshot["snapshot_id"])
        self.assertEqual([item["分片序号"] for item in archives], [1, 2, 3, 4])
        self.assertEqual(sum(item["记录数"] for item in archives), snapshot["输出数据总量"])

        pending = next(row for row in self.engine.pending_rows() if row["plan_id"] == "PLAN-0004")
        self.assertEqual(pending["当前水位"], 4)
        self.assertEqual(pending["下一批待归并数"], 6)

    def test_extend_takes_final_watermark(self) -> None:
        task, snapshot, _, status = self.engine.start(
            4, policy="extend", concurrent_shards=[5, 6]
        )
        self.assertEqual(status, "conflict")
        self.assertEqual(snapshot["启动水位"], 4)
        self.assertEqual(snapshot["最后确认水位"], 6)
        self.assertEqual(snapshot["分片完成水位"], 6)
        self.assertEqual(snapshot["已归并数"], 6)
        self.assertEqual(snapshot["输出数据总量"], 4200)
        self.assertEqual(snapshot["归档条目数"], 6)
        self.assertEqual(snapshot["下一批待归并数"], 4)

        archives = self.engine.archive_rows(snapshot["snapshot_id"])
        self.assertEqual([item["分片序号"] for item in archives], [1, 2, 3, 4, 5, 6])
        self.assertEqual(len(self.engine.result_rows(snapshot["snapshot_id"])), 6)

    def test_snapshot_numbers_share_one_watermark(self) -> None:
        _, snapshot, _, _ = self.engine.start(4, policy="freeze", concurrent_shards=[5, 6])
        self.assertEqual(snapshot["已归并数"], snapshot["分片完成水位"])
        self.assertEqual(
            snapshot["输出数据总量"],
            sum(row["记录数"] for row in self.engine.result_rows(snapshot["snapshot_id"])),
        )
        self.assertEqual(
            snapshot["输出数据总量"],
            sum(row["记录数"] for row in self.engine.archive_rows(snapshot["snapshot_id"])),
        )
        self.assertTrue(all(item["一致"] for item in self.engine.consistency()["items"]))

    def test_repeat_start_does_not_create_second_batch(self) -> None:
        _, first, _, _ = self.engine.start(4, policy="freeze")
        batch_id = self.engine.batch_for(4)["batch_id"]
        _, second, message, status = self.engine.start(4, policy="freeze")
        self.assertEqual(status, "conflict")
        self.assertIn("已有快照", message)
        self.assertEqual(second["snapshot_id"], first["snapshot_id"])
        self.assertEqual(self.engine.batch_for(4)["batch_id"], batch_id)
        self.assertEqual(len(self.engine.snapshots_for(task_id=4)), 1)

    def test_write_error_leaves_no_half_snapshot(self) -> None:
        self.engine.pending_rows()  # 先完成启动初始化（预置快照），再比较写入前后的规模
        before = (len(self.engine.snapshots), len(self.engine.merge_results), len(self.engine.archive_entries))
        _, snapshot, message, status = self.engine.start(4, policy="freeze", simulate_write_error=True)
        self.assertIsNone(snapshot)
        self.assertEqual(status, "failed")
        self.assertIn("回滚", message)
        after = (len(self.engine.snapshots), len(self.engine.merge_results), len(self.engine.archive_entries))
        self.assertEqual(before, after)
        gate = next(row for row in self.engine.gate_rows() if row["plan_id"] == "PLAN-0004")
        self.assertEqual(gate["闸门状态"], "写入失败")

        _, retried, _, retry_status = self.engine.retry(4, policy="freeze")
        self.assertEqual(retry_status, "ok")
        self.assertIsNotNone(retried)
        self.assertEqual(len(self.engine.archive_rows(retried["snapshot_id"])), retried["已归并数"])
        self.assertEqual(len(self.engine.result_rows(retried["snapshot_id"])), retried["已归并数"])

    def test_retry_blocks_on_open_gap(self) -> None:
        _, snapshot, message, status = self.engine.retry(2, policy="freeze")
        self.assertIsNone(snapshot)
        self.assertEqual(status, "failed")
        self.assertIn("缺口", message)
        self.assertEqual(len(self.engine.snapshots_for(task_id=2)), 1)

    def test_retry_continues_after_gap_closed(self) -> None:
        self.engine.pending_rows()  # 先按启动时的数据完成初始化，保留水位 4 的旧快照
        shard = next(row for row in store.rows(MODULE_SHARD) if row["分片编号"] == "SHAR-2005")
        shard["分片状态"] = "已完成"
        shard["status"] = "已完成"
        shard["记录数"] = 900
        _, snapshot, _, status = self.engine.retry(2, policy="freeze")
        self.assertEqual(status, "ok")
        self.assertEqual(snapshot["分片完成水位"], 6)
        self.assertEqual(snapshot["续跑起点"], 5)
        self.assertEqual(snapshot["已归并数"], 6)
        self.assertEqual(snapshot["输出数据总量"], 4550 + 900 + 860)

    def test_old_snapshot_keeps_original_basis(self) -> None:
        old = self.engine.latest_snapshot(2)
        old_numbers = (old["版本"], old["分片完成水位"], old["已归并数"], old["输出数据总量"])
        shard = next(row for row in store.rows(MODULE_SHARD) if row["分片编号"] == "SHAR-2005")
        shard["分片状态"] = "已完成"
        shard["status"] = "已完成"
        shard["记录数"] = 900
        _, new_snapshot, _, _ = self.engine.retry(2, policy="freeze")
        self.assertNotEqual(new_snapshot["snapshot_id"], old["snapshot_id"])
        self.assertEqual(
            (old["版本"], old["分片完成水位"], old["已归并数"], old["输出数据总量"]),
            old_numbers,
        )
        manifest, _ = self.engine.manifest(old["snapshot_version"])
        self.assertEqual(manifest["快照水位"], 4)
        self.assertEqual(manifest["已归并数"], 4)
        self.assertEqual(manifest["输出数据总量"], 4550)
        self.assertEqual(manifest["归档条目数"], 4)

    def test_manifest_is_scoped_to_snapshot_version(self) -> None:
        _, first, _, _ = self.engine.start(4, policy="freeze")
        _, second, _, _ = self.engine.retry(4, policy="extend", concurrent_shards=[5, 6])
        manifest_one, _ = self.engine.manifest(first["snapshot_version"])
        manifest_two, _ = self.engine.manifest(second["snapshot_version"])
        self.assertEqual(manifest_one["归档条目数"], 4)
        self.assertEqual(manifest_two["归档条目数"], 6)
        self.assertEqual(len(manifest_one["items"]), 4)
        self.assertEqual(len(manifest_two["items"]), 6)
        self.assertNotEqual(
            {item["archive_id"] for item in manifest_one["items"]},
            {item["archive_id"] for item in manifest_two["items"]},
        )

    def test_gate_and_review_follow_snapshot(self) -> None:
        _, snapshot, _, _ = self.engine.start(4, policy="freeze")
        gate = next(row for row in self.engine.gate_rows() if row["plan_id"] == "PLAN-0004")
        self.assertEqual(gate["快照版本"], snapshot["版本"])
        self.assertEqual(gate["已归并数"], snapshot["已归并数"])
        self.assertEqual(gate["数据总量"], snapshot["输出数据总量"])
        reviews = [row for row in self.engine.review_rows() if row["snapshot_id"] == snapshot["snapshot_id"]]
        self.assertEqual(len(reviews), len(snapshot["校验项"]))
        self.assertTrue(all(row["复核依据"] for row in reviews))


if __name__ == "__main__":
    unittest.main()
