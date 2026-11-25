"""控制程序语义测试。

用资料目录做端到端演练，并用最小构造数据覆盖边界规则。测试不依赖外网，
全部使用标准库。
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from drillctl.controller import (
    CleanupStatus, Controller, RequestStatus,
)
from drillctl.feeds import Block, FeedError, load_bundle
from drillctl.planning import Planner
from drillctl.scheduler import Job, JobKind, Scheduler
from drillctl.store import BlockState, BlockStore
from drillctl.topology import Topology
from drillctl.report import DrillReport

MATERIALS = Path(__file__).resolve().parent.parent / "materials"


def load():
    return load_bundle(MATERIALS)


class FeedTest(unittest.TestCase):
    def test_loads_and_detects_corrupt_digest(self):
        bundle = load()
        self.assertEqual(bundle.actual_digests["cfg2"] != bundle.blocks["cfg2"].digest, True)
        for object_id in ("base-c", "cfg1", "idx3", "rules0"):
            self.assertEqual(
                bundle.actual_digests[object_id], bundle.blocks[object_id].digest,
            )

    def test_rejects_bad_feed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "objects").mkdir()
            for name in ("services", "requests", "activity"):
                (root / f"{name}.jsonl").write_text("", encoding="utf-8")
            (root / "manifest.jsonl").write_text(
                json.dumps({
                    "object": "x", "service": "s", "group": None, "mark": None,
                    "minute": 0, "tier": "hot", "parent": None, "retrieve_min": 1,
                    "restore_min": 1, "retire_min": 10, "digest": "z" * 64,
                }), encoding="utf-8")
            with self.assertRaises(FeedError):
                load_bundle(root)


class TopologyTest(unittest.TestCase):
    def setUp(self):
        self.bundle = load()
        self.topo = Topology(self.bundle)

    def test_chain_closure_and_group_marks(self):
        self.assertEqual(self.topo.chain("config"),
                         ["base-c", "cfg1", "cfg2", "cfg3"])
        self.assertEqual(self.topo.closure("cfg2"), ("base-c", "cfg1", "cfg2"))
        marks = self.topo.group_marks("sync")
        self.assertEqual([c.mark for c in marks], ["k0", "k1", "k2", "k3"])
        self.assertEqual(set(marks[2].leaves), {"config", "index"})

    def test_reconstruct_applies_set_and_remove(self):
        # idx 链到 idx2：lane 被 remove，ref/segment 被覆盖。
        doc = self.topo.reconstruct("idx2")
        self.assertEqual(doc, {"ref": "routine-c", "segment": 43})


class PlanningTest(unittest.TestCase):
    def setUp(self):
        self.bundle = load()
        self.topo = Topology(self.bundle)
        self.store = BlockStore(self.bundle, initial_free=12)
        self.planner = Planner(self.bundle, self.topo, self.store)

    def test_candidates_new_to_old_and_group_atomic(self):
        points = self.planner.candidate_points(("config", "index"))
        self.assertEqual([p.business_minute for p in points], [30, 20, 10, 0])
        # 每个恢复点里 config/index 必须同 mark。
        for point in points:
            marks = {sp.mark for sp in point.services}
            self.assertEqual(len(marks), 1)

    def test_cross_unit_members_align_timeline(self):
        points = self.planner.candidate_points(("config", "index", "rules"))
        # rules0 在第 8 分钟才有；8 分钟时组服务回退到 k0@0 仍构成一致点。
        self.assertEqual(points[-1].business_minute, 8)
        services = {sp.service for sp in points[0].services}
        self.assertEqual(services, {"config", "index", "rules"})
        # 10 分钟起 rules0 与 k1 对齐。
        self.assertIn(10, [p.business_minute for p in points])

    def test_corruption_forces_whole_group_rollback(self):
        # 把 cfg2 判坏：k3/k2 都不可行，组内不能只替换 config，整组回退到 k1。
        self.store.issue_fetch("cfg2", 0, "f")
        self.store.mark_bytes_ready("cfg2", 1, "f")
        self.assertFalse(self.store.verify("cfg2", 1))
        plan = self.planner.plan("dr", ("config", "index"), earliest_point=0, deadline=99)
        self.assertEqual(plan.chosen.point_id, "mark:k1")
        self.assertEqual(plan.time_loss, 20)
        self.assertIn("cfg2", plan.evaluations[0].blockers)

    def test_deleted_remote_with_local_copy_still_feasible(self):
        self.store.issue_fetch("base-i", 0, "f")
        self.store.mark_bytes_ready("base-i", 1, "f")
        self.assertTrue(self.store.verify("base-i", 1))
        self.store.claim_delete("base-i", 5)
        self.store.confirm_delete("base-i", 6)
        plan = self.planner.plan("dr", ("config", "index"), earliest_point=0, deadline=99)
        # 本地核验副本仍可支撑既有恢复。
        self.assertTrue(any(
            o == "base-i" and plan.chosen_evaluation.verdicts[o].eligible
            for o in plan.chosen.blocks
        ))

    def test_failure_when_earliest_too_old(self):
        plan = self.planner.plan("dr", ("rules",), earliest_point=99, deadline=100)
        self.assertIsNone(plan.chosen)
        self.assertTrue(plan.failure_reasons)


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.bundle = load()
        self.store = BlockStore(self.bundle, initial_free=4)

    def test_fetch_consumes_space_and_pins_protect(self):
        self.store.issue_fetch("cfg1", 0, "f1")
        self.assertEqual(self.store.free_units, 3)
        self.store.mark_bytes_ready("cfg1", 1, "f1")
        self.assertTrue(self.store.verify("cfg1", 1))
        self.store.pin("cfg1", "dr9")
        # 有 pin 时不可驱逐。
        self.assertFalse(self.store.evict_if_unpinned("cfg1", 2))
        self.store.unpin_request("dr9")
        self.assertTrue(self.store.evict_if_unpinned("cfg1", 3))
        self.assertEqual(self.store.free_units, 4)

    def test_pressure_eviction_respects_pins_and_min_free(self):
        for object_id in ("cfg1", "idx1", "rules0"):
            self.store.issue_fetch(object_id, 0, f"f-{object_id}")
            self.store.mark_bytes_ready(object_id, 1, "r")
            self.store.verify(object_id, 1)
        self.assertEqual(self.store.free_units, 1)  # 4-3=1，低于保留线 2
        self.store.pin("cfg1", "dr9")
        evicted = self.store.evict_unpinned_to_relieve_pressure(2)
        self.assertNotIn("cfg1", evicted)  # 受保护
        self.assertIn("idx1", evicted)
        self.assertGreaterEqual(self.store.free_units, 2)

    def test_claimed_remote_blocks_new_promise_without_local_copy(self):
        self.store.claim_delete("idx2", 3)
        ok, reason = self.store.eligible_for_new_promise("idx2")
        self.assertFalse(ok)
        self.assertIn("领取/删除", reason)


class SchedulerTest(unittest.TestCase):
    def setUp(self):
        self.bundle = load()
        self.store = BlockStore(self.bundle, initial_free=12)
        self.scheduler = Scheduler(self.bundle, self.store, now=0)

    def test_cold_fetch_waits_for_receipt_hot_fetch_completes_by_clock(self):
        # base-c 是冷层（external），cfg1 是热层（内部倒计时）。
        self.scheduler.request_fetch("shared", "base-c")
        self.scheduler.request_fetch("shared", "cfg1")
        self.scheduler.tick(0)
        kinds = {j.object_id: j.external for j in self.scheduler.running}
        self.assertTrue(kinds["base-c"])
        self.assertFalse(kinds["cfg1"])
        # 第 2 分钟：热层 cfg1（retrieve 2）倒计时完成；冷层 base-c 不被倒计时结束。
        done = self.scheduler.complete(2)
        self.assertEqual([j.object_id for j in done], ["cfg1"])
        self.assertIn("base-c", [j.object_id for j in self.scheduler.running])
        # 冷层靠回执结束。
        self.scheduler.adopt_external_fetch("base-c", 11, "fetch-1")
        self.assertNotIn("base-c", [j.object_id for j in self.scheduler.running])

    def test_shared_restore_runs_once_for_two_requests(self):
        for object_id in ("base-c", "cfg1"):
            self.store.issue_fetch(object_id, 0, "f")
            self.store.mark_bytes_ready(object_id, 1, "r")
            self.store.verify(object_id, 1)
        self.scheduler.request_restore("dr1", "base-c")
        self.scheduler.request_restore("dr2", "base-c")  # 同块去重
        self.scheduler.tick(1)
        self.assertEqual(
            [j.object_id for j in self.scheduler.running if j.kind == JobKind.RESTORE],
            ["base-c"],
        )


class ControllerEndToEndTest(unittest.TestCase):
    def setUp(self):
        self.bundle = load()

    def _run(self):
        ctl = Controller(self.bundle, initial_free=11, start_minute=32)
        ctl.run(62)
        return ctl

    def test_full_drill_outcomes(self):
        ctl = self._run()
        self.assertEqual(ctl.requests["dr1"].status, RequestStatus.CANCELED)
        dr2 = ctl.requests["dr2"]
        dr3 = ctl.requests["dr3"]
        self.assertEqual(dr2.status, RequestStatus.DELIVERED)
        self.assertEqual(dr3.status, RequestStatus.DELIVERED)
        # 整组回退到 k1，业务时间损失 20。
        self.assertEqual(dr2.current_point.point_id, "mark:k1")
        self.assertEqual(dr3.current_point.business_minute, 10)
        # 都在 deadline 内。
        self.assertLessEqual(dr2.delivered_at, dr2.request.deadline)
        self.assertLessEqual(dr3.delivered_at, dr3.request.deadline)
        # 交付文档由已核验字节重建。
        self.assertEqual(dr2.delivered_content["config"]["model"], "routine-b")

    def test_corruption_event_and_group_replan(self):
        ctl = self._run()
        failures = [d for d in ctl.decisions if d.kind == "verify_failed"]
        self.assertEqual({d.detail["object"] for d in failures}, {"cfg2"})
        replans = [d for d in ctl.decisions if d.kind == "restore_replanned"]
        self.assertEqual({d.reference for d in replans}, {"dr2", "dr3"})
        for d in replans:
            self.assertEqual(d.detail["rolled_back_blocks"],
                             ["cfg2", "cfg3", "idx2", "idx3"])

    def test_delete_after_local_verify_preserves_delivery(self):
        ctl = self._run()
        rec = ctl.store.record("base-i")
        # 删除回执到达时本地已有核验副本，交付得以完成；交付释放 pin 后本地副本被驱逐。
        self.assertIn(rec.state, (BlockState.DELETED_REMOTE, BlockState.ABSENT))
        self.assertEqual(rec.pinned_by(), ())
        # 删除回执事实仍记录在案。
        self.assertIsNotNone(rec.deleted_at)
        # dr2/dr3 都已交付，没有因删除而失败。
        self.assertEqual(ctl.requests["dr2"].status, RequestStatus.DELIVERED)
        self.assertEqual(ctl.requests["dr3"].status, RequestStatus.DELIVERED)

    def test_cleanup_protection_window(self):
        ctl = self._run()
        del3 = ctl.cleanups["del3"]
        # idx1 在核验完成前受保护，之后清理可继续。
        self.assertIn(del3.status,
                      (CleanupStatus.WAITING, CleanupStatus.RELEASABLE))
        self.assertIsNotNone(del3.releasable_at)

    def test_reconcile_matches_continuous_run_at_every_cutoff(self):
        full = self._run()
        full_states = {
            ref: (s.status.value,
                  s.current_point.point_id if s.current_point else None,
                  s.delivered_at)
            for ref, s in full.requests.items()
        }
        for cutoff in (35, 45, 55, 60):
            part = Controller(self.bundle, 11, 32)
            part.run(cutoff, settle=False)
            journal = part.journal()
            resumed = Controller(self.bundle, 11, cutoff)
            resumed.reconcile(journal, cutoff)
            resumed.run(62)
            resumed_states = {
                ref: (s.status.value,
                      s.current_point.point_id if s.current_point else None,
                      s.delivered_at)
                for ref, s in resumed.requests.items()
            }
            self.assertEqual(full_states, resumed_states, f"cutoff={cutoff} 不一致")

    def test_replayed_fetch_receipt_does_not_refetch(self):
        ctl = self._run()
        # 62 分钟的 fetch-2/cfg2 是中断重放，应被识别为忽略而非再次判坏。
        ignored = [d for d in ctl.decisions if d.kind in (
            "fetch_ready_replay_ignored", "fetch_ready_stale_ignored",
        )]
        self.assertTrue(ignored)
        # cfg2 只产生一次实际校验失败。
        self.assertEqual(
            len([d for d in ctl.decisions
                 if d.kind == "verify_failed" and d.detail["object"] == "cfg2"]),
            1,
        )

    def test_report_views_are_complete(self):
        ctl = self._run()
        report = DrillReport(ctl).as_dict()
        self.assertEqual(len(report["recoveries"]), 3)
        self.assertTrue(report["rollback_causes"])
        self.assertIn("active_pins", report["resource_occupancy"])
        self.assertTrue(
            any(item["object"] == "base-i" for item in report["cleanup_schedule"]),
        )

    def test_impossible_earliest_point_leaves_failure_evidence(self):
        ctl = Controller(self.bundle, initial_free=11, start_minute=32)
        from drillctl.feeds import Request
        impossible = Request(
            at=40, kind="restore", reference="drImp",
            members=("config", "index"), earliest_point=99, deadline=120, object=None,
        )
        ctl.bundle.requests = ctl.bundle.requests + (impossible,)
        ctl.run(62)
        state = ctl.requests["drImp"]
        self.assertEqual(state.status, RequestStatus.FAILED)
        self.assertTrue(state.plan.failure_reasons)
        decision = next(d for d in ctl.decisions
                        if d.kind == "restore_rejected" and d.reference == "drImp")
        self.assertIn("99", decision.detail["reasons"][0])

    def test_deadline_breach_is_recorded_but_work_continues(self):
        ctl = Controller(self.bundle, initial_free=11, start_minute=32)
        from drillctl.feeds import Request
        tight = Request(
            at=40, kind="restore", reference="drLate",
            members=("config", "index"), earliest_point=0, deadline=42, object=None,
        )
        ctl.bundle.requests = ctl.bundle.requests + (tight,)
        ctl.run(62)
        state = ctl.requests["drLate"]
        self.assertEqual(state.deadline_breached_at, 42)
        # 延期不等于失败：资源到位后仍交付，并标明超时。
        self.assertEqual(state.status, RequestStatus.DELIVERED)
        self.assertGreater(state.delivered_at, 42)


if __name__ == "__main__":
    unittest.main()
