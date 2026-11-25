"""演练结束后的查询视图。

回答四类问题：
1. 每项恢复交付了哪组一致内容（或为何失败、备选是什么）；
2. 哪些块造成了回退（损坏、删除、未读完）；
3. 此刻还有哪些资源被占用（pin、本地字节、作业槽）；
4. 清理责任人：这批过期对象何时可以继续处理。
"""
from __future__ import annotations

from typing import Any

from .controller import Controller, RequestStatus
from .feeds import FeedBundle
from .store import BlockState


class DrillReport:
    def __init__(self, controller: Controller):
        self.ctl = controller
        self.bundle: FeedBundle = controller.bundle

    # ---- 1. 恢复交付视图 -----------------------------------------------

    def recoveries(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for ref in sorted(self.ctl.requests):
            state = self.ctl.requests[ref]
            req = state.request
            row: dict[str, Any] = {
                "reference": ref,
                "kind": "restore",
                "requested_at": req.at,
                "members": list(req.members),
                "earliest_point": req.earliest_point,
                "deadline": req.deadline,
                "status": state.status.value,
                "history": list(state.history),
            }
            if state.status == RequestStatus.DELIVERED:
                point = state.current_point
                row.update({
                    "delivered_point": point.point_id,
                    "business_minute": point.business_minute,
                    "delivered_at": state.delivered_at,
                    "on_time": state.delivered_at <= req.deadline,
                    "delay": max(0, state.delivered_at - req.deadline),
                    "service_leaves": {sp.service: sp.leaf for sp in point.services},
                    "blocks": list(point.blocks),
                    "content": state.delivered_content,
                    "consistency": self._consistency_note(point),
                })
            elif state.status == RequestStatus.FAILED:
                plan = state.plan
                row.update({
                    "failure_reasons": list(plan.failure_reasons),
                    "alternatives": self.ctl._alternative_summary(plan),
                })
            elif state.status == RequestStatus.CANCELED:
                row["canceled_at"] = next(
                    (d.at for d in self.ctl.decisions
                     if d.kind == "restore_canceled" and d.reference == ref), None,
                )
            if state.plan is not None and state.plan.time_loss and state.status != RequestStatus.FAILED:
                row["time_loss"] = state.plan.time_loss
            if state.deadline_breached_at is not None:
                row["deadline_breached_at"] = state.deadline_breached_at
            rows.append(row)
        return rows

    def _consistency_note(self, point) -> str:
        if point.group_marks:
            return (
                "组内成员在同一 mark "
                f"{', '.join(point.group_marks)} 共同启用，各服务文档由已核验链重建"
            )
        return "独立服务文档由已核验块链重建"

    # ---- 2. 回退责任视图 ------------------------------------------------

    def rollback_causes(self) -> list[dict[str, Any]]:
        causes: list[dict[str, Any]] = []
        for decision in self.ctl.decisions:
            if decision.kind == "verify_failed":
                causes.append({
                    "minute": decision.at,
                    "cause": "checksum_failure",
                    "object": decision.detail["object"],
                    "expected": decision.detail["digest_expected"],
                    "actual": decision.detail["digest_actual"],
                    "impact_requests": self._impacted_by(decision),
                })
            elif decision.kind == "restore_replanned":
                causes.append({
                    "minute": decision.at,
                    "cause": "rollback",
                    "reference": decision.reference,
                    "from_point": decision.detail["old_point"],
                    "to_point": decision.detail["new_point"],
                    "time_loss": decision.detail["time_loss"],
                    "rolled_back_blocks": decision.detail["rolled_back_blocks"],
                    "reason": decision.detail["cause"],
                })
            elif decision.kind in ("delete_claimed", "delete_confirmed"):
                causes.append({
                    "minute": decision.at,
                    "cause": decision.kind,
                    "object": decision.detail["object"],
                    "pins_at_that_time": decision.detail.get("pins", []),
                    "note": decision.detail.get("note", ""),
                })
        return causes

    def _impacted_by(self, decision) -> list[str]:
        for later in self.ctl.decisions:
            if later.at == decision.at and later.kind == "corruption":
                return list(later.detail["affected_requests"])
        return []

    # ---- 3. 资源占用视图 ------------------------------------------------

    def resource_occupancy(self) -> dict[str, Any]:
        blocks = []
        for object_id in sorted(self.bundle.blocks):
            rec = self.ctl.store.record(object_id)
            blocks.append({
                "object": object_id,
                "state": rec.state.value,
                "occupies_space": rec.occupies_space,
                "pins": rec.pinned_by(),
                "verified_local": rec.usable_verified and rec.evicted_at is None,
                "verified_at": rec.verified_at if rec.evicted_at is None else None,
                "fetch_ready_at": rec.fetch_ready_at,
                "claimed_at": rec.claimed_at,
                "deleted_at": rec.deleted_at,
                "evicted_at": rec.evicted_at,
                "bad_reason": rec.bad_reason,
            })
        return {
            "free_units": self.ctl.store.free_units,
            "minimum_free_space": self.ctl.store._min_free(),
            "space_pressure_events": list(self.ctl.storage_events),
            "blocks": blocks,
            "active_pins": {
                object_id: rec.pinned_by()
                for object_id, rec in self.ctl.store.records.items() if rec.pins
            },
            "scheduler": self.ctl.scheduler.snapshot(),
        }

    # ---- 4. 清理责任人视图 ----------------------------------------------

    def cleanup_schedule(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for ref in sorted(self.ctl.cleanups):
            state = self.ctl.cleanups[ref]
            rec = self.ctl.store.record(state.request.object)
            rows.append({
                "reference": ref,
                "object": state.request.object,
                "retire_min": self.bundle.blocks[state.request.object].retire_min,
                "status": state.status.value,
                "claimed_at": state.claimed_at,
                "deleted_at": state.deleted_at,
                "blocked_by": list(state.blocked_by),
                "protected_until": state.protected_until,
                "warehouse_actionable_at": self._warehouse_actionable(state),
                "local_evictable_at": state.releasable_at,
                "note": state.note,
                "remote_state": rec.state.value,
            })
        # 同时给出全部已过保留期对象（即使本批没有对应工单）。
        horizon = self.ctl.now
        work_ordered = {row["object"] for row in rows}
        extra: list[dict[str, Any]] = []
        for object_id, block in sorted(self.bundle.blocks.items(), key=lambda kv: kv[1].retire_min):
            if block.retire_min > horizon or object_id in work_ordered:
                continue
            rec = self.ctl.store.record(object_id)
            extra.append({
                "object": object_id,
                "retire_min": block.retire_min,
                "state": rec.state.value,
                "pins": rec.pinned_by(),
                "evictable_at": state_evictable(self.ctl, object_id),
            })
        return rows + [{"reference": None, **item} for item in extra]

    def _warehouse_actionable(self, state) -> int | None:
        """仓储删除最早可执行分钟：未读完依赖被保护到核验完成。"""
        object_id = state.request.object
        rec = self.ctl.store.record(object_id)
        if rec.state in (BlockState.CLAIMED_REMOTE, BlockState.DELETED_REMOTE):
            return state.claimed_at or state.deleted_at
        if not state.blocked_by:
            return max(state.request.at,
                       self.bundle.blocks[object_id].retire_min)
        finishes = []
        for ref in state.blocked_by:
            req_state = self.ctl.requests.get(ref)
            if req_state is not None and req_state.current_point is not None:
                finishes.append(req_state.eta or self.ctl.now)
        return max(finishes) if finishes else self.ctl.now

    # ---- 全量决定流 -----------------------------------------------------

    def decisions(self) -> list[dict[str, Any]]:
        return [
            {"at": d.at, "kind": d.kind, "reference": d.reference, "detail": d.detail}
            for d in self.ctl.decisions
        ]

    def as_dict(self) -> dict[str, Any]:
        return {
            "horizon": self.ctl.now,
            "recoveries": self.recoveries(),
            "rollback_causes": self.rollback_causes(),
            "resource_occupancy": self.resource_occupancy(),
            "cleanup_schedule": self.cleanup_schedule(),
            "decisions": self.decisions(),
        }


def state_evictable(ctl: Controller, object_id: str) -> int:
    rec = ctl.store.record(object_id)
    if not rec.pins:
        return ctl.now
    finishes = []
    for ref in rec.pinned_by():
        state = ctl.requests.get(ref)
        if state is not None:
            finishes.append(state.delivered_at or state.eta or ctl.now)
    return max(finishes) if finishes else ctl.now
