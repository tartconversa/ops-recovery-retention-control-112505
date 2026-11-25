"""事件驱动的恢复/清理控制器。

按演练分钟推进，依次处理：空间回执 -> 冷层召回回执 -> 删除回执 -> 工单，
再派发作业并推进作业槽。所有决定（接纳、回退、保护、失败、交付、删除放行）
都写入 decisions，并通过 journal 持久化，重启时先依据 journal 与 activity
对账已经发生的外部效果，再继续推进。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .feeds import FeedBundle, Receipt, Request
from .planning import Plan, Planner, PointEvaluation, RecoveryPoint
from .scheduler import Job, JobKind, Scheduler
from .store import BlockState, BlockStore
from .topology import Topology


class RequestStatus(str, Enum):
    PENDING = "pending"          # 已接纳，等待依赖与还原
    DELIVERED = "delivered"      # 一致内容已还原交付
    FAILED = "failed"            # 到最旧时间也无法满足
    CANCELED = "canceled"        # 被 cancel 工单撤销


class CleanupStatus(str, Enum):
    WAITING = "waiting"          # 保留期已到，但仍受承诺保护或尚未领取
    CLAIMED = "claimed"          # 清理端已领取，仓储承诺关闭
    DELETED = "deleted"          # 删除回执已到，本地副本按 pin 保留
    RELEASABLE = "releasable"    # 本地也可驱逐，清理责任人可继续处理


@dataclass
class RequestState:
    request: Request
    status: RequestStatus
    plan: Plan | None = None
    current_point: RecoveryPoint | None = None
    delivered_at: int | None = None
    delivered_content: dict[str, dict[str, object]] = field(default_factory=dict)
    deadline_breached_at: int | None = None
    history: list[str] = field(default_factory=list)
    eta: int | None = None


@dataclass
class CleanupState:
    request: Request
    status: CleanupStatus
    claimed_at: int | None = None
    deleted_at: int | None = None
    blocked_by: tuple[str, ...] = ()
    """仍需读完的实际依赖（未核验的 pin 块）。"""
    protected_until: int | None = None
    """承诺保护窗口结束分钟：最后一个未读完依赖核验完成的分钟。"""
    releasable_at: int | None = None
    note: str = ""


@dataclass
class Decision:
    at: int
    kind: str
    reference: str
    detail: dict[str, Any]


class Controller:
    def __init__(self, bundle: FeedBundle, initial_free: int, start_minute: int):
        self.bundle = bundle
        self.topo = Topology(bundle)
        self.store = BlockStore(bundle, initial_free)
        self.scheduler = Scheduler(bundle, self.store, start_minute)
        self.planner = Planner(bundle, self.topo, self.store)
        self.now = start_minute
        self.requests: dict[str, RequestState] = {}
        self.cleanups: dict[str, CleanupState] = {}
        self.decisions: list[Decision] = []
        self.receipts_seen: set[tuple[int, str, str, str | None]] = set()
        self.known_fetch_refs: set[tuple[str, str]] = set()
        """已处理过的 (fetch 引用, 对象)；重放回执按重复处理，不重新召回。"""
        self.journal_cursor = 0
        self.storage_events: list[tuple[int, int]] = []

    # =====================================================================
    # 主循环
    # =====================================================================

    def run(self, horizon: int | None = None, settle: bool = True) -> None:
        """推进演练。

        0..horizon 按工单与回执驱动；horizon 之后进入 settle 阶段，只推进作业
        与交付，直到全部恢复申请到达终态。冷层召回若回执尚未到达，按
        retrieve_min 在 settle 中模拟到达（资料窗口之外的合理兜底）。
        """
        events = self._timeline()
        event_horizon = horizon
        if event_horizon is None:
            event_horizon = max(
                [r.at for r in self.bundle.requests]
                + [r.at for r in self.bundle.receipts]
            )
        for minute in range(self.now, event_horizon + 1):
            self._run_minute(minute, events)
        self.now = event_horizon

        if settle:
            safety_cap = event_horizon + 200
            while self._has_live_requests() and self.now < safety_cap:
                minute = self.now + 1
                self._settle_external_fetches(minute)
                self._run_minute(minute, events)
                self.now = minute

    def _has_live_requests(self) -> bool:
        return any(s.status == RequestStatus.PENDING for s in self.requests.values())

    def _live_request_refs(self) -> set[str]:
        return {
            ref for ref, state in self.requests.items()
            if state.status == RequestStatus.PENDING
        }

    def _settle_external_fetches(self, minute: int) -> None:
        """资料窗口外的冷层召回：按 retrieve_min 模拟 fetch_ready 到达。"""
        for job in list(self.scheduler.running):
            if job.kind != JobKind.FETCH or not job.external:
                continue
            if job.finish_at <= minute:
                rec = self.store.record(job.object_id)
                if rec.state == BlockState.FETCHING:
                    synthetic = Receipt(
                        at=minute, kind="fetch_ready", reference=job.ref,
                        object=job.object_id, free_units=None,
                    )
                    self._handle_fetch_ready(synthetic, minute)

    def _run_minute(self, minute: int, events: list[tuple[int, str, Any]]) -> None:
        self.now = minute
        minute_events = [e for e in events if e[0] == minute]
        # 1) 作业完成（先于同分钟回执与工单）。
        self._complete_jobs(minute)
        for _, kind, payload in minute_events:
            if kind == "pressure":
                self._handle_pressure(payload, minute)
        for _, kind, payload in minute_events:
            if kind == "receipt" and payload.kind == "fetch_ready":
                self._handle_fetch_ready(payload, minute)
        for _, kind, payload in minute_events:
            if kind == "receipt" and payload.kind in ("delete_claimed", "deleted"):
                self._handle_delete_receipt(payload, minute)
        for _, kind, payload in minute_events:
            if kind == "request":
                self._handle_request(payload, minute)
        # 2) 派新作业、推进还原与交付。
        self._dispatch_request_work(minute)
        self._complete_jobs(minute)
        self._check_deliveries(minute)
        self._check_deadline_breach(minute)
        self._recheck_cleanups(minute)

    def _check_deadline_breach(self, minute: int) -> None:
        """越过最迟交付时间仍未交付：记录延期，作业不中断，供申请方重新决定。"""
        for state in self.requests.values():
            if state.status != RequestStatus.PENDING:
                continue
            if state.deadline_breached_at is not None:
                continue
            if minute <= state.request.deadline:
                continue
            state.deadline_breached_at = state.request.deadline
            state.history.append(
                f"minute {minute}: 已越过 deadline {state.request.deadline}，"
                f"当前 ETA {state.eta}，等待申请方决定是否继续",
            )
            self._decide(minute, "deadline_breached", state.request.reference, {
                "deadline": state.request.deadline,
                "current_point": state.current_point.point_id if state.current_point else None,
                "eta": state.eta,
                "overrun": minute - state.request.deadline,
            })

    def _timeline(self) -> list[tuple[int, str, Any]]:
        events: list[tuple[int, str, Any]] = []
        for receipt in self.bundle.receipts:
            key = (receipt.at, receipt.kind, receipt.reference, receipt.object)
            if key in self.receipts_seen:
                continue
            kind = "pressure" if receipt.kind == "storage_pressure" else "receipt"
            events.append((receipt.at, kind, receipt))
        for request in self.bundle.requests:
            events.append((request.at, "request", request))
        events.sort(key=lambda e: (e[0], 0 if e[1] == "pressure" else
                                   1 if e[1] == "receipt" else 2))
        return events

    # =====================================================================
    # 工单
    # =====================================================================

    def _handle_request(self, request: Request, minute: int) -> None:
        if request.kind == "restore":
            self._admit_restore(request, minute)
        elif request.kind == "cancel":
            self._handle_cancel(request, minute)
        else:
            self._admit_cleanup(request, minute)

    def _admit_restore(self, request: Request, minute: int) -> None:
        plan = self.planner.plan(
            request.reference, request.members,
            request.earliest_point, request.deadline,
        )
        state = RequestState(request=request, status=RequestStatus.PENDING, plan=plan)
        self.requests[request.reference] = state

        if not plan.feasible:
            state.status = RequestStatus.FAILED
            state.history.append(
                f"minute {minute}: 接纳即失败：" + "；".join(plan.failure_reasons),
            )
            self._decide(minute, "restore_rejected", request.reference, {
                "members": list(request.members),
                "earliest_point": request.earliest_point,
                "deadline": request.deadline,
                "reasons": plan.failure_reasons,
                "alternatives": self._alternative_summary(plan),
            })
            return

        point = plan.chosen
        state.current_point = point
        evaluation = plan.chosen_evaluation
        blocks = point.blocks
        self.store.pin_many(blocks, request.reference)
        extra = tuple(
            o for o in blocks
            if self.store.record(o).state == BlockState.ABSENT
            and self.store.record(o).is_remote_available
        )
        eta = self.scheduler.project_eta(minute, blocks, extra)
        state.eta = eta
        state.history.append(
            f"minute {minute}: 接纳恢复点 {point.point_id}（业务时间 {point.business_minute}）"
            f"，依赖 {len(blocks)} 块，ETA {eta}，deadline {request.deadline}",
        )
        detail: dict[str, Any] = {
            "members": list(request.members),
            "point": point.point_id,
            "business_minute": point.business_minute,
            "blocks": list(blocks),
            "eta": eta,
            "deadline": request.deadline,
            "meets_deadline": eta <= request.deadline,
            "time_loss": plan.time_loss,
        }
        if plan.time_loss:
            detail["preferred_minute"] = plan.preferred.business_minute
            detail["preferred_blockers"] = self._blocker_summary(evaluation)
            detail["alternatives"] = self._alternative_summary(plan)
            state.history.append(
                f"minute {minute}: 期望 {plan.preferred.business_minute} 不可用，"
                f"回退 {plan.time_loss} 分钟至 {point.business_minute}",
            )
        if eta > request.deadline:
            detail["delay"] = eta - request.deadline
            state.history.append(
                f"minute {minute}: 注意 ETA 超出 deadline {eta - request.deadline} 分钟",
            )
        self._decide(minute, "restore_admitted", request.reference, detail)
        self._recheck_cleanups(minute)

    def _handle_cancel(self, request: Request, minute: int) -> None:
        target = self.requests.get(request.reference)
        if target is None:
            self._decide(minute, "cancel_unknown", request.reference, {
                "note": f"取消工单引用了未知恢复 {request.reference}",
            })
            return
        if target.status in (RequestStatus.DELIVERED, RequestStatus.FAILED):
            self._decide(minute, "cancel_ignored", request.reference, {
                "status": target.status.value,
                "note": "申请已结束，取消不产生效果",
            })
            target.history.append(f"minute {minute}: 取消到达但状态为 {target.status.value}")
            return
        released = self.store.unpin_request(request.reference)
        target.status = RequestStatus.CANCELED
        self.scheduler.prune_queues(self._needed_objects())
        target.history.append(f"minute {minute}: 申请取消，释放 pin {sorted(released)}")
        self._decide(minute, "restore_canceled", request.reference, {
            "released_pins": sorted(released),
            "note": "已核验的共享字节保留，供其他申请复用；未读完的块让出保护与作业槽",
        })
        self._evict_dead_local(minute)
        self._recheck_cleanups(minute)

    def _admit_cleanup(self, request: Request, minute: int) -> None:
        object_id = request.object
        state = CleanupState(request=request, status=CleanupStatus.WAITING)
        self.cleanups[request.reference] = state
        block = self.bundle.blocks[object_id]
        if block.retire_min > minute:
            state.note = f"保留期 {block.retire_min} 未到，工单先登记"
        rec = self.store.record(object_id)
        if rec.state == BlockState.DELETED_REMOTE:
            state.status = CleanupStatus.DELETED
            state.deleted_at = rec.deleted_at
        elif rec.state == BlockState.CLAIMED_REMOTE:
            state.status = CleanupStatus.CLAIMED
            state.claimed_at = rec.claimed_at
        self._evaluate_cleanup(state, minute)
        self._decide(minute, "cleanup_admitted", request.reference, {
            "object": object_id,
            "status": state.status.value,
            "blocked_by": list(state.blocked_by),
            "releasable_at": state.releasable_at,
            "note": state.note,
        })

    # =====================================================================
    # 回执
    # =====================================================================

    def _handle_pressure(self, receipt: Receipt, minute: int) -> None:
        self.store.record_pressure(minute, receipt.free_units)
        self.storage_events.append((minute, receipt.free_units))
        evicted = self.store.evict_unpinned_to_relieve_pressure(minute)
        for object_id in evicted:
            self._decide(minute, "pressure_eviction", receipt.reference, {
                "object": object_id,
                "reason": "空间低于保留线，驱逐无 pin 的本地块（受保护块不动）",
            })
        self._decide(minute, "storage_pressure", receipt.reference, {
            "free_units": receipt.free_units,
            "evicted": evicted,
            "free_units_after": self.store.free_units,
        })

    def _handle_fetch_ready(self, receipt: Receipt, minute: int) -> None:
        object_id = receipt.object
        rec = self.store.record(object_id)
        ref_key = (receipt.reference, object_id)
        replay = ref_key in self.known_fetch_refs

        if replay and rec.state not in (BlockState.FETCHING, BlockState.ABSENT):
            # 中断重放的回执：字节早已处理，不再召回/校验/占空间。
            self._decide(minute, "fetch_ready_replay_ignored", receipt.reference, {
                "object": object_id, "state": rec.state.value,
                "note": "重复召回回执，本地状态已处理过该引用",
            })
            return

        if rec.state in (BlockState.LOCAL_VERIFIED, BlockState.LOCAL_BAD):
            self.known_fetch_refs.add(ref_key)
            self._decide(minute, "fetch_ready_duplicate", receipt.reference, {
                "object": object_id, "state": rec.state.value,
                "note": "字节已在本地（已核验/已判坏），回执幂等处理",
            })
            return

        if rec.state == BlockState.FETCHING:
            self.scheduler.adopt_external_fetch(object_id, minute, receipt.reference)
            self.store.mark_bytes_ready(object_id, minute, receipt.reference)
            self.known_fetch_refs.add(ref_key)
        elif rec.state == BlockState.ABSENT:
            if replay:
                # 引用见过但块已不在本地：说明字节被驱逐过，而该召回已结束，不能据此重拉。
                self._decide(minute, "fetch_ready_stale_ignored", receipt.reference, {
                    "object": object_id,
                    "note": "回执引用的召回已结束且本地无字节，按陈旧回执忽略",
                })
                return
            # 中断前召回已在外部完成、本地无记录：补登记字节并占用一个空间单位。
            if not self.store.can_admit_space(1):
                self._decide(minute, "fetch_ready_deferred", receipt.reference, {
                    "object": object_id, "reason": "空间不足，回执挂起等待空间",
                })
                return
            self.store.issue_fetch(object_id, minute, receipt.reference)
            self.store.mark_bytes_ready(object_id, minute, receipt.reference)
            self.scheduler.complete(minute)
            self.known_fetch_refs.add(ref_key)

        # 字节一到立即校验：读取成功不代表字节完整。
        if rec.state == BlockState.LOCAL_UNVERIFIED:
            ok = self.store.verify(object_id, minute)
            self._decide(minute, "verify_ok" if ok else "verify_failed", receipt.reference, {
                "object": object_id,
                "digest_expected": self.bundle.blocks[object_id].digest,
                "digest_actual": self.bundle.actual_digests[object_id],
            })
            if not ok:
                self._on_corruption(object_id, minute)
            else:
                self._dispatch_request_work(minute)

    def _handle_delete_receipt(self, receipt: Receipt, minute: int) -> None:
        object_id = receipt.object
        rec = self.store.record(object_id)
        if receipt.kind == "delete_claimed":
            if rec.state not in (BlockState.CLAIMED_REMOTE, BlockState.DELETED_REMOTE):
                self.store.claim_delete(object_id, minute)
            self._decide(minute, "delete_claimed", receipt.reference, {
                "object": object_id,
                "pins": rec.pinned_by(),
                "fully_read": rec.fully_read,
                "note": "仓储承诺关闭：未读完的依赖不能再服务新恢复",
            })
        else:
            if rec.state != BlockState.DELETED_REMOTE:
                self.store.confirm_delete(object_id, minute)
            self._decide(minute, "delete_confirmed", receipt.reference, {
                "object": object_id,
                "local_verified": rec.usable_verified,
                "pins": rec.pinned_by(),
                "note": "删除回执已到；已核验本地副本在 pin 释放前保留",
            })
        # 仓储变化可能让进行中的恢复失去仅存的来源。
        self._replan_affected(object_id, minute)
        self._recheck_cleanups(minute)

    # =====================================================================
    # 作业派发与完成
    # =====================================================================

    def _dispatch_request_work(self, minute: int) -> None:
        needed = self._needed_objects()
        for object_id in sorted(needed):
            rec = self.store.record(object_id)
            if rec.state == BlockState.ABSENT and rec.is_remote_available:
                if self.store.can_admit_space(1):
                    self.scheduler.request_fetch("shared", object_id)
        # 还原顺序：按各服务链（父先于子），verified 才入队，全局限一次。
        ordered: list[str] = []
        seen: set[str] = set()
        for state in self.requests.values():
            if state.status != RequestStatus.PENDING or state.current_point is None:
                continue
            for sp in state.current_point.services:
                for object_id in self.topo.closure(sp.leaf):
                    if object_id not in seen:
                        seen.add(object_id)
                        ordered.append(object_id)
        for object_id in ordered:
            self.scheduler.request_restore("shared", object_id)
        before = set(self.known_fetch_refs)
        self.scheduler.tick(minute)
        for job in self.scheduler.running:
            if job.kind == JobKind.FETCH:
                self.known_fetch_refs.add((job.ref, job.object_id))
        issued = set(self.known_fetch_refs) - before
        for ref, object_id in sorted(issued):
            self._decide(minute, "fetch_issued", ref, {
                "object": object_id,
                "tier": self.bundle.blocks[object_id].tier,
                "retrieve_min": self.bundle.blocks[object_id].retrieve_min,
                "external": self.bundle.blocks[object_id].tier == "cold",
            })

    def _needed_objects(self) -> set[str]:
        needed: set[str] = set()
        for state in self.requests.values():
            if state.status == RequestStatus.PENDING and state.current_point is not None:
                needed.update(state.current_point.blocks)
        return needed

    def _complete_jobs(self, minute: int) -> None:
        completed = self.scheduler.complete(minute)
        for job in completed:
            if job.kind == JobKind.FETCH:
                rec = self.store.record(job.object_id)
                if rec.state == BlockState.FETCHING:
                    self.store.mark_bytes_ready(job.object_id, minute, job.ref)
                if rec.state == BlockState.LOCAL_UNVERIFIED:
                    ok = self.store.verify(job.object_id, minute)
                    self._decide(minute, "verify_ok" if ok else "verify_failed", job.ref, {
                        "object": job.object_id,
                        "digest_expected": self.bundle.blocks[job.object_id].digest,
                        "digest_actual": self.bundle.actual_digests[job.object_id],
                    })
                    if not ok:
                        self._on_corruption(job.object_id, minute)
            else:
                self._decide(minute, "restore_done", job.ref, {
                    "object": job.object_id, "request": job.request_ref,
                })

    # =====================================================================
    # 损坏与重判
    # =====================================================================

    def _on_corruption(self, object_id: str, minute: int) -> None:
        affected = [
            ref for ref, state in self.requests.items()
            if state.status == RequestStatus.PENDING
            and state.current_point is not None
            and object_id in state.current_point.blocks
        ]
        self._decide(minute, "corruption", object_id, {
            "object": object_id,
            "affected_requests": sorted(affected),
            "rule": "组内不允许只替换损坏服务，整组重判恢复点",
        })
        for ref in affected:
            self._replan(ref, minute, cause=f"块 {object_id} 实际校验失败")
        # 损坏字节在无人需要后立即驱逐。
        self.store.evict_bad(object_id, minute)
        self._recheck_cleanups(minute)

    def _replan_affected(self, object_id: str, minute: int) -> None:
        for ref, state in list(self.requests.items()):
            if state.status != RequestStatus.PENDING or state.current_point is None:
                continue
            if object_id not in state.current_point.blocks:
                continue
            evaluation = self.planner.evaluate(state.current_point)
            if not evaluation.feasible:
                self._replan(ref, minute, cause=f"块 {object_id} 仓储状态变化")

    def _replan(self, ref: str, minute: int, cause: str) -> None:
        state = self.requests[ref]
        request = state.request
        old_point = state.current_point
        self.store.unpin_request(ref)
        plan = self.planner.plan(
            ref, request.members, request.earliest_point, request.deadline,
        )
        state.plan = plan
        if not plan.feasible:
            state.status = RequestStatus.FAILED
            state.current_point = None
            state.history.append(f"minute {minute}: 重判失败（{cause}）："
                                 + "；".join(plan.failure_reasons))
            self._decide(minute, "restore_failed", ref, {
                "cause": cause,
                "old_point": old_point.point_id if old_point else None,
                "reasons": plan.failure_reasons,
                "alternatives": self._alternative_summary(plan),
            })
            self.scheduler.prune_queues(self._needed_objects())
            self._evict_dead_local(minute)
            self._recheck_cleanups(minute)
            return
        point = plan.chosen
        state.current_point = point
        self.store.pin_many(point.blocks, ref)
        extra = tuple(
            o for o in point.blocks
            if self.store.record(o).state == BlockState.ABSENT
            and self.store.record(o).is_remote_available
        )
        eta = self.scheduler.project_eta(minute, point.blocks, extra)
        state.eta = eta
        rolled = [] if old_point is None else sorted(
            set(old_point.blocks) - set(point.blocks),
        )
        state.history.append(
            f"minute {minute}: 因 {cause}，恢复点 {old_point.point_id if old_point else '-'} "
            f"-> {point.point_id}（业务时间 {point.business_minute}），ETA {eta}",
        )
        self._decide(minute, "restore_replanned", ref, {
            "cause": cause,
            "old_point": old_point.point_id if old_point else None,
            "new_point": point.point_id,
            "business_minute": point.business_minute,
            "rolled_back_blocks": rolled,
            "time_loss": (old_point.business_minute - point.business_minute) if old_point else 0,
            "eta": eta,
            "meets_deadline": eta <= request.deadline,
            "delay": max(0, eta - request.deadline),
            "blockers_on_preferred": self._blocker_summary(plan.chosen_evaluation),
        })
        self.scheduler.prune_queues(self._needed_objects())
        self._evict_dead_local(minute)
        self._recheck_cleanups(minute)

    # =====================================================================
    # 交付
    # =====================================================================

    def _check_deliveries(self, minute: int) -> None:
        restored = self.scheduler.restored_objects()
        for state in self.requests.values():
            if state.status != RequestStatus.PENDING or state.current_point is None:
                continue
            point = state.current_point
            if not all(o in restored for o in point.blocks):
                continue
            # 交付前再做一次可行性确认（删除回执可能在还原期间到达）。
            evaluation = self.planner.evaluate(point)
            if not evaluation.feasible:
                self._replan(state.request.reference, minute, cause="交付前复核发现依赖失效")
                continue
            content = {
                sp.service: self.topo.reconstruct(sp.leaf) for sp in point.services
            }
            state.status = RequestStatus.DELIVERED
            state.delivered_at = minute
            state.delivered_content = content
            on_time = minute <= state.request.deadline
            state.history.append(
                f"minute {minute}: 交付 {point.point_id}，"
                f"{'按时' if on_time else f'超时 {minute - state.request.deadline} 分钟'}",
            )
            self._decide(minute, "restore_delivered", state.request.reference, {
                "point": point.point_id,
                "business_minute": point.business_minute,
                "delivered_at": minute,
                "deadline": state.request.deadline,
                "on_time": on_time,
                "services": {sp.service: sp.leaf for sp in point.services},
                "content": content,
                "shared_blocks": [
                    o for o in point.blocks
                    if len(self.store.record(o).pinned_by()) > 1 or self.scheduler.has_restore_history(o)
                ],
            })
            # 交付即承诺履行完毕：释放该申请的 pin，共享字节留给其他申请或按空间驱逐。
            released = self.store.unpin_request(state.request.reference)
            self.scheduler.prune_queues(self._needed_objects())
            state.history.append(
                f"minute {minute}: 释放已履行承诺的 pin {sorted(released)}",
            )
        self._evict_dead_local(minute)
        self._recheck_cleanups(minute)

    # =====================================================================
    # 清理协调
    # =====================================================================

    def _recheck_cleanups(self, minute: int) -> None:
        for state in self.cleanups.values():
            if state.status in (CleanupStatus.DELETED, CleanupStatus.RELEASABLE):
                # 仍可能随 pin 释放转入 releasable。
                pass
            self._evaluate_cleanup(state, minute)

    def _evaluate_cleanup(self, state: CleanupState, minute: int) -> None:
        object_id = state.request.object
        rec = self.store.record(object_id)
        pinners = rec.pinned_by()
        unread = tuple(sorted(
            ref for ref in pinners
            if not self._request_dependency_complete(ref, object_id)
        ))
        # blocked_by 表示“尚未读完的实际依赖”：块本身未核验时保护最硬。
        if not rec.fully_read and pinners:
            state.blocked_by = pinners
        elif unread:
            state.blocked_by = unread
        else:
            if state.blocked_by and state.protected_until is None:
                # 保护窗口在最后一个未读完依赖核验完成时结束。
                state.protected_until = minute
            state.blocked_by = ()
            if state.protected_until is None:
                # 接纳时就没有未读完依赖：保护窗口从未开启。
                state.protected_until = minute

        if rec.state == BlockState.DELETED_REMOTE:
            state.status = CleanupStatus.DELETED
            state.deleted_at = rec.deleted_at
        elif rec.state == BlockState.CLAIMED_REMOTE:
            state.status = CleanupStatus.CLAIMED
            state.claimed_at = rec.claimed_at

        # 本地何时可驱逐：最后一个 pin 释放的分钟（已交付/取消都会 unpin）。
        if not pinners:
            state.releasable_at = minute
            if rec.state == BlockState.DELETED_REMOTE:
                state.status = CleanupStatus.RELEASABLE
        else:
            finishes = [
                self.requests[ref].delivered_at or self.requests[ref].eta or minute
                for ref in pinners if ref in self.requests
            ]
            state.releasable_at = max(finishes) if finishes else minute

        if state.blocked_by:
            state.note = (
                f"对象仍被 {list(state.blocked_by)} 作为未读完依赖保护，"
                "清理端不得领取；已领取则不得再承诺给新恢复"
            )
        elif rec.fully_read:
            state.note = "本地已有核验副本，既有恢复可完成；新恢复不得再承诺该仓储对象"

    def _request_dependency_complete(self, request_ref: str, object_id: str) -> bool:
        state = self.requests.get(request_ref)
        if state is None or state.current_point is None:
            return False
        if object_id not in state.current_point.blocks:
            return False
        return self.store.record(object_id).fully_read

    def _evict_dead_local(self, minute: int) -> None:
        """无 pin 的本地字节（含已删除仓储的 verified 副本、坏块）释放空间。"""
        for rec in self.store.records.values():
            if rec.pins:
                continue
            if rec.state == BlockState.LOCAL_BAD:
                self.store.evict_bad(rec.object_id, minute)
            elif rec.state == BlockState.DELETED_REMOTE and rec.evicted_at is None:
                if self.store.evict_if_unpinned(rec.object_id, minute):
                    self._decide(minute, "local_evicted", rec.object_id, {
                        "object": rec.object_id, "reason": "仓储已删且无 pin",
                    })

    # =====================================================================
    # 重启对账
    # =====================================================================

    def reconcile(self, journal: dict[str, Any], minute: int) -> list[str]:
        """依据上次运行的 journal 与 activity 回执，重建已发生的效果。"""
        notes: list[str] = []
        self.now = minute
        for object_id, snap in journal.get("blocks", {}).items():
            rec = self.store.record(object_id)
            state = BlockState(snap["state"])
            rec.state = state
            rec.pins = set(snap.get("pins", ()))
            rec.fetch_ref = snap.get("fetch_ref")
            rec.fetch_issued_at = snap.get("fetch_issued_at")
            rec.fetch_ready_at = snap.get("fetch_ready_at")
            rec.verified_at = snap.get("verified_at")
            rec.bad_reason = snap.get("bad_reason")
            rec.claimed_at = snap.get("claimed_at")
            rec.deleted_at = snap.get("deleted_at")
            rec.evicted_at = snap.get("evicted_at")
        self.store.free_units = journal.get("free_units", self.store.free_units)
        self.decisions = [Decision(**d) for d in journal.get("decisions", ())]
        self.receipts_seen = {tuple(k) for k in journal.get("receipts_seen", ())}
        self.known_fetch_refs = {tuple(k) for k in journal.get("known_fetch_refs", ())}
        self._restore_request_states(journal, minute)
        # 重新消费 activity：以回执为准核对块状态，重复回执只记备注。
        for receipt in self.bundle.receipts:
            if receipt.at > minute:
                break
            key = (receipt.at, receipt.kind, receipt.reference, receipt.object)
            already = key in self.receipts_seen
            if receipt.kind == "storage_pressure":
                self.store.record_pressure(receipt.at, receipt.free_units)
            elif receipt.kind == "fetch_ready":
                rec = self.store.record(receipt.object)
                if rec.state == BlockState.FETCHING or rec.state == BlockState.ABSENT:
                    notes.append(f"对账：{receipt.object} 的召回回执在中断后补登记")
                    if rec.state == BlockState.ABSENT:
                        self.store.issue_fetch(receipt.object, receipt.at, receipt.reference)
                    self.store.mark_bytes_ready(receipt.object, receipt.at, receipt.reference)
                    ok = self.store.verify(receipt.object, receipt.at)
                    notes.append(
                        f"对账：{receipt.object} 校验{'通过' if ok else '失败'}",
                    )
                elif already:
                    notes.append(f"对账：{receipt.object} 重复 fetch_ready，状态 {rec.state.value}")
            elif receipt.kind in ("delete_claimed", "deleted"):
                rec = self.store.record(receipt.object)
                if receipt.kind == "delete_claimed" and rec.state not in (
                    BlockState.CLAIMED_REMOTE, BlockState.DELETED_REMOTE,
                ):
                    self.store.claim_delete(receipt.object, receipt.at)
                    notes.append(f"对账：补登记 {receipt.object} 删除领取")
                if receipt.kind == "deleted" and rec.state != BlockState.DELETED_REMOTE:
                    self.store.confirm_delete(receipt.object, receipt.at)
                    notes.append(f"对账：补登记 {receipt.object} 已删除回执")
            self.receipts_seen.add(key)
        # 重建作业表：以 journal 的 scheduler 快照为准，避免与块状态重复登记。
        self._restore_scheduler(journal, minute)
        return notes

    def _restore_scheduler(self, journal: dict[str, Any], minute: int) -> None:
        snap = journal.get("scheduler", {})
        self.scheduler.running = []
        for item in snap.get("running", ()):
            self.scheduler.reconcile_job(Job(
                ref=item["ref"],
                kind=JobKind(item["kind"]),
                object_id=item["object"],
                request_ref=item["request"],
                started_at=item["started_at"],
                finish_at=item["finish_at"],
                duration=item["finish_at"] - item["started_at"],
                external=item.get("external", False),
            ))
        # 已完成作业历史也要恢复，否则块会被重复还原。
        from .scheduler import SlotWindow
        self.scheduler.history = [
            SlotWindow(
                kind=JobKind(item["kind"]), ref=item["ref"], object_id=item["object"],
                request_ref=item["request"], start=item["start"], finish=item["finish"],
            )
            for item in snap.get("history", ())
        ]
        # 队列中的对象若仍被 live 请求需要，保留为待派发（顺序信息不重要）。
        needed = set()
        for state in self.requests.values():
            if state.status == RequestStatus.PENDING and state.current_point is not None:
                needed.update(state.current_point.blocks)
        self.scheduler.fetch_queue = [
            (i, "shared", object_id) for i, object_id in enumerate(snap.get("fetch_queue", ()))
            if object_id in needed
        ]
        self.scheduler.restore_queue = [
            (i, "shared", object_id) for i, object_id in enumerate(snap.get("restore_queue", ()))
            if object_id in needed
        ]
        self.scheduler._ticked_minutes = set(range(
            journal.get("minute", minute) + 1,
        ))
        self.scheduler.now = minute

    def _restore_request_states(self, journal: dict[str, Any], minute: int) -> None:
        """把 journal 中的请求快照还原为可继续推进的 RequestState。"""
        request_by_ref = {
            r.reference: r for r in self.bundle.requests if r.kind == "restore"
        }
        for ref, snap in journal.get("requests", {}).items():
            request = request_by_ref.get(ref)
            if request is None or request.kind != "restore":
                continue
            status = RequestStatus(snap["status"])
            state = RequestState(request=request, status=status, eta=snap.get("eta"),
                                 delivered_at=snap.get("delivered_at"),
                                 deadline_breached_at=snap.get("deadline_breached_at"),
                                 history=list(snap.get("history", ())))
            point_id = snap.get("point")
            if point_id:
                for point in self.planner.candidate_points(request.members):
                    if point.point_id == point_id:
                        state.current_point = point
                        if status == RequestStatus.PENDING:
                            state.plan = self.planner.plan(
                                ref, request.members,
                                request.earliest_point, request.deadline,
                            )
                        elif status == RequestStatus.DELIVERED:
                            state.delivered_content = {
                                sp.service: self.topo.reconstruct(sp.leaf)
                                for sp in point.services
                            }
                        break
            self.requests[ref] = state

    def journal(self) -> dict[str, Any]:
        return {
            "minute": self.now,
            "free_units": self.store.free_units,
            "blocks": self.store.snapshot(),
            "requests": {
                ref: {
                    "status": state.status.value,
                    "point": state.current_point.point_id if state.current_point else None,
                    "delivered_at": state.delivered_at,
                    "deadline_breached_at": state.deadline_breached_at,
                    "eta": state.eta,
                    "history": state.history,
                }
                for ref, state in self.requests.items()
            },
            "cleanups": {
                ref: {
                    "status": state.status.value,
                    "claimed_at": state.claimed_at,
                    "deleted_at": state.deleted_at,
                    "blocked_by": list(state.blocked_by),
                    "protected_until": state.protected_until,
                    "releasable_at": state.releasable_at,
                }
                for ref, state in self.cleanups.items()
            },
            "scheduler": self.scheduler.snapshot(),
            "decisions": [
                {"at": d.at, "kind": d.kind, "reference": d.reference, "detail": d.detail}
                for d in self.decisions
            ],
            "receipts_seen": [list(k) for k in self.receipts_seen],
            "known_fetch_refs": [list(k) for k in self.known_fetch_refs],
        }

    # =====================================================================
    # 小工具
    # =====================================================================

    def _decide(self, minute: int, kind: str, reference: str, detail: dict[str, Any]) -> None:
        self.decisions.append(Decision(minute, kind, reference, detail))

    @staticmethod
    def _blocker_summary(evaluation: PointEvaluation | None) -> list[dict[str, str]]:
        if evaluation is None:
            return []
        return [
            {"object": o, "reason": evaluation.verdicts[o].reason,
             "state": evaluation.verdicts[o].state}
            for o in evaluation.blockers
        ]

    def _alternative_summary(self, plan: Plan) -> list[dict[str, Any]]:
        out = []
        for alt in plan.alternatives:
            out.append({
                "point": alt.point.point_id,
                "business_minute": alt.point.business_minute,
                "time_loss": alt.time_loss,
                "blockers": list(alt.blockers_on_preferred),
                "note": alt.note,
            })
        return out
