"""冷层召回与本地还原的作业能力调度。

整个恢复站共用现场能力：fetch_slots 个召回槽、restore_slots 个还原槽，
以及统一的保留空间约束。分钟是最小时间单位：第 m 分钟开始的作业，持续 d
分钟，在第 m+d 分钟开始时完成。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .feeds import FeedBundle
from .store import BlockState, BlockStore


class JobKind(str, Enum):
    FETCH = "fetch"
    RESTORE = "restore"


@dataclass
class Job:
    ref: str
    kind: JobKind
    object_id: str
    request_ref: str
    started_at: int
    finish_at: int
    duration: int
    external: bool = False
    """external=True 表示完成由 activity 回执驱动，而不是作业槽倒计时。"""


@dataclass
class SlotWindow:
    kind: JobKind
    ref: str
    object_id: str
    request_ref: str
    start: int
    finish: int


class Scheduler:
    def __init__(self, bundle: FeedBundle, store: BlockStore, now: int = 0):
        self.bundle = bundle
        self.store = store
        self.now = now
        self.fetch_slots = max(u.fetch_slots for u in bundle.units)
        self.restore_slots = max(u.restore_slots for u in bundle.units)
        self.running: list[Job] = []
        self.fetch_queue: list[tuple[int, str, str]] = []
        """(请求顺序, request_ref, object_id)"""
        self.restore_queue: list[tuple[int, str, str]] = []
        self.history: list[SlotWindow] = []
        self._ticked_minutes: set[int] = set()
        self._queue_seq = 0
        self._fetch_seq = 0
        self._restore_seq = 0

    # ---- 排队 -----------------------------------------------------------

    def request_fetch(self, request_ref: str, object_id: str) -> None:
        rec = self.store.record(object_id)
        if rec.state != BlockState.ABSENT or not rec.is_remote_available:
            return
        if any(j.kind == JobKind.FETCH and j.object_id == object_id for j in self.running):
            return
        if any(item[2] == object_id for item in self.fetch_queue):
            return
        self.fetch_queue.append((self._queue_seq, request_ref, object_id))
        self._queue_seq += 1

    def request_restore(self, request_ref: str, object_id: str) -> None:
        rec = self.store.record(object_id)
        if not rec.usable_verified:
            return
        if any(j.object_id == object_id and j.kind == JobKind.RESTORE for j in self.running):
            return
        if any(item[2] == object_id for item in self.restore_queue):
            return
        # 同一已核验块只需全恢复站还原一次，结果可服务多个申请。
        if any(w.kind == JobKind.RESTORE and w.object_id == object_id for w in self.history):
            return
        self.restore_queue.append((self._queue_seq, request_ref, object_id))
        self._queue_seq += 1

    def prune_queues(self, needed_objects: set[str]) -> None:
        """取消/重判后，排队中已无人需要的块让出槽位；在跑作业不中断。"""
        self.fetch_queue = [item for item in self.fetch_queue if item[2] in needed_objects]
        self.restore_queue = [item for item in self.restore_queue if item[2] in needed_objects]

    # ---- 每分钟驱动 -----------------------------------------------------

    def complete(self, minute: int) -> list[Job]:
        """结算到 minute 为止完成的内部作业并登记历史。

        冷层召回是外部作业，只能由 fetch_ready 回执完成，倒计时不会结束它。
        """
        self.now = minute
        completed = [
            job for job in self.running
            if job.finish_at <= minute and not job.external
        ]
        for job in completed:
            self.running.remove(job)
            self.history.append(SlotWindow(
                job.kind, job.ref, job.object_id, job.request_ref,
                job.started_at, job.finish_at,
            ))
        return completed

    def tick(self, minute: int) -> list[Job]:
        """推进到 minute 分钟开始时：完成结算 + 新派发。

        召回派发按分钟幂等（重复调用不得重复消耗空间）；还原派发天然可
        重入——只有 verified 的块才入队，且每块全局限一次。
        """
        completed = self.complete(minute)
        if minute not in self._ticked_minutes:
            self._ticked_minutes.add(minute)
            self._dispatch_fetches(minute)
        self._dispatch_restores(minute)
        return completed

    def _dispatch_fetches(self, minute: int) -> None:
        # 召回：槽位 + 空间双约束。冷层由外部回执完成（external），热层内部倒计时。
        while self.fetch_queue and self._free_fetch_slots(minute) > 0:
            order, request_ref, object_id = self.fetch_queue[0]
            rec = self.store.record(object_id)
            if rec.state != BlockState.ABSENT:
                self.fetch_queue.pop(0)
                continue
            if not self.store.can_admit_space(1):
                break  # 空间压力下排队等待，不丢弃申请
            self.fetch_queue.pop(0)
            block = self.bundle.blocks[object_id]
            external = block.tier == "cold"
            self._fetch_seq += 1
            ref = f"fetch-{self._fetch_seq}"
            job = Job(ref, JobKind.FETCH, object_id, request_ref, minute,
                      minute + block.retrieve_min, block.retrieve_min, external=external)
            self.store.issue_fetch(object_id, minute, ref)
            self.running.append(job)

    def _dispatch_restores(self, minute: int) -> None:
        # 还原：仅消费已核验字节，按父块优先（链顺序天然满足）。
        while self.restore_queue and self._free_restore_slots(minute) > 0:
            order, request_ref, object_id = self.restore_queue[0]
            rec = self.store.record(object_id)
            if not rec.usable_verified:
                self.restore_queue.pop(0)
                continue
            self.restore_queue.pop(0)
            duration = self.bundle.blocks[object_id].restore_min
            self._restore_seq += 1
            ref = f"restore-{self._restore_seq:02d}"
            self.running.append(Job(
                ref, JobKind.RESTORE, object_id, request_ref, minute,
                minute + duration, duration,
            ))

    def _free_fetch_slots(self, minute: int) -> int:
        # 冷层召回在回执到达前同样占用共享召回能力。
        used = sum(1 for j in self.running if j.kind == JobKind.FETCH)
        return self.fetch_slots - used

    def _free_restore_slots(self, minute: int) -> int:
        used = sum(1 for j in self.running if j.kind == JobKind.RESTORE)
        return self.restore_slots - used

    # ---- 外部回执 -------------------------------------------------------

    def adopt_external_fetch(self, object_id: str, minute: int, ref: str) -> Job | None:
        """外部回执完成召回：立即结束在跑作业、释放槽位并登记历史。

        冷层召回的完成只能由 fetch_ready 回执驱动；倒计时永远不会结束
        external 作业，因此这里直接结算，避免僵尸作业占住共享召回能力。
        """
        for job in self.running:
            if job.kind == JobKind.FETCH and job.object_id == object_id:
                job.external = True
                job.finish_at = minute
                self.running.remove(job)
                self.history.append(SlotWindow(
                    job.kind, job.ref, job.object_id, job.request_ref,
                    job.started_at, minute,
                ))
                return job
        return None

    def has_restore_history(self, object_id: str) -> bool:
        return any(w.object_id == object_id for w in self.history)

    def restored_objects(self) -> set[str]:
        return {w.object_id for w in self.history if w.kind == JobKind.RESTORE}

    # ---- ETA 推演（不改变实际状态）--------------------------------------

    def project_eta(
        self, minute: int, needed: tuple[str, ...],
        extra_fetch: tuple[str, ...] = (),
    ) -> int:
        """给定当前队列，估算把 needed 全部还原完成的最早分钟。

        extra_fetch 是本次新承诺还要发起的召回（未入队）。推演使用块上的
        retrieve_min/restore_min 与共享槽位，已核验/已还原的块零成本。
        """
        fetch_ends = self._project_fetch_timeline(minute, extra_fetch)
        return self._project_restore_timeline(minute, needed, fetch_ends)

    def _project_fetch_timeline(self, minute: int, extra: tuple[str, ...]) -> dict[str, int]:
        ends: dict[str, int] = {}
        slots = sorted(
            j.finish_at for j in self.running if j.kind == JobKind.FETCH and not j.external
        )[: self.fetch_slots]
        queued = [object_id for _, _, object_id in self.fetch_queue]
        pending = list(queued)
        for object_id in extra:
            rec = self.store.record(object_id)
            if rec.state == BlockState.ABSENT and object_id not in pending:
                pending.append(object_id)
        # 已在跑的按完成时间登记。
        for job in self.running:
            if job.kind == JobKind.FETCH:
                ends[job.object_id] = job.finish_at
        next_slot = list(slots)
        cursor = minute
        for object_id in pending:
            rec = self.store.record(object_id)
            if rec.state != BlockState.ABSENT or object_id in ends:
                continue
            if next_slot:
                start = max(minute, next_slot.pop(0))
            else:
                start = cursor
            finish = start + self.bundle.blocks[object_id].retrieve_min
            ends[object_id] = finish
            cursor = max(cursor, finish)
            next_slot.append(finish)
            next_slot.sort()
        return ends

    def _project_restore_timeline(
        self, minute: int, needed: tuple[str, ...], fetch_ends: dict[str, int],
    ) -> int:
        done = self.restored_objects()
        targets = [o for o in needed if o not in done]
        if not targets:
            return minute
        slot_ends = [j.finish_at for j in self.running if j.kind == JobKind.RESTORE]
        slot_ends.sort()
        available = list(slot_ends)
        cursor = minute
        last_finish = minute
        for object_id in targets:
            rec = self.store.record(object_id)
            ready_at = fetch_ends.get(object_id, minute)
            if rec.usable_verified:
                ready_at = min(ready_at, minute)
            if available:
                start = max(minute, ready_at, available.pop(0))
            else:
                start = max(cursor, ready_at)
            finish = start + self.bundle.blocks[object_id].restore_min
            available.append(finish)
            available.sort()
            cursor = max(cursor, finish)
            last_finish = max(last_finish, finish)
        return last_finish

    # ---- 重启对账 -------------------------------------------------------

    def reconcile_job(self, job: Job) -> None:
        self.running.append(job)

    def snapshot(self) -> dict[str, object]:
        return {
            "now": self.now,
            "running": [
                {
                    "ref": j.ref, "kind": j.kind.value, "object": j.object_id,
                    "request": j.request_ref, "started_at": j.started_at,
                    "finish_at": j.finish_at, "external": j.external,
                }
                for j in self.running
            ],
            "fetch_queue": [q[2] for q in self.fetch_queue],
            "restore_queue": [q[2] for q in self.restore_queue],
            "history": [
                {
                    "kind": w.kind.value, "ref": w.ref, "object": w.object_id,
                    "request": w.request_ref, "start": w.start, "finish": w.finish,
                }
                for w in self.history
            ],
        }
