"""块的本地生命周期状态机与占用引用。

一个块在恢复站中的状态：

- ``absent``：冷层/热层对象尚未召回，本地无字节
- ``fetching``：已发出召回，作业槽占用中，字节未读全
- ``local_unverified``：字节已到本地，尚未做摘要校验
- ``local_verified``：摘要与 manifest 一致，可独立服务多个恢复申请
- ``local_bad``：实际摘要不符（或字节不可解析），不可用于任何恢复
- ``claimed_remote``：清理端已领取删除，仓储对象不可再承诺给新恢复
- ``deleted_remote``：仓储删除回执已到；若本地已有 verified 副本，既有承诺仍可完成

三类资源要分得清：已核验可独立使用的本地副本、未读完（fetching/unverified）
的对象、仍被其他请求 pin 占用的块。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .feeds import FeedBundle


class BlockState(str, Enum):
    ABSENT = "absent"
    FETCHING = "fetching"
    LOCAL_UNVERIFIED = "local_unverified"
    LOCAL_VERIFIED = "local_verified"
    LOCAL_BAD = "local_bad"
    CLAIMED_REMOTE = "claimed_remote"
    DELETED_REMOTE = "deleted_remote"


@dataclass
class BlockRecord:
    object_id: str
    state: BlockState = BlockState.ABSENT
    pins: set[str] = field(default_factory=set)
    """持有该块的恢复申请编号；verified 本地副本的实际内容可被多申请共享。"""
    fetch_ref: str | None = None
    """当前召回作业的外部关联标识（receipt 用它回执）。"""
    fetch_issued_at: int | None = None
    fetch_ready_at: int | None = None
    verified_at: int | None = None
    bad_reason: str | None = None
    claimed_at: int | None = None
    deleted_at: int | None = None
    evicted_at: int | None = None

    @property
    def is_remote_available(self) -> bool:
        """仓储对象是否还能对新承诺开放。"""
        return self.state in (
            BlockState.ABSENT, BlockState.FETCHING,
            BlockState.LOCAL_UNVERIFIED, BlockState.LOCAL_VERIFIED, BlockState.LOCAL_BAD,
        )

    @property
    def is_local(self) -> bool:
        return self.state in (
            BlockState.LOCAL_UNVERIFIED, BlockState.LOCAL_VERIFIED, BlockState.LOCAL_BAD,
        ) or (self.state == BlockState.DELETED_REMOTE and self.verified_at is not None)

    @property
    def usable_verified(self) -> bool:
        """是否存在已核验、可独立服务的本地副本。"""
        return self.verified_at is not None and self.state != BlockState.LOCAL_BAD

    @property
    def occupies_space(self) -> bool:
        """是否占用恢复站的一个空间单位（本地字节存在且未驱逐）。"""
        return self.evicted_at is None and (
            self.state in (BlockState.FETCHING, BlockState.LOCAL_UNVERIFIED,
                           BlockState.LOCAL_VERIFIED, BlockState.LOCAL_BAD,
                           BlockState.DELETED_REMOTE)
        )

    @property
    def fully_read(self) -> bool:
        """承诺保护口径：块是否已经读完并核验（verified 才是读完且可用）。"""
        return self.usable_verified

    def pinned_by(self) -> tuple[str, ...]:
        return tuple(sorted(self.pins))

    def has_pin(self) -> bool:
        return bool(self.pins)


class BlockStore:
    def __init__(self, bundle: FeedBundle, initial_free: int):
        self.bundle = bundle
        self.records = {object_id: BlockRecord(object_id) for object_id in bundle.blocks}
        self.free_units = initial_free
        self.free_history: list[tuple[int, int]] = []

    def record(self, object_id: str) -> BlockRecord:
        return self.records[object_id]

    # ---- 空间 -----------------------------------------------------------

    def record_pressure(self, minute: int, free_units: int) -> None:
        self.free_units = free_units
        self.free_history.append((minute, free_units))

    def _consume_space(self) -> None:
        self.free_units -= 1

    def _release_space(self) -> None:
        self.free_units += 1

    def can_admit_space(self, extra_objects: int) -> bool:
        return self.free_units - extra_objects >= self._min_free()

    def _min_free(self) -> int:
        # 整个恢复站共用同一限额（services 中各单元的 minimum_free_space 一致）。
        values = {u.minimum_free_space for u in self.bundle.units}
        if len(values) != 1:
            raise ValueError("各恢复站单元的 minimum_free_space 必须一致才能共用现场")
        return values.pop()

    # ---- 召回与校验 -----------------------------------------------------

    def issue_fetch(self, object_id: str, minute: int, ref: str) -> None:
        rec = self.records[object_id]
        if rec.state != BlockState.ABSENT:
            raise IllegalTransition(f"{object_id} 已在召回/本地，不能重复发起：{rec.state.value}")
        if not rec.is_remote_available:
            raise IllegalTransition(f"{object_id} 仓储已不可用：{rec.state.value}")
        rec.state = BlockState.FETCHING
        rec.fetch_ref = ref
        rec.fetch_issued_at = minute
        self._consume_space()

    def mark_bytes_ready(self, object_id: str, minute: int, ref: str | None = None) -> None:
        """冷层/热层回执：字节已到本地（一次读取只代表获取字节，不代表完整）。"""
        rec = self.records[object_id]
        if rec.state == BlockState.LOCAL_VERIFIED:
            return  # 重复回执，幂等
        if rec.state == BlockState.LOCAL_BAD:
            return
        if rec.state != BlockState.FETCHING:
            raise IllegalTransition(f"{object_id} 收到就绪回执但未在召回：{rec.state.value}")
        if ref is not None and rec.fetch_ref is not None and ref != rec.fetch_ref:
            # 外部重发的同对象召回：演练里允许新 ref 覆盖，记录但不改变占用。
            rec.fetch_ref = ref
        rec.state = BlockState.LOCAL_UNVERIFIED
        rec.fetch_ready_at = minute

    def verify(self, object_id: str, minute: int) -> bool:
        """对本地字节做 SHA-256 校验，返回是否通过。坏块永久标记。"""
        rec = self.records[object_id]
        if rec.state == BlockState.LOCAL_VERIFIED:
            return True
        if rec.state == BlockState.LOCAL_BAD:
            return False
        if rec.state != BlockState.LOCAL_UNVERIFIED:
            raise IllegalTransition(f"{object_id} 无本地字节，无法校验：{rec.state.value}")
        actual = self.bundle.actual_digests[object_id]
        expected = self.bundle.blocks[object_id].digest
        if actual == expected:
            rec.state = BlockState.LOCAL_VERIFIED
            rec.verified_at = minute
            return True
        rec.state = BlockState.LOCAL_BAD
        rec.bad_reason = f"digest mismatch: actual={actual[:12]} expected={expected[:12]}"
        return False

    def evict_bad(self, object_id: str, minute: int) -> bool:
        """损坏且无人 pin 的字节立即驱逐，释放空间。"""
        rec = self.records[object_id]
        if rec.state != BlockState.LOCAL_BAD or rec.pins:
            return False
        rec.state = BlockState.ABSENT
        rec.evicted_at = minute
        rec.fetch_ref = None
        rec.fetch_issued_at = None
        rec.fetch_ready_at = None
        self._release_space()
        return True

    # ---- 承诺保护 -------------------------------------------------------

    def pin(self, object_id: str, request_ref: str) -> None:
        self.records[object_id].pins.add(request_ref)

    def pin_many(self, objects: tuple[str, ...], request_ref: str) -> None:
        for object_id in objects:
            self.pin(object_id, request_ref)

    def unpin_request(self, request_ref: str) -> list[str]:
        """取消/失败/交付后释放某申请的全部 pin，返回被释放的块。"""
        released: list[str] = []
        for rec in self.records.values():
            if request_ref in rec.pins:
                rec.pins.discard(request_ref)
                released.append(rec.object_id)
        return released

    def pinned_by(self, object_id: str) -> tuple[str, ...]:
        return self.records[object_id].pinned_by()

    def has_pin(self, object_id: str) -> bool:
        return self.records[object_id].has_pin()

    # ---- 删除回执 -------------------------------------------------------

    def claim_delete(self, object_id: str, minute: int) -> None:
        rec = self.records[object_id]
        if rec.state in (BlockState.CLAIMED_REMOTE, BlockState.DELETED_REMOTE):
            return
        # 已核验本地副本可以保留；未读完的对象被领取后仍无法承诺给新申请。
        rec.state = BlockState.CLAIMED_REMOTE
        rec.claimed_at = minute

    def confirm_delete(self, object_id: str, minute: int) -> None:
        rec = self.records[object_id]
        if rec.state == BlockState.DELETED_REMOTE:
            return
        rec.state = BlockState.DELETED_REMOTE
        rec.deleted_at = minute
        # verified 本地字节保留至最后一个 pin 释放后驱逐；空间仍被占用。

    def evict_if_unpinned(self, object_id: str, minute: int) -> bool:
        """驱逐无 pin 的本地字节并释放空间；坏块回到 absent 可重新召回。"""
        rec = self.records[object_id]
        if rec.pins or rec.evicted_at is not None:
            return False
        if not rec.occupies_space:
            return False
        rec.evicted_at = minute
        previous = rec.state
        rec.state = BlockState.ABSENT
        rec.fetch_ref = None
        rec.fetch_issued_at = None
        rec.fetch_ready_at = None
        if previous == BlockState.LOCAL_BAD:
            rec.bad_reason = None
        self._release_space()
        return True

    def evict_unpinned_to_relieve_pressure(self, minute: int) -> list[str]:
        """空间低于保留线时，驱逐无 pin 的本地块（先坏块、再已删仓储、最后普通块）。"""
        evicted: list[str] = []

        def order_key(rec: BlockRecord) -> tuple[int, int]:
            rank = {
                BlockState.LOCAL_BAD: 0,
                BlockState.DELETED_REMOTE: 1,
                BlockState.LOCAL_VERIFIED: 2,
                BlockState.LOCAL_UNVERIFIED: 3,
            }.get(rec.state, 9)
            return rank, rec.verified_at or minute

        candidates = sorted(
            (rec for rec in self.records.values()
             if not rec.pins and rec.occupies_space),
            key=order_key,
        )
        for rec in candidates:
            if self.free_units >= self._min_free():
                break
            if self.evict_if_unpinned(rec.object_id, minute):
                evicted.append(rec.object_id)
        return evicted

    # ---- 新承诺的可用性 -------------------------------------------------

    def eligible_for_new_promise(self, object_id: str) -> tuple[bool, str]:
        """块能否进入一个新恢复申请的计划。"""
        rec = self.records[object_id]
        if rec.state == BlockState.LOCAL_BAD:
            return False, f"块 {object_id} 校验失败：{rec.bad_reason}"
        if rec.state in (BlockState.CLAIMED_REMOTE, BlockState.DELETED_REMOTE):
            if rec.usable_verified:
                return True, f"块 {object_id} 仓储已删除，仅本地核验副本可用"
            return False, f"块 {object_id} 仓储已被领取/删除且无本地核验副本"
        return True, ""

    def snapshot(self) -> dict[str, dict[str, object]]:
        out: dict[str, dict[str, object]] = {}
        for object_id, rec in sorted(self.records.items()):
            out[object_id] = {
                "state": rec.state.value,
                "pins": sorted(rec.pins),
                "fetch_ref": rec.fetch_ref,
                "fetch_ready_at": rec.fetch_ready_at,
                "verified_at": rec.verified_at,
                "bad_reason": rec.bad_reason,
                "claimed_at": rec.claimed_at,
                "deleted_at": rec.deleted_at,
                "evicted_at": rec.evicted_at,
                "occupies_space": rec.occupies_space,
            }
        return out


class IllegalTransition(RuntimeError):
    """块状态机收到了不允许的迁移。"""
