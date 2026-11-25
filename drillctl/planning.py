"""恢复点候选、可行性判断、备选与时间损失。

规划只回答“在当前块状态下，哪些恢复点可以启用、哪些块挡住了路”，作业
时间推演交给 scheduler，块状态判断交给 store。组内成员只在同一 mark 上
共同启用；某个服务的链在某点不可用，整个组都要回退，不能只替换一个服务。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .feeds import Block, FeedBundle
from .store import BlockState, BlockStore
from .topology import Topology


@dataclass(frozen=True)
class ServicePoint:
    """一个服务在恢复点上选用的叶块。"""

    service: str
    leaf: str
    minute: int
    mark: str | None
    group: str | None


@dataclass(frozen=True)
class RecoveryPoint:
    """一次申请在某业务时间上的完整恢复点：各服务叶块 + 依赖闭包。"""

    point_id: str
    """组用 mark，独立服务用叶块编号；多服务拼接。"""
    business_minute: int
    services: tuple[ServicePoint, ...]
    blocks: tuple[str, ...]
    group_marks: tuple[str, ...]


@dataclass
class BlockVerdict:
    object_id: str
    eligible: bool
    reason: str
    state: str
    verified_local: bool


@dataclass
class PointEvaluation:
    point: RecoveryPoint
    feasible: bool
    verdicts: dict[str, BlockVerdict]
    blockers: tuple[str, ...]
    service_blocks: dict[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def bad_blocks(self) -> tuple[str, ...]:
        return tuple(o for o, v in self.verdicts.items()
                     if self.point and v.state == BlockState.LOCAL_BAD.value)

    @property
    def missing_blocks(self) -> tuple[str, ...]:
        return tuple(o for o, v in self.verdicts.items() if not v.eligible)


@dataclass
class Alternative:
    point: RecoveryPoint
    time_loss: int
    """相对申请期望时间（最新候选的业务时间）回退的分钟数。"""
    blockers_on_preferred: tuple[str, ...]
    note: str


@dataclass
class Plan:
    request_ref: str
    members: tuple[str, ...]
    earliest_point: int
    deadline: int
    preferred: RecoveryPoint | None
    chosen: RecoveryPoint | None
    evaluations: list[PointEvaluation]
    alternatives: list[Alternative]
    failure_reasons: list[str]
    chosen_evaluation: PointEvaluation | None = None

    @property
    def feasible(self) -> bool:
        return self.chosen is not None

    @property
    def time_loss(self) -> int:
        if self.chosen is None or self.preferred is None:
            return 0
        return self.preferred.business_minute - self.chosen.business_minute


class Planner:
    def __init__(self, bundle: FeedBundle, topology: Topology, store: BlockStore):
        self.bundle = bundle
        self.topo = topology
        self.store = store

    # ---- 候选生成 -------------------------------------------------------

    def candidate_points(self, members: tuple[str, ...]) -> list[RecoveryPoint]:
        """按业务时间从新到旧列出申请成员集合的所有一致恢复点。

        组服务只能整组落在同一 mark 上；独立服务各自取不晚于该业务时间的
        最新块。时间线取所有组成员 mark 时间与独立服务块时间的并集。
        """
        members_set = set(members)
        grouped: dict[str, set[str]] = {}
        standalone: set[str] = set()
        for service in members:
            group = self.topo.group_of_service(service)
            if group is None:
                if not self.topo.is_standalone(service):
                    raise ValueError(f"服务 {service} 既未入组也不是独立服务")
                standalone.add(service)
            else:
                grouped.setdefault(group, set()).add(service)

        # 每个组必须整体出现：申请里的组成员要覆盖该组全部服务。
        for group, services in grouped.items():
            commit_services = set(self.topo.marks(group)[0].services)
            if services != commit_services:
                raise ValueError(
                    f"组 {group} 必须整组共同启用，缺少 {sorted(commit_services - services)}",
                )

        # 时间线：组 mark 时间 + 独立服务块时间。
        timeline: set[int] = set()
        for service in standalone:
            timeline.update(self.bundle.blocks[o].minute for o in self.topo.chain(service))
        for group in grouped:
            timeline.update(commit.minute for commit in self.topo.marks(group))

        points: list[RecoveryPoint] = []
        prev_blocks: tuple[str, ...] | None = None
        for minute in sorted(timeline, reverse=True):
            sps: list[ServicePoint] = []
            ok = True
            for group in grouped:
                commit = self.topo.group_at_or_before(group, minute)
                if commit is None:
                    ok = False
                    break
                for service, leaf in commit.leaves.items():
                    sps.append(ServicePoint(service, leaf, commit.minute, commit.mark, group))
            if not ok:
                continue
            for service in standalone:
                leaf = self.topo.service_at_or_before(service, minute)
                if leaf is None:
                    ok = False
                    break
                block = self.bundle.blocks[leaf]
                sps.append(ServicePoint(service, leaf, block.minute, None, None))
            if not ok or {s.service for s in sps} != members_set:
                continue
            point = self._assemble(f"min:{minute}", minute, tuple(sps))
            if prev_blocks != point.blocks:
                points.append(point)
                prev_blocks = point.blocks
        return points

    def _assemble(self, point_id: str, business_minute: int, sps: tuple[ServicePoint, ...]) -> RecoveryPoint:
        blocks: list[str] = []
        seen: set[str] = set()
        for sp in sorted(sps, key=lambda x: x.service):
            for object_id in self.topo.closure(sp.leaf):
                if object_id not in seen:
                    seen.add(object_id)
                    blocks.append(object_id)
        marks = tuple(sorted({sp.mark for sp in sps if sp.mark is not None}))
        # 优先用组 mark 作为恢复点标识，纯独立服务才退回时间标识。
        if marks:
            point_id = "mark:" + "+".join(marks)
        return RecoveryPoint(point_id, business_minute, tuple(sorted(sps, key=lambda x: x.service)),
                             tuple(blocks), marks)

    # ---- 可行性 ---------------------------------------------------------

    def evaluate(self, point: RecoveryPoint, allow_local_only: bool = True) -> PointEvaluation:
        """依据块当前在仓/本地状态判断恢复点是否可启用。"""
        verdicts: dict[str, BlockVerdict] = {}
        service_blocks: dict[str, tuple[str, ...]] = {}
        blockers: list[str] = []
        for sp in point.services:
            chain = self.topo.closure(sp.leaf)
            service_blocks[sp.service] = chain
            for object_id in chain:
                if object_id in verdicts:
                    continue
                rec = self.store.record(object_id)
                eligible, reason = self._verdict(rec, allow_local_only)
                verdict = BlockVerdict(
                    object_id, eligible, reason, rec.state.value, rec.usable_verified,
                )
                verdicts[object_id] = verdict
                if not eligible:
                    blockers.append(object_id)
        return PointEvaluation(
            point=point, feasible=not blockers, verdicts=verdicts,
            blockers=tuple(blockers), service_blocks=service_blocks,
        )

    @staticmethod
    def _verdict(rec, allow_local_only: bool) -> tuple[bool, str]:
        state = rec.state
        if state == BlockState.LOCAL_BAD:
            return False, f"校验失败：{rec.bad_reason}"
        if state in (BlockState.CLAIMED_REMOTE, BlockState.DELETED_REMOTE):
            if rec.usable_verified and allow_local_only:
                return True, "仓储不可用，但本地核验副本可独立使用"
            return False, "仓储对象已被领取/删除，且没有已核验本地副本"
        if state in (BlockState.ABSENT, BlockState.FETCHING, BlockState.LOCAL_UNVERIFIED):
            return True, "仓储对象仍可召回（尚未读完）"
        if state == BlockState.LOCAL_VERIFIED:
            return True, "本地已核验，可独立使用"
        return False, f"未知状态 {state.value}"

    # ---- 完整规划 -------------------------------------------------------

    def plan(
        self, request_ref: str, members: tuple[str, ...],
        earliest_point: int, deadline: int,
    ) -> Plan:
        candidates = [p for p in self.candidate_points(members)
                      if p.business_minute >= earliest_point]
        evaluations = [self.evaluate(p) for p in candidates]
        failure_reasons: list[str] = []
        if not candidates:
            failure_reasons.append(
                f"最旧可接受时间 {earliest_point} 之前没有覆盖 {sorted(members)} 的一致恢复点",
            )

        preferred = candidates[0] if candidates else None
        chosen: RecoveryPoint | None = None
        chosen_eval: PointEvaluation | None = None
        alternatives: list[Alternative] = []
        for index, evaluation in enumerate(evaluations):
            point = evaluation.point
            if evaluation.feasible:
                if chosen is None:
                    chosen = point
                    chosen_eval = evaluation
                elif preferred is not None:
                    alternatives.append(Alternative(
                        point=point,
                        time_loss=preferred.business_minute - point.business_minute,
                        blockers_on_preferred=evaluations[0].blockers,
                        note=self._alternative_note(evaluation),
                    ))
            elif preferred is not None and point is not preferred:
                alternatives.append(Alternative(
                    point=point,
                    time_loss=preferred.business_minute - point.business_minute,
                    blockers_on_preferred=evaluation.blockers,
                    note="该备选当前也不可用：" + "；".join(
                        evaluation.verdicts[b].reason for b in evaluation.blockers),
                ))

        if chosen is None and candidates:
            reasons = sorted({
                f"{b}（{evaluations[0].verdicts[b].reason}）"
                for ev in evaluations for b in ev.blockers
            })
            failure_reasons.append(
                "到最旧可接受时间 "
                f"{earliest_point} 仍无完整可用依赖，涉及：{'; '.join(reasons)}",
            )

        plan = Plan(
            request_ref=request_ref, members=members, earliest_point=earliest_point,
            deadline=deadline, preferred=preferred, chosen=chosen,
            evaluations=evaluations, alternatives=alternatives, failure_reasons=failure_reasons,
        )
        plan.chosen_evaluation = chosen_eval
        return plan

    @staticmethod
    def _alternative_note(evaluation: PointEvaluation) -> str:
        local_only = [
            o for o, v in evaluation.verdicts.items()
            if v.eligible and v.state in (
                BlockState.CLAIMED_REMOTE.value, BlockState.DELETED_REMOTE.value,
            )
        ]
        if local_only:
            return f"可行，但 {sorted(local_only)} 仅靠本地核验副本"
        return "依赖完整且在仓可用"

    def block(self, object_id: str) -> Block:
        return self.bundle.blocks[object_id]
