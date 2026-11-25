"""块依赖拓扑、组提交标记与业务文档重建。

manifest 中每个服务的块通过 parent 串成链；group+mark 相同的块属于同一批
提交，组内成员必须在同一 mark 上共同启用。文档由 base 键值叠加各 delta 的
set/remove 得到。
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from .feeds import Block, FeedBundle, FeedError


@dataclass(frozen=True)
class GroupCommit:
    """一个组提交标记下的同批块：service -> 块编号。"""

    group: str
    mark: str
    minute: int
    leaves: dict[str, str]

    @property
    def services(self) -> tuple[str, ...]:
        return tuple(sorted(self.leaves))


class Topology:
    def __init__(self, bundle: FeedBundle):
        self.bundle = bundle
        self.blocks = bundle.blocks
        self._chains: dict[str, list[str]] = {}
        self._groups: dict[str, dict[str, GroupCommit]] = {}
        self._standalone: dict[str, list[str]] = {}
        self._service_group: dict[str, str] = {}
        self._build()

    def _build(self) -> None:
        # 按服务聚合并校验 parent 链（无环、单根、时间递增已在 feeds 中部分校验）。
        per_service: dict[str, list[Block]] = {}
        for block in self.blocks.values():
            per_service.setdefault(block.service, []).append(block)

        for service, service_blocks in per_service.items():
            by_id = {b.object: b for b in service_blocks}
            roots = [b.object for b in service_blocks if b.parent is None]
            if len(roots) != 1:
                raise FeedError(f"服务 {service} 的块链应有唯一根，实际为 {sorted(roots)}")
            order: list[str] = []
            current = roots[0]
            seen: set[str] = set()
            while True:
                if current in seen:
                    raise FeedError(f"服务 {service} 的块链存在环：{current}")
                seen.add(current)
                order.append(current)
                children = [b.object for b in service_blocks if b.parent == current]
                if not children:
                    break
                if len(children) > 1:
                    raise FeedError(f"块 {current} 有多个后继，演练模型只支持线性链")
                current = children[0]
            if set(order) != set(by_id):
                raise FeedError(f"服务 {service} 存在无法挂到根链上的块")
            self._chains[service] = order

        # 组提交标记：同 group 同 mark 的块必须覆盖该组的全部服务、时间一致。
        group_services: dict[str, set[str]] = {}
        raw_marks: dict[tuple[str, str], dict[str, str]] = {}
        mark_minutes: dict[tuple[str, str], int] = {}
        for block in self.blocks.values():
            if block.group is None:
                self._standalone.setdefault(block.service, []).append(block.object)
                continue
            group_services.setdefault(block.group, set()).add(block.service)
            if block.service in self._service_group and self._service_group[block.service] != block.group:
                raise FeedError(f"服务 {block.service} 同时属于多个组")
            self._service_group[block.service] = block.group
            key = (block.group, block.mark)
            raw_marks.setdefault(key, {})[block.service] = block.object
            if key in mark_minutes and mark_minutes[key] != block.minute:
                raise FeedError(f"组 {block.group} 的 mark {block.mark} 下块时间不一致")
            mark_minutes[key] = block.minute

        for (group, mark), leaves in raw_marks.items():
            if set(leaves) != group_services[group]:
                missing = sorted(group_services[group] - set(leaves))
                raise FeedError(f"组 {group} 的 mark {mark} 缺少成员块：{missing}")
            self._groups.setdefault(group, {})[mark] = GroupCommit(
                group=group, mark=mark, minute=mark_minutes[(group, mark)], leaves=dict(leaves),
            )
        for marks in self._groups.values():
            minutes = [commit.minute for commit in marks.values()]
            if len(set(minutes)) != len(minutes):
                raise FeedError("同一组内不同 mark 的提交时间必须可区分")

    # ---- 链与闭包 -------------------------------------------------------

    def chain(self, service: str) -> list[str]:
        """根 -> 叶的块编号序列。"""
        return list(self._chains[service])

    def services(self) -> tuple[str, ...]:
        return tuple(sorted(self._chains))

    def groups(self) -> tuple[str, ...]:
        return tuple(sorted(self._groups))

    def marks(self, group: str) -> list[GroupCommit]:
        return self.group_marks(group)

    def closure(self, leaf: str) -> tuple[str, ...]:
        """叶块启用所需的全部块（含自身，根 -> 叶）。"""
        out: list[str] = []
        current: str | None = leaf
        while current is not None:
            out.append(current)
            current = self.blocks[current].parent
        return tuple(reversed(out))

    def service_at_or_before(self, service: str, minute: int) -> str | None:
        """独立服务在给定时间前（含）的最新块。"""
        candidates = [o for o in self._chains[service] if self.blocks[o].minute <= minute]
        return candidates[-1] if candidates else None

    def group_at_or_before(self, group: str, minute: int) -> GroupCommit | None:
        commits = sorted(self._groups[group].values(), key=lambda c: c.minute)
        candidates = [c for c in commits if c.minute <= minute]
        return candidates[-1] if candidates else None

    def group_marks(self, group: str) -> list[GroupCommit]:
        return sorted(self._groups[group].values(), key=lambda c: c.minute)

    def group_of_service(self, service: str) -> str | None:
        return self._service_group.get(service)

    def is_standalone(self, service: str) -> bool:
        return service in self._standalone

    # ---- 文档重建 -------------------------------------------------------

    def reconstruct(self, leaf: str) -> dict[str, object]:
        """把叶块及其祖先叠加成最终业务文档（仅基于已校验字节时才有意义）。"""
        document: dict[str, object] = {}
        for object_id in self.closure(leaf):
            try:
                payload = json.loads(self.bundle.object_bytes[object_id])
            except (json.JSONDecodeError, KeyError) as exc:
                raise FeedError(f"块 {object_id} 内容无法解析") from exc
            block = self.blocks[object_id]
            if block.parent is None:
                if set(payload) != {"base"} or not isinstance(payload["base"], dict):
                    raise FeedError(f"根块 {object_id} 应只含 base 对象")
                document = dict(payload["base"])
            else:
                if set(payload) != {"set", "remove"}:
                    raise FeedError(f"增量块 {object_id} 应只含 set/remove")
                sets, removes = payload["set"], payload["remove"]
                if not isinstance(sets, dict) or not isinstance(removes, list):
                    raise FeedError(f"增量块 {object_id} 的 set/remove 类型错误")
                for key in removes:
                    document.pop(key, None)
                document.update(sets)
        return document
