"""演练资料读取与基础校验。

资料目录属于不可信输入：只做解析与字段校验，绝不从中加载代码。
所有路径通过参数传入，读取对象字节时限定在 ``objects`` 目录内。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

FEED_NAMES = ("manifest", "services", "requests", "activity")
MANIFEST_FIELDS = (
    "object", "service", "group", "mark", "minute", "tier", "parent",
    "retrieve_min", "restore_min", "retire_min", "digest",
)
SERVICE_FIELDS = ("unit", "members", "fetch_slots", "restore_slots", "minimum_free_space")
REQUEST_FIELDS = ("at", "kind", "reference", "members", "earliest_point", "deadline", "object")
ACTIVITY_FIELDS = ("at", "kind", "reference", "object", "free_units")
TIERS = ("hot", "cold")
REQUEST_KINDS = ("restore", "cleanup", "cancel")
ACTIVITY_KINDS = ("fetch_ready", "delete_claimed", "deleted", "storage_pressure")


class FeedError(ValueError):
    """资料内容不符合演练格式。"""


@dataclass(frozen=True)
class Block:
    """manifest 中的一个备份块（发布元数据，仓储字节另行读取校验）。"""

    object: str
    service: str
    group: str | None
    mark: str | None
    minute: int
    tier: str
    parent: str | None
    retrieve_min: int
    restore_min: int
    retire_min: int
    digest: str


@dataclass(frozen=True)
class Unit:
    """一个共用现场（恢复站）的能力与共同启用成员。"""

    unit: str
    members: tuple[str, ...]
    fetch_slots: int
    restore_slots: int
    minimum_free_space: int


@dataclass(frozen=True)
class Request:
    at: int
    kind: str
    reference: str
    members: tuple[str, ...]
    earliest_point: int | None
    deadline: int | None
    object: str | None


@dataclass(frozen=True)
class Receipt:
    at: int
    kind: str
    reference: str
    object: str | None
    free_units: int | None


@dataclass
class FeedBundle:
    """一次演练的全部输入。"""

    directory: Path
    blocks: dict[str, Block]
    units: tuple[Unit, ...]
    requests: tuple[Request, ...]
    receipts: tuple[Receipt, ...]
    object_bytes: dict[str, bytes] = field(default_factory=dict)
    actual_digests: dict[str, str] = field(default_factory=dict)

    def block(self, object_id: str) -> Block:
        try:
            return self.blocks[object_id]
        except KeyError as exc:
            raise FeedError(f"manifest 中不存在对象：{object_id}") from exc

    def unit_of_service(self, service: str) -> Unit:
        for unit in self.units:
            if service in unit.members:
                return unit
        raise FeedError(f"服务 {service} 不属于任何恢复站单元")


def _iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise FeedError(f"{path.name} 第 {number} 行不是有效 JSON：{exc}") from exc
            if not isinstance(row, dict):
                raise FeedError(f"{path.name} 第 {number} 行应为 JSON 对象")
            yield row


def _check_fields(row: dict[str, Any], fields: tuple[str, ...], where: str) -> None:
    if set(row) != set(fields):
        missing = sorted(set(fields) - set(row))
        extra = sorted(set(row) - set(fields))
        raise FeedError(f"{where} 字段不符，缺少 {missing}，多出 {extra}")


def load_bundle(directory: str | Path) -> FeedBundle:
    """加载并交叉校验整个资料目录，同时读取合成对象字节计算实际摘要。"""
    root = Path(directory).resolve()
    if not root.is_dir():
        raise FeedError(f"资料目录不存在：{root}")

    blocks: dict[str, Block] = {}
    for row in _iter_jsonl(root / "manifest.jsonl"):
        _check_fields(row, MANIFEST_FIELDS, f"manifest/{row.get('object')}")
        if row["object"] in blocks:
            raise FeedError(f"manifest 对象重复：{row['object']}")
        if row["tier"] not in TIERS:
            raise FeedError(f"对象 {row['object']} 的 tier 非法：{row['tier']}")
        if not (isinstance(row["digest"], str) and len(row["digest"]) == 64):
            raise FeedError(f"对象 {row['object']} 的 digest 应为 64 位十六进制")
        if (row["group"] is None) != (row["mark"] is None):
            raise FeedError(f"对象 {row['object']} 的 group 与 mark 必须同时为空或同时有值")
        blocks[row["object"]] = Block(
            object=row["object"], service=row["service"], group=row["group"],
            mark=row["mark"], minute=int(row["minute"]), tier=row["tier"],
            parent=row["parent"], retrieve_min=int(row["retrieve_min"]),
            restore_min=int(row["restore_min"]), retire_min=int(row["retire_min"]),
            digest=row["digest"].lower(),
        )

    for block in blocks.values():
        if block.parent is not None:
            if block.parent not in blocks:
                raise FeedError(f"对象 {block.object} 的 parent 不存在：{block.parent}")
            parent = blocks[block.parent]
            if parent.service != block.service:
                raise FeedError(f"对象 {block.object} 的 parent 必须属于同一服务")
            if parent.minute >= block.minute:
                raise FeedError(f"对象 {block.object} 的 parent 时间必须更早")

    units: list[Unit] = []
    seen_services: dict[str, str] = {}
    for row in _iter_jsonl(root / "services.jsonl"):
        _check_fields(row, SERVICE_FIELDS, f"services/{row.get('unit')}")
        members = tuple(row["members"])
        if not members:
            raise FeedError(f"单元 {row['unit']} 的 members 不能为空")
        for member in members:
            if member in seen_services:
                raise FeedError(f"服务 {member} 同时属于 {seen_services[member]} 与 {row['unit']}")
            seen_services[member] = row["unit"]
        if min(row["fetch_slots"], row["restore_slots"]) < 1:
            raise FeedError(f"单元 {row['unit']} 的作业槽至少为 1")
        if row["minimum_free_space"] < 0:
            raise FeedError(f"单元 {row['unit']} 的 minimum_free_space 不能为负")
        units.append(Unit(
            unit=row["unit"], members=members, fetch_slots=int(row["fetch_slots"]),
            restore_slots=int(row["restore_slots"]),
            minimum_free_space=int(row["minimum_free_space"]),
        ))

    manifest_services = {b.service for b in blocks.values()}
    unknown = manifest_services - set(seen_services)
    if unknown:
        raise FeedError(f"manifest 中的服务没有恢复站单元：{sorted(unknown)}")

    requests: list[Request] = []
    for row in _iter_jsonl(root / "requests.jsonl"):
        _check_fields(row, REQUEST_FIELDS, f"requests/{row.get('reference')}")
        if row["kind"] not in REQUEST_KINDS:
            raise FeedError(f"工单 {row['reference']} 类型非法：{row['kind']}")
        req = Request(
            at=int(row["at"]), kind=row["kind"], reference=row["reference"],
            members=tuple(row["members"] or ()),
            earliest_point=None if row["earliest_point"] is None else int(row["earliest_point"]),
            deadline=None if row["deadline"] is None else int(row["deadline"]),
            object=row["object"],
        )
        if req.kind == "restore":
            if not req.members or req.earliest_point is None or req.deadline is None:
                raise FeedError(f"恢复申请 {req.reference} 缺少 members/earliest_point/deadline")
            unknown_members = [m for m in req.members if m not in seen_services]
            if unknown_members:
                raise FeedError(f"恢复申请 {req.reference} 含未知服务：{unknown_members}")
            if req.deadline < req.at:
                raise FeedError(f"恢复申请 {req.reference} 的 deadline 早于申请时间")
        elif req.kind == "cleanup":
            if not req.object:
                raise FeedError(f"清理工单 {req.reference} 缺少 object")
            if req.object not in blocks:
                raise FeedError(f"清理工单 {req.reference} 目标不存在：{req.object}")
        elif req.kind == "cancel" and not req.reference:
            raise FeedError("取消工单缺少 reference")
        requests.append(req)

    receipts: list[Receipt] = []
    for row in _iter_jsonl(root / "activity.jsonl"):
        _check_fields(row, ACTIVITY_FIELDS, f"activity/{row.get('reference')}")
        if row["kind"] not in ACTIVITY_KINDS:
            raise FeedError(f"回执 {row['reference']} 类型非法：{row['kind']}")
        receipt = Receipt(
            at=int(row["at"]), kind=row["kind"], reference=row["reference"],
            object=row["object"], free_units=row["free_units"],
        )
        if receipt.kind in ("fetch_ready", "delete_claimed", "deleted") and not receipt.object:
            raise FeedError(f"{receipt.kind} 回执 {receipt.reference} 缺少 object")
        if receipt.kind == "storage_pressure" and receipt.free_units is None:
            raise FeedError(f"storage_pressure 回执 {receipt.reference} 缺少 free_units")
        if receipt.object is not None and receipt.object not in blocks:
            raise FeedError(f"回执引用了未知对象：{receipt.object}")
        receipts.append(receipt)

    object_bytes: dict[str, bytes] = {}
    actual_digests: dict[str, str] = {}
    objects_dir = root / "objects"
    for object_id, block in blocks.items():
        path = objects_dir / f"{object_id}.json"
        # 合成对象是不可信数据：限定在 objects 目录内，按字节读取。
        if not path.is_file():
            raise FeedError(f"对象字节缺失：{path.name}")
        data = path.read_bytes()
        object_bytes[object_id] = data
        actual_digests[object_id] = hashlib.sha256(data).hexdigest()

    return FeedBundle(
        directory=root, blocks=blocks, units=tuple(units),
        requests=tuple(requests), receipts=tuple(receipts),
        object_bytes=object_bytes, actual_digests=actual_digests,
    )
