#!/usr/bin/env python3
"""恢复与清理本地控制程序入口。

设计为可在无外网、隔离模式（python -I）下运行：脚本自行把项目根加入
sys.path，只使用标准库。资料目录与输出目录均通过参数传入，对象字节作为
不可信数据只读解析，绝不从中加载代码。

子命令：
  run        按 requests/activity 完整跑一遍演练，输出报告与 journal
  reconcile  从 journal 重启，先对账已发生的外部效果，再继续到 horizon
  report     从已有 journal 重新生成查询报告
  inspect    打印 manifest 推导的恢复点候选与当前块状态（不跑工单）
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from drillctl.controller import Controller, RequestStatus  # noqa: E402
from drillctl.feeds import FeedError, load_bundle  # noqa: E402
from drillctl.report import DrillReport  # noqa: E402

DEFAULT_INITIAL_FREE = 12


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")


def _load_initial_free(bundle, explicit: int | None) -> int:
    if explicit is not None:
        return explicit
    pressure = [r for r in bundle.receipts if r.kind == "storage_pressure"]
    if pressure:
        # 演练开始时取第一次压力事件之前的容量：用最大观测值兜底。
        return max(r.free_units for r in pressure)
    return DEFAULT_INITIAL_FREE


def cmd_run(args: argparse.Namespace) -> int:
    bundle = load_bundle(args.directory)
    initial_free = _load_initial_free(bundle, args.initial_free)
    start = min(r.at for r in bundle.requests)
    horizon = max(
        [r.at for r in bundle.requests] + [r.at for r in bundle.receipts] + [start],
    )
    controller = Controller(bundle, initial_free, start_minute=start)
    controller.run(horizon)
    return _emit(controller, args, journal_suffix=args.journal_name or "journal.json")


def cmd_reconcile(args: argparse.Namespace) -> int:
    bundle = load_bundle(args.directory)
    journal_path = Path(args.journal)
    if not journal_path.is_file():
        raise FeedError(f"journal 不存在：{journal_path}")
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    initial_free = _load_initial_free(bundle, args.initial_free)
    controller = Controller(bundle, initial_free, start_minute=journal["minute"])
    notes = controller.reconcile(journal, journal["minute"])
    if args.horizon is not None:
        horizon = args.horizon
        if horizon > controller.now:
            controller.run(horizon)
    else:
        # 默认续跑到全部申请终态（含资料窗口之外的 settle）。
        controller.run(max(
            [r.at for r in bundle.requests] + [r.at for r in bundle.receipts],
        ))
    output = _emit(controller, args, journal_suffix="journal-resumed.json")
    if args.output:
        _write_json(Path(args.output) / "reconcile-notes.json",
                    {"journal_minute": journal["minute"], "notes": notes})
    print("\n".join(notes))
    return output


def cmd_report(args: argparse.Namespace) -> int:
    bundle = load_bundle(args.directory)
    journal = json.loads(Path(args.journal).read_text(encoding="utf-8"))
    controller = Controller(bundle, journal.get("free_units", DEFAULT_INITIAL_FREE),
                            start_minute=journal["minute"])
    controller.reconcile(journal, journal["minute"])
    report = DrillReport(controller).as_dict()
    if args.output:
        _write_json(Path(args.output) / "report.json", report)
    _print_text_report(report)
    return 0


def cmd_inspect(args: argparse.Namespace) -> int:
    from drillctl.planning import Planner
    from drillctl.topology import Topology

    bundle = load_bundle(args.directory)
    topo = Topology(bundle)
    print("== 块链与组提交 ==")
    for service in topo.services():
        print(f"{service}: " + " -> ".join(topo.chain(service)))
    for group in topo.groups():
        for commit in topo.marks(group):
            print(f"group {group} mark {commit.mark} @ {commit.minute}: {commit.leaves}")
    print("\n== 各恢复站成员集合的恢复点候选（新 -> 旧） ==")
    for unit in bundle.units:
        ctl_store = Controller(bundle, _load_initial_free(bundle, args.initial_free), 0).store
        planner = Planner(bundle, topo, ctl_store)
        print(f"[unit {unit.unit}] members={list(unit.members)}")
        for point in planner.candidate_points(unit.members):
            print(f"  {point.point_id:>10} @{point.business_minute:>3} blocks={list(point.blocks)}")
    print("\n== 实际摘要校验 ==")
    for object_id, block in bundle.blocks.items():
        actual = bundle.actual_digests[object_id]
        ok = actual == block.digest
        print(f"  {object_id:>8} {'OK ' if ok else 'BAD'} tier={block.tier} "
              f"retire={block.retire_min}")
    return 0


def _emit(controller: Controller, args: argparse.Namespace, journal_suffix: str) -> int:
    report = DrillReport(controller).as_dict()
    if args.output:
        out_dir = Path(args.output)
        _write_json(out_dir / journal_suffix, controller.journal())
        _write_json(out_dir / "report.json", report)
        _write_jsonl(out_dir / "decisions.jsonl", report["decisions"])
    _print_text_report(report)
    failed = any(r["status"] == RequestStatus.FAILED.value for r in report["recoveries"])
    return 2 if (failed and args.fail_on_failed) else 0


def _print_text_report(report: dict) -> None:
    print(f"# 演练报告（horizon={report['horizon']} 分钟）\n")
    print("## 恢复申请")
    for row in report["recoveries"]:
        head = f"- {row['reference']} [{row['status']}] 成员={row['members']} deadline={row['deadline']}"
        print(head)
        if row["status"] == "delivered":
            timing = "按时" if row["on_time"] else f"超时 {row['delay']} 分钟"
            loss = f"，时间损失 {row['time_loss']} 分钟" if row.get("time_loss") else ""
            print(f"    交付 {row['delivered_point']}（业务时间 {row['business_minute']}）"
                  f" 于第 {row['delivered_at']} 分钟，{timing}"
                  f"，块={row['blocks']}{loss}")
            for service, doc in row["content"].items():
                print(f"    {service} 文档: {json.dumps(doc, ensure_ascii=False)}")
        elif row["status"] == "failed":
            for reason in row["failure_reasons"]:
                print(f"    失败依据：{reason}")
            for alt in row["alternatives"]:
                print(f"    备选 {alt['point']}@{alt['business_minute']} "
                      f"损失 {alt['time_loss']} 分钟：{alt['note']}")
        for line in row["history"]:
            print(f"    | {line}")
    print("\n## 回退与删除原因")
    for cause in report["rollback_causes"]:
        print(f"- 第 {cause['minute']} 分钟 {cause['cause']}: "
              f"{json.dumps({k: v for k, v in cause.items() if k not in ('minute', 'cause')}, ensure_ascii=False)}")
    print("\n## 资源占用")
    occ = report["resource_occupancy"]
    print(f"- 空闲空间 {occ['free_units']}（保留 {occ['minimum_free_space']}），"
          f"活跃 pin={json.dumps(occ['active_pins'], ensure_ascii=False)}")
    for block in occ["blocks"]:
        print(f"    {block['object']:>8} {block['state']:<16} pin={block['pins']} "
              f"占空间={block['occupies_space']} verified={block['verified_local']}")
    print("\n## 清理排期")
    for item in report["cleanup_schedule"]:
        ref = item["reference"] or "(无工单)"
        status = item.get("status", item.get("state", "?"))
        print(f"- {ref} 对象 {item['object']} 状态={status} "
              f"仓储可处理={item.get('warehouse_actionable_at', item.get('evictable_at'))} "
              f"本地可驱逐={item.get('local_evictable_at', item.get('evictable_at'))} "
              f"blocked_by={item.get('blocked_by', item.get('pins', []))}")
        if item.get("note"):
            print(f"    {item['note']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--directory", type=Path, default=PROJECT_ROOT / "materials",
                        help="演练资料目录（manifest/services/requests/activity/objects）")
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "run-output",
                        help="报告与 journal 输出目录（与资料目录分离）")
    parser.add_argument("--initial-free", type=int, default=None,
                        help="恢复站初始空闲空间单位，默认取首次压力事件观测值")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="完整运行演练")
    p_run.add_argument("--journal-name", default="journal.json")
    p_run.add_argument("--fail-on-failed", action="store_true")
    p_run.set_defaults(func=cmd_run)

    p_rec = sub.add_parser("reconcile", help="从 journal 中断恢复并继续")
    p_rec.add_argument("--journal", required=True)
    p_rec.add_argument("--horizon", type=int, default=None)
    p_rec.set_defaults(func=cmd_reconcile)

    p_rep = sub.add_parser("report", help="从 journal 重新生成报告")
    p_rep.add_argument("--journal", required=True)
    p_rep.set_defaults(func=cmd_report)

    p_ins = sub.add_parser("inspect", help="只查看恢复点候选与块校验，不跑工单")
    p_ins.set_defaults(func=cmd_inspect)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except FeedError as exc:
        parser.error(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
