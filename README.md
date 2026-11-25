# 备份分层下的并行恢复与保留协调

数据恢复演练使用小型合成块文件。manifest 中的 digest 是备份发布时记录的原字节 SHA-256，仓储字节可能与其不一致（本批资料里 cfg2 即损坏块）。文档另列本地读块操作，一次读取只代表获取字节。

## 在本地查看资料

运行环境使用 Python 3.11.2 和标准库，无需安装额外包，可在隔离模式下运行：

```sh
python materials.py
python materials.py manifest
python -m unittest discover -s tests -v
# 隔离模式（忽略 PYTHONPATH/用户站点，脚本自行处理导入路径）
python -I drill.py inspect
python -I -m unittest discover -s tests -v
```

资料浏览器也接受其他资料名。JSONL 使用 UTF-8 编码，每行是一个对象。资料字段和业务口径见[演练资料格式](materials/README.md)。

## 合成对象仓

使用 `python archive.py base-c` 查看一个对象的原字节。对象文件位于 `materials/objects/`。对象的校验值是对原字节计算的 SHA-256，读取成功不保证字节完整。

## 恢复与清理控制程序（drill.py）

`drill.py` 与 `drillctl/` 包把恢复申请、冷层召回、校验、还原与分层清理交给一个
本地可跑通的事件驱动控制器。资料目录与输出目录都通过参数传入，更换备份目录或
演练记录时不需要改代码：

```sh
# 查看恢复点候选、组提交标记与实际摘要校验，不跑工单
python -I drill.py inspect

# 完整跑一遍演练，报告与 journal 写到 run-output/
python -I drill.py run
python -I drill.py --directory /path/to/other-materials --output /path/to/records run

# 中断后先对账（冷层可能已召回、删除回执可能已发出），再继续到全部申请终态
python -I drill.py reconcile --journal run-output/journal.json

# 只从 journal 重新生成查询报告
python -I drill.py report --journal run-output/journal.json
```

### 控制器遵守的业务口径

- **一致恢复点**：`group`+`mark` 相同的成员必须整组在同一提交标记上共同启用；
  独立服务各自取不晚于业务时间的最新块。恢复点按业务时间从新到旧枚举，依赖闭包
  沿 parent 链闭合。
- **在仓状态决定可行性**：冷/热层未召回、已核验本地副本、已被领取/删除的仓储对象
  区分处理。较新记录缺完整可用依赖时，给出备选恢复点与时间损失；到申请的最旧可接受
  时间仍无法满足时，留下失败依据和备选供申请方重新决定。
- **冷层召回是外部作业**：只能由 `fetch_ready` 回执完成，`retrieve_min` 仅用于 ETA
  与资料窗口外的 settle 兜底；热层召回与本地还原由共享作业槽倒计时完成。
- **字节复用、判断独立**：一个块的已核验字节可服务多个申请，还原全恢复站只做一次；
  但每个申请仍独立判断自己的组数据能否启用。
- **校验失败整组重判**：实际 SHA-256 不符时不允许只替换损坏服务，受影响申请回到规划
  器重选恢复点，回退块与时间损失全部记录。
- **承诺保护与删除协调**：已接纳恢复 pin 住尚未读完的实际依赖；清理端已领取且不可
  撤回的对象不能再承诺给新恢复，但已核验本地副本仍可完成既有恢复。空间低于保留线时
  只驱逐无 pin 的本地块，受保护块不动。
- **中断可恢复**：journal 持久化块状态、pin、作业表、请求与清理状态；重启先用 journal
  和 activity 回执对账已发生的外部效果，再继续推进。多个中断点的续跑终态与一次性运行
  一致（由测试保证）。

### 输出文件

| 文件 | 内容 |
| --- | --- |
| `report.json` | 四类查询视图：恢复交付、回退原因、资源占用、清理排期 |
| `journal.json` | 可用于 reconcile 的完整状态快照 |
| `decisions.jsonl` | 每个关键决定（接纳/回退/校验/删除/交付/延期…）逐行记录 |

报告回答演练结束时的四个问题：每项恢复交付了哪组一致内容、哪些块造成回退、还有哪些
资源在占用、过期对象何时可以继续处理。
