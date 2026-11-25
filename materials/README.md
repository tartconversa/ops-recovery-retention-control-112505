# 演练资料格式

数据恢复演练使用小型合成块文件。manifest 中的 digest 是备份发布时记录的原字节 SHA-256，仓储字节可能与其不一致。文档另列本地读块操作，一次读取只代表获取字节。

## `manifest` 对象仓目录及组提交标记

parent 描述该块依赖的块，role 为 group 中成员。group 与 mark 相同的成员组合可作为同批内容。retrieve_min 为从当前层取回时间，restore_min 为处理时间。digest 为发布时的 SHA-256。 块内只有 UTF-8 JSON 业务数据，base 提供整个文档的键值，delta 的 set 给出本次更新，remove 为本次删除的键。后续块叠加于 parent 指向的文档。

字段为 `object`、`service`、`group`、`mark`、`minute`、`tier`、`parent`、`retrieve_min`、`restore_min`、`retire_min`、`digest`。

## `services` 可独立启用范围与共用工作量上限

members 为需要共同启用的服务或单独服务。fetch_slots 和 restore_slots 是共用现场能力，minimum_free_space 为恢复站必须保留的空间单位。 相同的作业槽与空间限额适用于整个恢复站，同一时刻不能按不同 unit 重复分配。一个对象需要一个空间单位。

字段为 `unit`、`members`、`fetch_slots`、`restore_slots`、`minimum_free_space`。

## `requests` 新申请、取消和清理工单

earliest_point 是最旧可接受备份的演练时间。deadline 为最迟交付分钟。cleanup 以已到保留期的 object 为目标。cancel 引用原恢复编号。

字段为 `at`、`kind`、`reference`、`members`、`earliest_point`、`deadline`、`object`。

## `activity` 冷层和清理端提供的回执

at 为收到回执的分钟。reference 为请求端给予外部操作的关联标识。kind 有 fetch_ready、delete_claimed、deleted 和 storage_pressure。

字段为 `at`、`kind`、`reference`、`object`、`free_units`。
