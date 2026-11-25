"""备份分层下的并行恢复与保留协调控制程序。

模块划分：
- feeds：读取并校验演练资料（manifest/services/requests/activity 与合成对象字节）
- topology：块链、组提交标记、依赖闭包与文档重建
- planning：恢复点候选、可行性判断、备选与时间损失
- store：块生命周期状态机（冷层召回、本地校验、删除回执、占用引用）
- scheduler：作业槽、空间约束与 ETA 推演
- controller：事件驱动主循环、承诺保护、重判与中断恢复
- report：演练结束后的查询视图
"""

__all__ = [
    "feeds",
    "topology",
    "planning",
    "store",
    "scheduler",
    "controller",
    "report",
]
