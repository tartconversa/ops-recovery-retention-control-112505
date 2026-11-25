# 备份分层下的并行恢复与保留协调

数据恢复演练使用小型合成块文件。manifest 中的 digest 是备份发布时记录的原字节 SHA-256，仓储字节可能与其不一致。文档另列本地读块操作，一次读取只代表获取字节。

## 在本地查看资料

运行环境使用 Python 3.11.2 和标准库。启动资料浏览器无需安装额外包。

```sh
python materials.py
python materials.py manifest
python -m unittest discover -s tests -v
```

资料浏览器也接受下方的其他资料名。JSONL 使用 UTF-8 编码，每行是一个对象。表内的顺序对应来源记录，事件时间与接收时间的区别以资料说明为准。各入口可以在无外网的环境中运行。

资料字段和业务口径见[演练资料格式](materials/README.md)。

## 合成对象仓

使用 `python archive.py base-c` 查看一个对象的原字节。对象文件位于 `materials/objects/`，可对照备份仓目录查看编号。对象的校验值是对原字节计算的 SHA-256，读取成功不保证字节完整。
