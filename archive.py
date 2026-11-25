"""读取合成对象仓中的原始备份块。"""
import argparse
from pathlib import Path
import sys

from materials import ROOT, read_feed


def read_object(object_id, root=ROOT):
    if object_id not in {row["object"] for row in read_feed("manifest", root)}:
        raise ValueError(f"未知的对象：{object_id}")
    return (root / "objects" / (object_id + ".json")).read_bytes()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("object_id")
    args = parser.parse_args()
    try:
        sys.stdout.buffer.write(read_object(args.object_id))
    except ValueError as exc:
        parser.error(str(exc))

