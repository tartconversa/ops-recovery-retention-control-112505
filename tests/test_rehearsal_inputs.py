import hashlib
import json
import unittest

from archive import read_object
from materials import read_feed


class InputsTest(unittest.TestCase):
    def test_archive_bytes_and_actual_dependencies(self):
        rows = {r["object"]: r for r in read_feed("manifest")}
        for key, row in rows.items():
            with self.subTest(object=key):
                data = read_object(key)
                item = json.loads(data)
                digest = hashlib.sha256(data).hexdigest()
                self.assertEqual(len(row["digest"]), 64)
                if key == "cfg2":
                    self.assertNotEqual(digest, row["digest"])
                else:
                    self.assertEqual(digest, row["digest"])
                if row["parent"]:
                    self.assertEqual(rows[row["parent"]]["service"], row["service"])
                    self.assertLess(rows[row["parent"]]["minute"], row["minute"])
                    self.assertEqual(set(item), {"set", "remove"})
                else:
                    self.assertEqual(set(item), {"base"})
        self.assertEqual(rows["cfg3"]["parent"], "cfg2")
        self.assertEqual(rows["idx3"]["mark"], rows["cfg3"]["mark"])

