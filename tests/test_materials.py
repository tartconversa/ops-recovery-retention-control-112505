import unittest

from materials import list_feeds, read_feed

FIELDS = {
    'manifest': ('object', 'service', 'group', 'mark', 'minute', 'tier', 'parent', 'retrieve_min', 'restore_min', 'retire_min', 'digest'),
    'services': ('unit', 'members', 'fetch_slots', 'restore_slots', 'minimum_free_space'),
    'requests': ('at', 'kind', 'reference', 'members', 'earliest_point', 'deadline', 'object'),
    'activity': ('at', 'kind', 'reference', 'object', 'free_units'),
}


class MaterialTest(unittest.TestCase):
    def test_feed_fields(self):
        self.assertEqual(list_feeds(), sorted(FIELDS))
        for name, fields in FIELDS.items():
            with self.subTest(feed=name):
                records = list(read_feed(name))
                self.assertTrue(records)
                self.assertTrue(all(set(row) == set(fields) for row in records))
