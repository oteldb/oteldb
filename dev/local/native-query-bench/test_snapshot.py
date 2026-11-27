import unittest

from snapshot import targets_from_rows


class TargetsTest(unittest.TestCase):
    def rows(self):
        return [dict(kind="query", server="oteldb", mode="random", trial=0,
                     query_index=i, instant=False,
                     query='metric{instance="host-123",job="node_exporter"}')
                for i in range(242)]

    def test_normalizes_and_orders_targets(self):
        targets = targets_from_rows(reversed(self.rows()))
        self.assertEqual(list(range(242)), [target["query_index"] for target in targets])
        self.assertTrue(all('instance="host-0"' in target["query"] for target in targets))

    def test_rejects_partial_dashboard(self):
        with self.assertRaisesRegex(ValueError, "242-target"):
            targets_from_rows(self.rows()[:-1])

    def test_rejects_unresolved_instance(self):
        rows = self.rows()
        rows[0]["query"] = 'metric{instance="$node"}'
        with self.assertRaisesRegex(ValueError, "concrete instance"):
            targets_from_rows(rows)


if __name__ == "__main__":
    unittest.main()
