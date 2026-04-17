#!/usr/bin/env python3
import datetime
import os
import sys
import unittest


sys.path.append(os.path.join(os.path.dirname(__file__)))
import mute_quarantine as mq  # noqa: E402


class _StubYdbWrapper:
    def __init__(self, table_path="test_results/analytics/issues", rows=None, path_error=None, query_error=None):
        self._table_path = table_path
        self._rows = rows or []
        self._path_error = path_error
        self._query_error = query_error

    def get_table_path(self, table_name):
        if self._path_error:
            raise self._path_error
        return self._table_path

    def execute_scan_query(self, query, query_name=None):
        _ = query
        _ = query_name
        if self._query_error:
            raise self._query_error
        return self._rows


class TestMuteQuarantine(unittest.TestCase):
    def setUp(self):
        self.thresholds = {"quarantine_user_fixed_window_days": 7}
        self.base_now = datetime.datetime(2026, 4, 21, 12, 0, 0, tzinfo=datetime.timezone.utc)
        self.all_data = [
            {"full_name": "ydb/tests/suite/test_a", "suite_folder": "ydb/tests/suite", "test_name": "test_a"},
            {"full_name": "ydb/tests/suite/test_b", "suite_folder": "ydb/tests/suite", "test_name": "test_b"},
            {"full_name": "ydb/tests/suite/test_c", "suite_folder": "ydb/tests/suite", "test_name": "test_c"},
        ]
        self.aggregated_for_unmute = [
            {"full_name": "ydb/tests/suite/test_b", "pass_count": 10, "fail_count": 0, "mute_count": 0}
        ]

    def test_classifies_hide_restore_stable_by_quarantine_age(self):
        rows = [
            {
                "body": (
                    "Mute:<!--mute_list_start-->\n"
                    "ydb/tests/suite/test_a\n"
                    "<!--mute_list_end-->\n\n"
                    "Branch:<!--branch_list_start-->\nmain\n<!--branch_list_end-->\n\n"
                    "Build type:<!--build_type_list_start-->\nrelwithdebinfo\n<!--build_type_list_end-->\n"
                ),
                "closed_by_type": "User",
                "closed_at": self.base_now - datetime.timedelta(days=2),
            },
            {
                "body": (
                    "Mute:<!--mute_list_start-->\n"
                    "ydb/tests/suite/test_b\n"
                    "<!--mute_list_end-->\n\n"
                    "Branch:<!--branch_list_start-->\nmain\n<!--branch_list_end-->\n\n"
                    "Build type:<!--build_type_list_start-->\nrelwithdebinfo\n<!--build_type_list_end-->\n"
                ),
                "closed_by_type": "User",
                "closed_at": self.base_now - datetime.timedelta(days=9),
            },
            {
                "body": (
                    "Mute:<!--mute_list_start-->\n"
                    "ydb/tests/suite/test_c\n"
                    "<!--mute_list_end-->\n\n"
                    "Branch:<!--branch_list_start-->\nmain\n<!--branch_list_end-->\n\n"
                    "Build type:<!--build_type_list_start-->\nrelwithdebinfo\n<!--build_type_list_end-->\n"
                ),
                "closed_by_type": "User",
                "closed_at": self.base_now - datetime.timedelta(days=9),
            },
        ]
        wrapper = _StubYdbWrapper(rows=rows)

        actions = mq.resolve_user_fixed_quarantine_actions(
            ydb_wrapper=wrapper,
            branch="main",
            build_type="relwithdebinfo",
            all_data=self.all_data,
            aggregated_for_unmute=self.aggregated_for_unmute,
            thresholds=self.thresholds,
            now_utc=self.base_now,
        )

        self.assertEqual(actions["hide"], {"ydb/tests/suite test_a"})
        self.assertEqual(actions["stable"], {"ydb/tests/suite test_b"})
        self.assertEqual(actions["restore"], {"ydb/tests/suite test_c"})
        self.assertTrue(any("active" in line for line in actions["hide_debug"]))
        self.assertTrue(any("passed" in line for line in actions["stable_debug"]))
        self.assertTrue(any("expired" in line for line in actions["restore_debug"]))

    def test_returns_empty_actions_when_table_path_missing(self):
        wrapper = _StubYdbWrapper(path_error=KeyError("issues table missing"))
        actions = mq.resolve_user_fixed_quarantine_actions(
            ydb_wrapper=wrapper,
            branch="main",
            build_type="relwithdebinfo",
            all_data=self.all_data,
            aggregated_for_unmute=self.aggregated_for_unmute,
            thresholds=self.thresholds,
        )
        self.assertEqual(actions["hide"], set())
        self.assertEqual(actions["restore"], set())
        self.assertEqual(actions["stable"], set())

    def test_returns_empty_actions_on_query_error(self):
        wrapper = _StubYdbWrapper(query_error=RuntimeError("query failed"))
        actions = mq.resolve_user_fixed_quarantine_actions(
            ydb_wrapper=wrapper,
            branch="main",
            build_type="relwithdebinfo",
            all_data=self.all_data,
            aggregated_for_unmute=self.aggregated_for_unmute,
            thresholds=self.thresholds,
        )
        self.assertEqual(actions["hide"], set())
        self.assertEqual(actions["restore"], set())
        self.assertEqual(actions["stable"], set())
        self.assertIn("error", actions.get("stats", {}))

    def test_apply_quarantine_actions_updates_sets_and_debug(self):
        to_unmute = ["a", "b", "c"]
        to_delete = ["a", "b", "d"]
        to_unmute_debug = ["a # keep", "b # keep", "c # keep"]
        to_delete_debug = ["a # keep", "b # keep", "d # keep"]
        actions = {
            "hide": {"a"},
            "restore": {"x"},
            "stable": {"b"},
            "hide_debug": ["a # hidden"],
            "restore_debug": ["x # restored"],
            "stable_debug": ["b # stable"],
        }

        updated = mq.apply_quarantine_actions(to_unmute, to_delete, to_unmute_debug, to_delete_debug, actions)

        self.assertEqual(updated["to_unmute"], ["b", "c"])
        self.assertEqual(updated["to_delete"], ["b", "d"])
        self.assertEqual(updated["to_unmute_debug"], ["b # keep", "c # keep"])
        self.assertEqual(updated["to_delete_debug"], ["b # keep", "d # keep"])
        self.assertEqual(updated["quarantine_hide_set"], {"a"})
        self.assertEqual(updated["quarantine_restore_set"], {"x"})
        self.assertIn("a", updated["quarantine_debug_map"])
        self.assertIn("x", updated["quarantine_debug_map"])

    def test_finalize_new_muted_ya_invariants(self):
        all_muted_ya = ["a", "b", "c"]
        to_delete = ["b"]
        to_unmute = ["c"]
        to_mute = ["d"]
        quarantine_hide_set = {"a"}
        quarantine_restore_set = {"x"}

        final_tests = mq.build_final_muted_ya_list(
            base_muted_ya=all_muted_ya,
            to_delete=set(to_delete),
            to_unmute=set(to_unmute),
            to_mute=to_mute,
            quarantine_hide=quarantine_hide_set,
            quarantine_restore=quarantine_restore_set,
        )

        self.assertNotIn("a", final_tests)
        self.assertIn("x", final_tests)
        self.assertIn("d", final_tests)


if __name__ == "__main__":
    unittest.main()
