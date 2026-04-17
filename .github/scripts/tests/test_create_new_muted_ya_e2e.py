#!/usr/bin/env python3
import os
import sys
import tempfile
import types
import unittest
import logging
from contextlib import redirect_stdout
from io import StringIO


if "ydb" not in sys.modules:
    sys.modules["ydb"] = types.ModuleType("ydb")
if "ydb_wrapper" not in sys.modules:
    ydb_wrapper_stub = types.ModuleType("ydb_wrapper")

    class _DummyYDBWrapper:  # pragma: no cover - import-time stub only
        pass

    ydb_wrapper_stub.YDBWrapper = _DummyYDBWrapper
    sys.modules["ydb_wrapper"] = ydb_wrapper_stub

sys.path.append(os.path.join(os.path.dirname(__file__)))
import create_new_muted_ya as mute_flow  # noqa: E402

logging.getLogger().setLevel(logging.WARNING)


class _FakeMuteCheck:
    def __init__(self, muted_tests):
        self._muted_tests = set(muted_tests)

    def __call__(self, suite_name, test_name):
        return f"{suite_name} {test_name}" in self._muted_tests


def _row(
    suite_folder,
    test_name,
    full_name,
    *,
    is_muted,
    pass_count=0,
    fail_count=0,
    mute_count=0,
    skip_count=0,
    is_test_chunk=0,
):
    return {
        "suite_folder": suite_folder,
        "test_name": test_name,
        "full_name": full_name,
        "owner": "ci",
        "is_muted": is_muted,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "mute_count": mute_count,
        "skip_count": skip_count,
        "is_test_chunk": is_test_chunk,
        "period_days": 7,
        "date_window": "2026-04-17",
        "state": "MUTED" if is_muted else "FLAKY",
    }


def _read_lines(path):
    with open(path, "r", encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip()]


class TestCreateNewMutedYaE2E(unittest.TestCase):
    def test_generates_consistent_artifacts_with_quarantine_actions(self):
        suite = "ydb/tests/suite"

        test_hidden = _row(
            suite,
            "test_hidden",
            "ydb/tests/suite/test_hidden",
            is_muted=True,
            pass_count=8,
            fail_count=0,
            mute_count=0,
        )
        test_delete = _row(
            suite,
            "test_delete",
            "ydb/tests/suite/test_delete",
            is_muted=True,
            pass_count=0,
            fail_count=0,
            mute_count=0,
            skip_count=0,
        )
        test_stays_muted = _row(
            suite,
            "test_stays_muted",
            "ydb/tests/suite/test_stays_muted",
            is_muted=True,
            pass_count=1,
            fail_count=1,
            mute_count=2,
        )
        test_new_mute = _row(
            suite,
            "test_new_mute",
            "ydb/tests/suite/test_new_mute",
            is_muted=False,
            pass_count=0,
            fail_count=2,
            mute_count=0,
        )

        all_data = [test_hidden, test_delete, test_stays_muted, test_new_mute]
        aggregated_for_mute = [test_hidden, test_delete, test_stays_muted, test_new_mute]
        aggregated_for_unmute = [test_hidden, test_stays_muted]
        aggregated_for_delete = [test_hidden, test_delete, test_stays_muted]

        muted_rows = {
            f"{suite} test_hidden",
            f"{suite} test_delete",
            f"{suite} test_stays_muted",
        }
        mute_check = _FakeMuteCheck(muted_rows)

        quarantine_actions = {
            "hide": {f"{suite} test_hidden"},
            "restore": {f"{suite} test_restored"},
            "stable": set(),
            "hide_debug": [f"{suite} test_hidden # quarantine_user_fixed active"],
            "restore_debug": [f"{suite} test_restored # quarantine_user_fixed expired"],
            "stable_debug": [],
            "stats": {},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            # The production function prints progress bars to stdout.
            # Keep test output deterministic and compact.
            with redirect_stdout(StringIO()):
                muted_count = mute_flow.apply_and_add_mutes(
                    all_data=all_data,
                    output_path=tmpdir,
                    mute_check=mute_check,
                    aggregated_for_mute=aggregated_for_mute,
                    aggregated_for_unmute=aggregated_for_unmute,
                    aggregated_for_delete=aggregated_for_delete,
                    quarantine_actions=quarantine_actions,
                )
            self.assertEqual(muted_count, 1)

            update_dir = os.path.join(tmpdir, "mute_update")
            new_muted_ya = _read_lines(os.path.join(update_dir, "new_muted_ya.txt"))
            to_unmute = _read_lines(os.path.join(update_dir, "to_unmute.txt"))
            to_delete = _read_lines(os.path.join(update_dir, "to_delete.txt"))
            quarantine_hidden = _read_lines(os.path.join(update_dir, "quarantine_hidden.txt"))
            quarantine_restored = _read_lines(os.path.join(update_dir, "quarantine_restored.txt"))
            changes = _read_lines(os.path.join(update_dir, "muted_ya_changes.txt"))

            self.assertNotIn(f"{suite} test_hidden", new_muted_ya)
            self.assertNotIn(f"{suite} test_delete", new_muted_ya)
            self.assertIn(f"{suite} test_stays_muted", new_muted_ya)
            self.assertIn(f"{suite} test_new_mute", new_muted_ya)
            self.assertIn(f"{suite} test_restored", new_muted_ya)

            self.assertNotIn(f"{suite} test_hidden", to_unmute)
            self.assertIn(f"{suite} test_delete", to_delete)
            self.assertEqual(quarantine_hidden, [f"{suite} test_hidden"])
            self.assertEqual(quarantine_restored, [f"{suite} test_restored"])

            self.assertIn(f"--- {suite} test_hidden", changes)
            self.assertIn(f"xxx {suite} test_delete", changes)
            self.assertIn(f"+++ {suite} test_new_mute", changes)
            self.assertIn(f"+++ {suite} test_restored", changes)


if __name__ == "__main__":
    unittest.main()
