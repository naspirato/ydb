#!/usr/bin/env python3
import os
import sys
import unittest


sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from mute_policy_rules import (  # noqa: E402
    DEFAULT_THRESHOLDS,
    is_delete_candidate_counts,
    passes_default_mute,
    passes_default_unmute,
)


class TestMutePolicyRules(unittest.TestCase):
    def test_passes_default_mute_low_volume(self):
        self.assertTrue(passes_default_mute(pass_count=1, fail_count=2, thresholds=DEFAULT_THRESHOLDS))

    def test_passes_default_mute_high_volume(self):
        self.assertTrue(passes_default_mute(pass_count=20, fail_count=3, thresholds=DEFAULT_THRESHOLDS))

    def test_passes_default_unmute(self):
        self.assertTrue(
            passes_default_unmute(
                pass_count=4,
                fail_count=0,
                mute_count=0,
                thresholds=DEFAULT_THRESHOLDS,
            )
        )

    def test_delete_candidate_for_skipped_muted_test(self):
        self.assertTrue(
            is_delete_candidate_counts(
                pass_count=0,
                fail_count=0,
                mute_count=0,
                skip_count=2,
                is_muted=True,
            )
        )


if __name__ == "__main__":
    unittest.main()
