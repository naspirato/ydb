#!/usr/bin/env python3
"""
Shared mute/unmute/quarantine policy thresholds and predicates.
"""

from __future__ import annotations

import json
import os
from typing import Dict


_CONFIG_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "config",
        "mute_coordinator_thresholds.json",
    )
)


DEFAULT_THRESHOLDS: Dict[str, int] = {
    "default_mute_window_days": 4,
    "default_unmute_window_days": 7,
    "default_delete_window_days": 7,
    "default_unmute_min_runs": 4,
    "default_mute_total_runs_branch_boundary": 10,
    "default_mute_min_fails_high_volume": 3,
    "default_mute_min_fails_low_volume": 2,
    "default_mute_min_fails_medium_volume": 2,
    "default_mute_medium_volume_total_runs_gt_exclusive": 10,
    "quarantine_user_fixed_window_days": 7,
}


def load_mute_coordinator_thresholds() -> Dict[str, int]:
    thresholds = dict(DEFAULT_THRESHOLDS)
    if not os.path.exists(_CONFIG_PATH):
        return thresholds

    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)

    for key, value in raw.items():
        thresholds[key] = int(value)
    return thresholds


def passes_default_mute(pass_count: int, fail_count: int, thresholds: Dict[str, int]) -> bool:
    total_runs = int(pass_count) + int(fail_count)
    fail_count = int(fail_count)
    boundary = int(thresholds["default_mute_total_runs_branch_boundary"])
    high_volume_fails = int(thresholds["default_mute_min_fails_high_volume"])
    low_volume_fails = int(thresholds["default_mute_min_fails_low_volume"])
    medium_volume_fails = int(thresholds["default_mute_min_fails_medium_volume"])
    medium_volume_boundary = int(thresholds["default_mute_medium_volume_total_runs_gt_exclusive"])

    if (fail_count >= high_volume_fails and total_runs > boundary) or (
        fail_count >= low_volume_fails and total_runs <= boundary
    ):
        return True
    if medium_volume_fails <= 0:
        return False
    return fail_count >= medium_volume_fails and total_runs > medium_volume_boundary


def passes_default_unmute(
    pass_count: int,
    fail_count: int,
    mute_count: int,
    thresholds: Dict[str, int],
) -> bool:
    min_runs = int(thresholds["default_unmute_min_runs"])
    total_runs = int(pass_count) + int(fail_count) + int(mute_count)
    total_fails = int(fail_count) + int(mute_count)
    return total_runs >= min_runs and total_fails == 0


def is_delete_candidate_counts(
    pass_count: int,
    fail_count: int,
    mute_count: int,
    skip_count: int,
    *,
    is_muted: bool,
) -> bool:
    pass_count = int(pass_count)
    fail_count = int(fail_count)
    mute_count = int(mute_count)
    skip_count = int(skip_count)
    total_runs = pass_count + fail_count + mute_count + skip_count
    only_skipped_while_muted = is_muted and skip_count > 0 and pass_count == 0 and fail_count == 0 and mute_count == 0
    return total_runs == 0 or only_skipped_while_muted
