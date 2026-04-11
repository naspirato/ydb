#!/usr/bin/env python3

import json
import math
import os
from functools import lru_cache


_DEFAULTS = {
    "mute_window_days": 4,
    "default_unmute_window_days": 7,
    "delete_window_days": 7,
    "manual_fast_unmute_window_days": 1,
    "control_comment_part_max_tests": 200,
}


def _thresholds_path():
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, "..", "..", "config", "mute_thresholds.json")


@lru_cache(maxsize=1)
def load_thresholds():
    path = _thresholds_path()
    try:
        with open(path, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
    except FileNotFoundError:
        payload = {}

    thresholds = dict(_DEFAULTS)
    raw = payload or {}

    # Backward-compatible aliases for older key names.
    if "mute_days" in raw and "mute_window_days" not in raw:
        raw["mute_window_days"] = raw["mute_days"]
    if "delete_days" in raw and "delete_window_days" not in raw:
        raw["delete_window_days"] = raw["delete_days"]
    if (
        "manual_fast_unmute_wait_hours" in raw
        and "manual_fast_unmute_window_days" not in raw
    ):
        wait_hours = int(raw["manual_fast_unmute_wait_hours"])
        raw["manual_fast_unmute_window_days"] = max(1, math.ceil(wait_hours / 24))

    thresholds.update(raw)

    # Normalize numeric fields.
    thresholds["mute_window_days"] = int(thresholds["mute_window_days"])
    thresholds["default_unmute_window_days"] = int(thresholds["default_unmute_window_days"])
    thresholds["delete_window_days"] = int(thresholds["delete_window_days"])
    thresholds["manual_fast_unmute_window_days"] = int(thresholds["manual_fast_unmute_window_days"])
    thresholds["control_comment_part_max_tests"] = int(thresholds["control_comment_part_max_tests"])

    # Derived value (single source of truth is manual_fast_unmute_window_days).
    thresholds["manual_fast_unmute_wait_hours"] = int(thresholds["manual_fast_unmute_window_days"]) * 24
    return thresholds


def get_thresholds():
    return load_thresholds()


def get_mute_thresholds():
    return load_thresholds()

