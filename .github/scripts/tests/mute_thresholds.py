#!/usr/bin/env python3

import json
import os
from functools import lru_cache


_DEFAULTS = {
    "mute_days": 4,
    "default_unmute_window_days": 7,
    "delete_days": 7,
    "manual_fast_unmute_window_days": 1,
    "manual_fast_unmute_wait_hours": 24,
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
    thresholds.update(payload or {})
    return thresholds

