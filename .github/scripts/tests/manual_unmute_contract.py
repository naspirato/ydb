#!/usr/bin/env python3

PENDING_FAST_UNMUTE_WAIT_STATUS = "pending_fast_unmute_wait"
READY_FOR_FAST_UNMUTE_STATUS = "ready_for_fast_unmute"
IDLE_STATUS = "idle"

_LEGACY_PENDING_STATUS = "pending_24h"


def normalize_manual_unmute_status(status, requested=False):
    if not status:
        return PENDING_FAST_UNMUTE_WAIT_STATUS if requested else IDLE_STATUS
    if status == _LEGACY_PENDING_STATUS:
        return PENDING_FAST_UNMUTE_WAIT_STATUS
    return status

