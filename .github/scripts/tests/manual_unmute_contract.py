#!/usr/bin/env python3

PENDING_FAST_UNMUTE_WAIT_STATUS = "pending_fast_unmute_wait"
READY_FOR_FAST_UNMUTE_STATUS = "ready_for_fast_unmute"
IDLE_STATUS = "idle"

_LEGACY_PENDING_STATUS = "pending_24h"

# Common schema for test_results/analytics/manual_unmute_requests.
MANUAL_UNMUTE_BASE_COLUMNS = [
    ("issue_number", "Uint64"),
    ("full_name", "Utf8"),
    ("branch", "Utf8"),
    ("build_type", "Utf8"),
    ("issue_state", "Utf8"),
    ("issue_closed_by_login", "Utf8"),
    ("issue_closed_by_type", "Utf8"),
    ("linked_pr_count", "Uint32"),
    ("requested", "Uint8"),
    ("control_state", "Utf8"),
    ("control_status", "Utf8"),
    ("reason", "Utf8"),
    ("requested_at", "Timestamp"),
    ("resolved_at", "Timestamp"),
    ("wait_hours_left", "Uint32"),
    ("effective_unmute_window_days", "Uint32"),
    ("default_unmute_window_days", "Uint32"),
    ("manual_fast_unmute_window_days", "Uint32"),
    ("manual_fast_unmute_wait_hours", "Uint32"),
    ("exported_at", "Timestamp"),
]

# Compatibility aliases used by existing marts.
MANUAL_UNMUTE_ADDITIONAL_COLUMNS = [
    ("manual_unmute_status", "Utf8"),
    ("manual_request_status", "Utf8"),
    ("manual_request_active", "Uint8"),
    ("manual_requested_at", "Timestamp"),
    ("hours_until_ready", "Uint32"),
    ("manual_wait_hours", "Uint32"),
    ("manual_wait_hours_total", "Uint32"),
    ("manual_wait_hours_left", "Uint32"),
    ("manual_request_reason", "Utf8"),
]


def normalize_manual_unmute_status(status, requested=False):
    if not status:
        return PENDING_FAST_UNMUTE_WAIT_STATUS if requested else IDLE_STATUS
    if status == _LEGACY_PENDING_STATUS:
        return PENDING_FAST_UNMUTE_WAIT_STATUS
    return status


def build_manual_unmute_row_payload(
    status,
    state,
    requested,
    requested_at,
    resolution_reason,
    wait_hours,
    wait_hours_left,
):
    normalized_status = normalize_manual_unmute_status(status, requested=requested)
    is_active_requested = int(state == "active" and bool(requested))
    wait_hours_value = int(wait_hours)
    wait_hours_left_value = int(wait_hours_left)
    return {
        "manual_unmute_status": normalized_status,
        "manual_request_status": normalized_status,
        "manual_request_active": is_active_requested,
        "manual_requested_at": requested_at,
        "hours_until_ready": wait_hours_left_value,
        "manual_wait_hours": wait_hours_value,
        "manual_wait_hours_total": wait_hours_value,
        "manual_wait_hours_left": wait_hours_left_value,
        "manual_request_reason": resolution_reason,
    }

