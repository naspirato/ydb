# Fast unmute override design (draft)

## Context

Current default policy is stable and should stay unchanged:

- unmute window: **7 days**
- no failures in window
- issue can be closed when tests are unmuted

This covers real cases where tests recover because of infra, dependency, or environment changes, without requiring a fix PR.

At the same time, teams may want an **optional fast path** for selected cases.

## Goals

1. Keep default behavior unchanged (7-day unmute).
2. Add an optional 1-day unmute window for explicitly approved cases.
3. Apply fast window only to tests that were still "in scope" for an issue at close time.
4. Avoid fragile reopen logic across multiple historical issues.

## Non-goals

- Replacing the default 7-day policy.
- Requiring fix PR link for every close/unmute.
- Mandatory reopen of old closed issues.

## Signal for fast path

Use an explicit, machine-readable opt-in signal on issue:

- label: `fast-unmute-1d`

Optional guard (recommended): issue is closed by non-bot actor.

Reason:

- explicit intent from humans
- no hidden heuristics
- easy to audit

## Why not "manual muted_ya edit" as primary signal

Manual `muted_ya` change is too weak semantically:

- may represent temporary operational actions
- does not necessarily mean test is fixed

It may be used as fallback in future, but not as primary fast-unmute trigger.

## Core rule

For each issue:

- `T_all`: tests from issue body (`<!--mute_list_start--> ... <!--mute_list_end-->`)
- `T_prev_unmuted`: tests already announced as unmuted in prior comments
- `T_remaining_before_close = T_all - T_prev_unmuted`

Fast window can be applied only to `T_remaining_before_close`.

This prevents retroactive impact on tests that were unmuted earlier in partial updates.

## Reliable storage for "already unmuted in this issue"

Do not parse free-form text. Add markers in unmute comments:

```text
Some tests have been unmuted:
<!--unmute_list_start-->
- Test ydb/path/test_a
- Test ydb/path/test_b
<!--unmute_list_end-->
```

The parser should read only marked blocks.

## Persistence for overrides

Introduce table: `test_results/analytics/unmute_overrides`

Primary key:

- `(full_name, branch, build_type)`

Columns:

- `full_name` Utf8 NOT NULL
- `branch` Utf8 NOT NULL
- `build_type` Utf8 NOT NULL
- `window_days` Uint32 NOT NULL (for now: 1)
- `reason` Utf8 NOT NULL (example: `issue_fast_unmute_1d`)
- `issue_number` Uint64
- `activated_at` Timestamp NOT NULL
- `expires_at` Timestamp
- `active` Uint8 NOT NULL
- `consumed_at` Timestamp

Notes:

- store only active rows in hot path queries
- set `consumed_at` when test is actually unmuted

## Runtime behavior

1. Default unmute logic still uses 7-day window.
2. For tests with active override:
   - evaluate unmute condition on 1-day aggregate
   - keep existing thresholds (runs and failures) unless separately tuned
3. If test gets unmuted:
   - mark override as `active=0`
   - set `consumed_at`
4. If override expires before unmute:
   - fallback to default 7-day path automatically

## Integration points

1. `.github/scripts/tests/update_mute_issues.py`
   - include unmute list markers in comments
   - extract `T_prev_unmuted` from markers
2. `.github/scripts/tests/create_new_muted_ya.py`
   - load active overrides
   - aggregate 1-day and 7-day windows
   - apply per-test window selection
3. `.github/scripts/analytics/export_issues_to_ydb.py` (optional guard)
   - export closed actor login to allow non-bot checks
4. New helper module:
   - read/write `unmute_overrides` table

## Rollout

Phase 1 (safe):

- add comment markers + parser
- add override table and helper code
- no automatic override creation yet

Phase 2:

- create override rows only for closed issues with `fast-unmute-1d`
- optional non-bot guard enabled

Phase 3:

- add dashboard card: active overrides and outcomes
- tune thresholds if needed (for example, minimum runs for 1-day window)

## Expected impact

Pros:

- default stable process unchanged
- fast path is explicit and auditable
- no fragile reopen dependency

Risks:

- 1-day window may still be optimistic for highly flaky tests
- requires small amount of new state management

Mitigation:

- keep fast path opt-in only
- add override expiration and consumption semantics
