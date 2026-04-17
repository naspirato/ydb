# Mute / Unmute automation runbook

This document describes the **current** behavior of mute automation in this repository.

## Decision rules

Rule thresholds are loaded from:

- `.github/config/mute_coordinator_thresholds.json`
- fallback defaults in `.github/scripts/mute_policy_rules.py`

### Mute candidate

A test is added to `to_mute` when it is not currently muted and satisfies default mute predicate.

Default threshold values:

- window: `4` days
- if `pass + fail > 10`, require at least `3` fails
- otherwise require at least `2` fails

### Unmute candidate

A test is added to `to_unmute` when default unmute predicate passes.

Default threshold values:

- window: `7` days
- minimum runs `(pass + fail + mute) >= 4`
- failures `(fail + mute) == 0`

### Delete-from-mute candidate (`to_delete`)

A muted test is added to `to_delete` when:

- it has no runs in the delete window, or
- it was muted and only skipped in the window.

Default delete window: `7` days.

## User-fixed quarantine

Quarantine logic lives in `.github/scripts/tests/mute_quarantine.py`.

Signal source: recently closed issues from YDB `issues` table where:

- `closed_by_type == "User"`
- closure reason is `COMPLETED` (non-completed user closures are rejected)
- issue body maps to current branch/build profile

Actions:

- `hide`: test is hidden from generated muted output while quarantine window is active
- `stable`: quarantine ended and unmute rule is still satisfied
- `restore`: quarantine ended, unmute rule is not satisfied -> return to muted output

Fail-safe behavior:

- if issues table path/query fails, quarantine returns empty actions with explicit error code in stats

## Workflows

### 1) Update muted file

Workflow: `.github/workflows/update_muted_ya.yml`

- schedule: hourly, from `04:00` to `21:00` UTC
- also supports `workflow_dispatch`
- generates `mute_update/new_muted_ya.txt`
- if changed vs base branch file, opens/updates PR with `.github/config/muted_ya.txt`

### 2) Create issues for merged mute PR

Workflow: `.github/workflows/create_issues_for_muted_tests.yml`

- triggers when PR to `main` is merged
- runs only for PRs with label `mute-unmute`
- also supports `workflow_dispatch`
- creates missing mute issues, comments PR, refreshes analytics tables

## Generated files (`mute_update/`)

### Core action files

| File | Meaning |
|---|---|
| `to_mute.txt` | Candidates to add into mute |
| `to_unmute.txt` | Candidates to unmute |
| `to_delete.txt` | Candidates to remove from mute because they disappeared / only skipped |
| `muted_ya.txt` | Current muted set reconstructed from monitor rows |
| `new_muted_ya.txt` | Final output used to update `.github/config/muted_ya.txt` |

### Intermediate snapshots

| File | Meaning |
|---|---|
| `muted_ya+to_mute.txt` | current muted + new mute candidates |
| `muted_ya-to_unmute.txt` | current muted minus unmute candidates |
| `muted_ya-to_delete.txt` | current muted minus delete candidates |
| `muted_ya-to-delete-to-unmute.txt` | current muted minus delete and unmute |
| `muted_ya-to-delete-to-unmute+to_mute.txt` | pre-final merged set before copying to `new_muted_ya.txt` |
| `muted_ya_changes.txt` | combined diff-like view (`+++`, `---`, `xxx`) |

### Quarantine snapshots

| File | Meaning |
|---|---|
| `quarantine_hidden.txt` | hidden during quarantine |
| `quarantine_restored.txt` | restored after failed quarantine |
| `quarantine_stable_unmuted.txt` | stayed unmuted after quarantine |

For most `.txt` files, a corresponding `*_debug.txt` file is generated with decision context.

## Dashboards

- Main test analytics dashboard: https://datalens.yandex/4un3zdm0zcnyr
