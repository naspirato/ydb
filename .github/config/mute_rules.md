# Mute / Unmute rules (short guide)

This is a short human-readable guide that explains when tests are muted or unmuted.

---

## MUTE when (`to_mute`)

Look at the last **4 days**:

- if runs (`pass + fail`) are **more than 10** -> need at least **3 fails**
- if runs are **10 or less** -> need at least **2 fails**

Examples:
- 15 runs, 3 fails -> goes to mute
- 6 runs, 2 fails -> goes to mute

---

## UNMUTE when (`to_unmute`)

Look at the last **7 days**:

- runs (`pass + fail + mute`) are at least **4**
- failures (`fail + mute`) are exactly **0**

Example:
- 5 runs, all successful -> goes to unmute

---

## REMOVE FROM MUTE when (`to_delete`)

Look at the last **7 days**:

- either there were no runs at all
- or the test was muted and only had `skip` events

---

## User-fixed quarantine

If a test issue was closed by a **User** with `state_reason == COMPLETED`, the test can enter quarantine:

- `hide` - temporarily hide from final muted output
- `stable` - quarantine ended and unmute conditions still pass
- `restore` - quarantine ended but unmute conditions do not pass, return to muted

Important:
- `NOT_PLANNED`, `DUPLICATE`, and other non-completed closure reasons are **not** treated as user-fixed
- if issues data is unavailable, quarantine is safely disabled for that run

---

## What is updated automatically

### Mute update workflow
`.github/workflows/update_muted_ya.yml`

- runs on schedule (hourly from 04:00 to 21:00 UTC) and manually (`workflow_dispatch`)
- calculates candidates and builds `mute_update/new_muted_ya.txt`
- opens/updates PR with `.github/config/muted_ya.txt` if changes exist

### Issue sync workflow
`.github/workflows/create_issues_for_muted_tests.yml`

- runs after merge to `main` (for PRs with `mute-unmute` label) or manually
- creates/updates mute issues and refreshes analytics tables

---

## Main output files (`mute_update/`)

- `to_mute.txt` - tests to add to mute
- `to_unmute.txt` - tests to unmute as stable
- `to_delete.txt` - tests to remove from mute as stale
- `new_muted_ya.txt` - final file used to update `.github/config/muted_ya.txt`
- `muted_ya_changes.txt` - change list with prefixes:
  - `+++` added to mute
  - `---` removed from mute
  - `xxx` removed as stale

Quarantine snapshots:
- `quarantine_hidden.txt`
- `quarantine_restored.txt`
- `quarantine_stable_unmuted.txt`

Most files also have `*_debug.txt` with explanation details.

---

## Where to change thresholds

- `.github/config/mute_coordinator_thresholds.json` - primary thresholds
- `.github/scripts/mute_policy_rules.py` - fallback values and predicates

---

## Dashboard

- https://datalens.yandex/4un3zdm0zcnyr
