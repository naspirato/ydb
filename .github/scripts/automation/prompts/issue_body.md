## Instructions for Copilot

After investigating the root cause, please:

1. **Add exactly one of these labels** (depending on where the root cause is):
   - **`copilot-test-issue`** — root cause in test code / test env / flakiness
   - **`copilot-ydb-issue`** — root cause in product (YDB core, storage, etc.)

2. **Set the area label** for this issue (and for the related PR(s) if you touch them):
   - Area labels are defined in **`.github/config/owner_area_mapping.json`** (owner → area).
   - Use the **suggested area** from Context below when the owner matches; otherwise use CODEOWNERS.
   - Format is always `area/<component>` (e.g. `area/schemeshard`, `area/queryprocessor`).

3. **Same suite, similar failures:** If other tests in this suite show the same or similar failure pattern (e.g. same error type, same component), investigate and fix them as well; do not limit the fix to the tests listed below.

(Checklist for labels, area, backport, summary and PR link will be requested in a comment on the linked PR after tests pass.)

---
## Context

Tests failed in **PR-check** while remaining stable in regression/nightly (blocked failures).

- **Owner (tests_monitor):** `{{OWNER}}`
- **Suite:** `{{SUITE}}`
- **Failures in window:** {{FAILURES_COUNT}}
- **Branches affected:** {{BRANCHES_AFFECTED}}
{{SUGGESTED_AREA_LINE}}

---
## Failures (by test; branch and links per occurrence)

{{FAILURES_SECTION}}
