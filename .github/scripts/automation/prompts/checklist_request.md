@copilot Please fill the checklist below (edit this comment or reply with the block). We will use it to set labels and mark the PR ready.

Copy the block, fill the values, and paste in your reply:

```
<!-- autodebug-checklist -->
area: area/<component>
resolution: copilot-test-issue
summary: One-line summary
backport: no
<!-- /autodebug-checklist -->
```

- **area:** Use `area/<component>` from `.github/config/owner_area_mapping.json` or CODEOWNERS (e.g. `area/topics`, `area/schemeshard`).
- **resolution:** Exactly one of `copilot-test-issue` (root cause in test/env/flakiness) or `copilot-ydb-issue` (root cause in product).
- **summary:** Short summary of the fix (optional).
- **backport:** `no` if the fix is main-only, or list stable branches where the failure was reported / fix is needed. Branch list: `.github/config/stable_tests_branches.json` (e.g. `stable-25-4-1`, `stable-25-4`). Open separate PRs for backport if needed.
