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
- **summary:** Short summary of the fix.
- **backport:** `no` if the fix is main-only, or list stable branches where the failure was reported / fix is needed. Branch list: `.github/config/stable_tests_branches.json` (file in `main` branch) (e.g. `stable-25-4-1`, `stable-25-4`). If backbort needed at comment ad button like this
```[![▶  Backport manual](https://img.shields.io/badge/▶%20%20Backport%20to%20stable-25-3%2Cstable-25-3-1%2Cstable-25-4%2Cstable-25-4-1%20-2196F3)](https://gh-ci-app.ydb.tech/workflow/trigger?owner=ydb-platform&repo=ydb&workflow_id=cherry_pick_v2.yml&ref=main&commits_and_prs=34746&target_branches=stable-25-3%2Cstable-25-3-1%2Cstable-25-4%2Cstable-25-4-1&allow_unmerged=true&return_url=https%3A%2F%2Fgithub.com%2Fydb-platform%2Fydb%2Fpull%2F34746&ui=true)``` (fill link with correct pull number + target_branches )
