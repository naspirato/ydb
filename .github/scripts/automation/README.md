# Automation scripts (autodebug / Copilot)

Scripts for creating and managing autodebug issues and Copilot-driven PR flow.

## Main script

- **`create_issues_pr_blocked_by_suite.py`** — Creates GitHub issues for PR-check blocked failures (from data mart), assigns Copilot, `--check-issues` (assign + ok-to-test + autodebug on linked PRs), `--add-ok-to-test`, `--assign-issue`. Depends on `../analytics/ydb_wrapper.py` for YDB.

## Helpers

- **`pr_check_status_parser.py`** — Parses PR-check status comments from github-actions; extracts report.json links (try_1..try_3) and outcome; builds markup for autodebug-stage comments.
- **`debug_timeline.py`** — Fetches issue/PR timeline (e.g. to inspect `copilot_work_finished` events).

## Data

- **`run_ex.txt`** — Example PR-check status comment (3 tries); used as reference for parser.

## Prompts

- **`prompts/`** — Folder for prompt templates (verify, closure, checklist) when posting comments to Copilot.

## Running

From repo root:

```bash
# Check-issues mode (assign Copilot, add labels to linked PRs)
python3 .github/scripts/automation/create_issues_pr_blocked_by_suite.py --check-issues --dry-run

# Create issues from mart (requires YDB credentials)
python3 .github/scripts/automation/create_issues_pr_blocked_by_suite.py --lookback_days 1 --dry-run --execute
```

Requires `GITHUB_TOKEN` for GitHub API; `CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` for mart-based issue creation.
