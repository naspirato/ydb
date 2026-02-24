#!/usr/bin/env python3
"""
Debug script: fetch issue/PR timeline and print raw JSON.
Usage: GITHUB_TOKEN=... python3 debug_timeline.py [PR_OR_ISSUE_NUMBER]
Example: GITHUB_TOKEN=xxx python3 .github/scripts/automation/debug_timeline.py 34745
"""
import json
import os
import sys
import urllib.request
import urllib.error

OWNER = "ydb-platform"
REPO = "ydb"

def main():
    number = int(sys.argv[1]) if len(sys.argv) > 1 else 34745
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("Set GITHUB_TOKEN", file=sys.stderr)
        sys.exit(1)

    url = f"https://api.github.com/repos/{OWNER}/{REPO}/issues/{number}/timeline?per_page=100"
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.fp.read().decode("utf-8", errors="replace") if e.fp else ""
        print(f"HTTP {e.code}: {body[:500]}", file=sys.stderr)
        sys.exit(1)

    print(f"# Timeline for {OWNER}/{REPO} #{number} ({len(data)} events)\n")
    for i, evt in enumerate(data):
        ev_type = evt.get("event") or evt.get("event_type") or type(evt)
        print(f"--- Event {i+1}: {ev_type} ---")
        print(json.dumps(evt, indent=2, default=str))
        print()

if __name__ == "__main__":
    main()