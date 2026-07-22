#!/usr/bin/env python3
"""Dispatch Tableau TDVT and mirror its result. Used by nightly.yml and release.yml."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

TDVT_REPO = "ClickHouse/clickhouse-tableau-tdvt"
WORKFLOW = "tableau-tdvt.yml"
POLL_ATTEMPTS = 24
POLL_INTERVAL_SEC = 15
WATCH_INTERVAL_SEC = 30
CREATED_FILTER_SKEW_SEC = 120


def fail(message: str) -> None:
    print(f"::error::{message}", file=sys.stderr)
    sys.exit(1)


def gh_json(*args: str) -> dict | list:
    result = subprocess.run(["gh", *args], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def gh_api_call(path: str, *fields: str) -> None:
    args = ["gh", "api", path]
    for field in fields:
        flag = "-F" if field.startswith("client_payload[") else "-f"
        args.extend([flag, field])
    subprocess.run(args, check=True)


def latest_run_id(repo: str) -> int:
    data = gh_json(
        "api",
        f"repos/{repo}/actions/workflows/{WORKFLOW}/runs?event=repository_dispatch&per_page=1",
    )
    runs = data.get("workflow_runs", [])
    return runs[0]["id"] if runs else 0


def list_runs(repo: str, created_since: str) -> list[dict]:
    data = gh_json(
        "api",
        f"repos/{repo}/actions/workflows/{WORKFLOW}/runs"
        f"?event=repository_dispatch&created=>={created_since}&per_page=20",
    )
    return data.get("workflow_runs", [])


def title_has_version(display_title: str, version: str) -> bool:
    # TDVT run-name is "TDVT (JDBC {version})" — require the closing ")" boundary.
    return re.search(r"\(JDBC " + re.escape(version) + r"\)", display_title) is not None


def version_matches(runs: list[dict], threshold: int, version: str) -> list[dict]:
    return [
        run
        for run in runs
        if run.get("id", 0) > threshold
        and title_has_version(run.get("display_title", ""), version)
    ]


def parse_github_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def find_run_id(
    runs: list[dict],
    threshold: int,
    version: str,
    correlation_id: str,
    *,
    final: bool = False,
    dispatch_after: datetime | None = None,
) -> int | None:
    matches = version_matches(runs, threshold, version)

    corr = [run for run in matches if correlation_id in run.get("display_title", "")]
    if len(corr) == 1:
        return corr[0]["id"]
    if len(corr) > 1:
        return None
    if len(matches) > 1:
        return None
    if len(matches) == 1:
        new_runs = [run for run in runs if run.get("id", 0) > threshold]
        if len(new_runs) > 1:
            return matches[0]["id"]
        if final and dispatch_after is not None:
            created = parse_github_time(matches[0]["created_at"])
            if created >= dispatch_after - timedelta(seconds=10):
                return matches[0]["id"]
    return None


def write_summary(version: str, run_url: str, info: dict) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    lines = [
        f"## Tableau TDVT - clickhouse-jdbc `{version}`",
        "",
        f"[Full TDVT run]({run_url})",
        "",
        f"**Conclusion: {info.get('conclusion', '')}**",
        "",
        "| Job | Result |",
        "|---|---|",
    ]
    for job in info.get("jobs", []):
        result = job.get("conclusion") or job.get("status") or ""
        lines.append(f"| {job.get('name', '')} | {result} |")
    with open(path, "a", encoding="utf-8") as summary:
        summary.write("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-type", required=True, choices=("jdbc-snapshot", "jdbc-release"))
    parser.add_argument("--version", required=True)
    parser.add_argument("--correlation-id", required=True)
    parser.add_argument("--tdvt-repo", default=TDVT_REPO)
    args = parser.parse_args()

    if not os.environ.get("GH_TOKEN"):
        fail("GH_TOKEN is required")

    print(
        f"Dispatching TDVT for clickhouse-jdbc {args.version} "
        f"(correlation: {args.correlation_id})"
    )

    prev_id = latest_run_id(args.tdvt_repo)
    created_since = (
        datetime.now(timezone.utc) - timedelta(seconds=CREATED_FILTER_SKEW_SEC)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    dispatch_after = datetime.now(timezone.utc)
    gh_api_call(
        f"repos/{args.tdvt_repo}/dispatches",
        f"event_type={args.event_type}",
        f"client_payload[jdbc_version]={args.version}",
        f"client_payload[correlation_id]={args.correlation_id}",
    )

    run_id = None
    for _ in range(POLL_ATTEMPTS):
        run_id = find_run_id(
            list_runs(args.tdvt_repo, created_since),
            prev_id,
            args.version,
            args.correlation_id,
        )
        if run_id is not None:
            break
        time.sleep(POLL_INTERVAL_SEC)

    if run_id is None:
        runs = list_runs(args.tdvt_repo, created_since)
        run_id = find_run_id(
            runs, prev_id, args.version, args.correlation_id,
            final=True, dispatch_after=dispatch_after,
        )
        if run_id is None:
            if len(version_matches(runs, prev_id, args.version)) > 1:
                fail(
                    f"Multiple TDVT runs match JDBC {args.version} after dispatch; "
                    "cannot pick the correct run safely"
                )
            fail("TDVT run did not appear after dispatch")

    run_url = f"https://github.com/{args.tdvt_repo}/actions/runs/{run_id}"
    print(f"TDVT run: {run_url}")

    subprocess.run(
        ["gh", "run", "watch", str(run_id), "--repo", args.tdvt_repo, "--interval", str(WATCH_INTERVAL_SEC)],
        check=False,
    )
    info = gh_json("run", "view", str(run_id), "--repo", args.tdvt_repo, "--json", "conclusion,jobs")
    conclusion = info.get("conclusion") or ""
    print(f"TDVT concluded: {conclusion}")
    write_summary(args.version, run_url, info)

    if conclusion != "success":
        fail(f"Tableau TDVT did not pass (conclusion: {conclusion}) - {run_url}")


if __name__ == "__main__":
    main()
