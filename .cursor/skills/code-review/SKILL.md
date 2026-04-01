---
name: code-review
description: Review changes in clickhouse-java for correctness, compatibility, API stability, and missing tests. Use when reviewing pull requests, commits, diffs, patches, or when the user asks for a code review.
---
# Code Review

## Required context

Before reviewing, read:

- `AGENTS.md`
- `docs/ai-review.md`

If the review touches `client-v2` or `jdbc-v2`, also read:

- `docs/features.md`

## Review workflow

1. Identify the changed module and any compatibility-sensitive surfaces.
2. Check correctness, regressions, API and behavior stability, JDBC or R2DBC semantics, and missing focused tests before style suggestions.
3. Use common Java and driver behavior as a comparison aid, but preserve existing project behavior unless the change is intentionally user-visible.
4. Report findings first, ordered by severity, then open questions or assumptions, then a brief summary.

## Final response requirements

State:

- what changed
- whether compatibility risk exists
- what tests were run, or why they were not run
- whether docs should change

If no issues are found, say so explicitly and mention residual risk or testing gaps.
