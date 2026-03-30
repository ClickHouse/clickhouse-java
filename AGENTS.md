# AGENTS.md

## Project overview

`clickhouse-java` is a multi-module Maven repository for ClickHouse Java clients and drivers.

Main modules:

- `clickhouse-data`: shared data types and low-level conversion utilities
- `clickhouse-client` and `clickhouse-http-client`: legacy v1 client stack
- `client-v2`: current HTTP client implementation
- `clickhouse-jdbc` and `jdbc-v2`: JDBC drivers and compatibility layers
- `clickhouse-r2dbc`: R2DBC integration
- `packages/clickhouse-jdbc-all`: packaging for the all-in-one JDBC artifact

## How to work in this repo

- Preserve backward compatibility by default.
- Treat public API changes as high risk.
- Keep formatted output stable unless a task explicitly requires a change.
- Prefer the smallest safe change over broad refactors.
- Reuse existing patterns in the touched module.

Before changing behavior, explicitly check:

- public classes, methods, constructors, constants, and configuration properties
- SQL rendering, escaping, string formatting, and serialized output
- JDBC and R2DBC semantics
- null handling, timezone behavior, numeric precision, and type conversions

## Compatibility guidance

- When touching `client-v2` or `jdbc-v2`, review `docs/features.md` as a compatibility contract.
- If a change affects a documented feature, confirm the behavior is still supported.
- Add or update focused tests for compatibility-sensitive behavior.
- Update `docs/features.md` when user-visible behavior changes intentionally.

## Setup and test commands

Prefer targeted Maven commands over full-repo runs.

- Run module tests: `mvn -pl <module> test`
- Run a module with dependencies: `mvn -pl <module> -am test`
- Examples:
- `mvn -pl client-v2 test`
- `mvn -pl jdbc-v2 test`
- `mvn -pl clickhouse-data test`

Avoid broad dependency, formatting, or unrelated cleanup churn unless required by the task.

## Editing expectations

- Identify the affected module before editing.
- Check whether the task changes public API or user-visible behavior.
- Read nearby code and follow existing local patterns before introducing new abstractions.
- Make focused changes and keep diffs narrow.
- Prefer focused tests near the affected module over repo-wide runs.

## Review expectations

For review requests, follow `docs/ai-review.md` as the shared review standard across AI agents.

In final summaries, state:

- what changed
- whether compatibility risk exists
- what tests were run, or why they were not run
- whether docs should change

## Optional nested AGENTS.md

If a module develops its own conventions, add a nested `AGENTS.md` inside that module.
The nearest `AGENTS.md` to the edited files should be treated as the most specific guidance.
