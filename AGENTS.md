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

- Minimal supported Java version is 8.
- Follow `docs/ai-review.md` for compatibility, breaking-change, and review instructions.

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
- Read nearby code and follow existing local patterns before introducing new abstractions.
- Prefer focused tests near the affected module over repo-wide runs.

## Review expectations

For review requests, follow `docs/ai-review.md` as the shared review standard across AI agents.

## Optional nested AGENTS.md

If a module develops its own conventions, add a nested `AGENTS.md` inside that module.
The nearest `AGENTS.md` to the edited files should be treated as the most specific guidance.
