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
- Follow `AI_POLICY.md`: take the same responsibility for generated code as hand-written code, don't
  submit code you don't understand, and respect licensing when reproducing any external code.

## When to pause and ask

- Before implementing a new feature, a large behavior change, or anything touching public API,
  configuration, protocol handling, serialization, or JDBC/R2DBC behavior: check whether an approved
  issue or proposal already covers it. If not, say so and ask before implementing instead of proceeding silently — see `CONTRIBUTING.md`.
- Do not modify CI workflow files (`.github/workflows/**`). CI changes are restricted; raise it with the user first.

## Setup and test commands

Prefer targeted Maven commands over full-repo runs.

- Run module tests: `mvn -pl <module> test`
- Run a module with dependencies: `mvn -pl <module> -am test`
- Examples:
- `mvn -pl client-v2 test`
- `mvn -pl jdbc-v2 test`
- `mvn -pl clickhouse-data test`

Also compile examples or packaging modules (e.g. `packages/clickhouse-jdbc-all`) when a change affects examples, packaging, public APIs, or other user-facing behavior — running the touched module's tests alone isn't enough.

Run benchmarks under `clickhouse-benchmark` when a change touches a performance-sensitive path (see
`docs/ai-review.md`); this is optional and only worth doing when performance is actually in question.

Avoid broad dependency, formatting, or unrelated cleanup churn unless required by the task.

## Editing expectations

- Identify the affected module before editing.
- Read nearby code and follow existing local patterns before introducing new abstractions.
- Prefer focused tests near the affected module over repo-wide runs.

## Scope discipline

- One logical change per commit/PR: a feature, a bug fix, a refactor, or a doc update. Don't mix.
- Implement the smallest change that solves the problem. Defer polish, extra configuration, and
  optimization to follow-ups rather than expanding scope.
- Flag it if a change is growing large: ~400 LOC starts to hurt reviewability, 800+ LOC needs to be
  split into smaller changes.

## Testing expectations

- Add negative tests when a change affects validation, error handling, parsing, serialization, or
  compatibility-sensitive behavior. The test should prove the failure path produces the correct
  result/exception, not just that the original bug no longer reproduces.
- Don't pad coverage: add a test only when it covers a distinct scenario, edge case, module, type,
  format, or failure mode. Avoid duplicating existing coverage.
- Favor scenario coverage (boundary values, invalid input, nullable/nested interactions) over raw
  coverage percentage.
- Keep tests compact, readable, and focused on the scenario being verified. Reduce duplication in test setup and assertions so a reviewer can quickly see what behavior is being tested.

## Closeout checklist

- Run the affected module's tests locally (see "Setup and test commands") before treating a change as done. Don't hand off a change with failing or unrun tests.
- Update `CHANGELOG.md` for user-facing changes: what the problem was, how it was fixed, and a link to the issue.
- Update `docs/features.md` when a `client-v2` or `jdbc-v2` feature is added, removed, or its behavior intentionally changes.
- In the final summary: state compatibility impact explicitly, describe any user-visible behavior
  change, and link the related issue (note that one should be created if it's missing).

## Review expectations

For review requests, follow `docs/ai-review.md` as the shared review standard across AI agents.

Before treating implementation work as done, self-review your own diff against `docs/ai-review.md` —
not only when a review is explicitly requested.

## Optional nested AGENTS.md

If a module develops its own conventions, add a nested `AGENTS.md` inside that module.
The nearest `AGENTS.md` to the edited files should be treated as the most specific guidance.
