# AI Review Guide

Use this guide for code reviews across Cursor, Claude, and other AI assistants working in this repository.

## Review priorities

Review for correctness, regressions, and compatibility risk before suggesting refactors or style changes.

- Preserve backward compatibility by default.
- Treat public API and user-visible behavior changes as high risk.
- Prefer the smallest safe change over broader cleanup.

## Breaking change review first

Check these areas before recommending or approving a change:

- public classes, interfaces, enums, methods, constructors, and fields
- argument types, return types, generics, nullability expectations, and thrown exceptions
- configuration properties, headers, query parameters, connection settings, and defaults
- retry behavior, timeouts, parsing, serialization, and error handling
- JDBC and R2DBC semantics, compatibility, and type mappings

If a breaking change seems possible, call it out explicitly and explain the user impact.

## Type and output stability

Verify carefully that the change does not alter externally visible types or formatted output unless the task explicitly requires it.

Be especially careful with:

- overload resolution and method selection
- wrapper types, collection element types, and ordering guarantees
- numeric conversions, precision, null handling, and timezone behavior
- `toString()` output, SQL rendering, escaping, whitespace, casing, and delimiter choices
- exception messages, HTTP headers, URLs, parameter encoding, and serialized payloads

Do not treat an internal cleanup as safe if it can change observable behavior.

## Feature contract review

When reviewing changes in `client-v2` or `jdbc-v2`, use `docs/features.md` as a compatibility checklist.

- Check whether a documented feature is removed, weakened, or changed.
- Call out compatibility-sensitive behavior even when the code change looks small.
- Recommend focused tests when the change touches a documented feature.
- Recommend a `docs/features.md` update when user-visible behavior changes intentionally.

## Compare with common Java practice

Use common Java, JDBC, and R2DBC behavior as a review aid, not as a reason to normalize behavior unnecessarily.

If the implementation differs from common practice, state:

1. what the common approach is
2. how this repository behaves today
3. whether the difference is compatibility-sensitive
4. whether preserving existing project behavior is more important than matching the common approach

## Performance-sensitive paths

Treat data conversion, parsing, serialization, collection handling, and row-processing code as performance-sensitive.

Look for:

- unnecessary allocations or temporary objects
- boxing and unboxing in hot paths
- repeated string conversion or copying
- streams or collectors in tight loops
- exceptions used in normal control flow

Do not accept a performance improvement if it changes formatting, null handling, precision, timezone behavior, or JDBC/R2DBC semantics unless that behavior change is explicitly intended.

## Tests and docs

Call out missing focused tests when a change affects:

- public API behavior
- compatibility-sensitive parsing or formatting
- documented features
- JDBC or R2DBC semantics

Also state whether docs should change, especially when `docs/features.md` is part of the affected contract.

## Review response format

When responding to a review request:

1. List findings first, ordered by severity.
2. Focus on bugs, regressions, compatibility risk, security issues, and missing tests.
3. Use file or symbol references where helpful.
4. Keep any change summary brief and secondary.
5. If no issues are found, say so explicitly and mention residual risk or testing gaps.

In the final summary, state:

- what changed
- whether compatibility risk exists
- what tests were run, or why they were not run
- whether docs should change
