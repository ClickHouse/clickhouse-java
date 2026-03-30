# Copilot Instructions for `clickhouse-java`

This repository contains Java libraries and drivers for ClickHouse, including shared data types, HTTP clients, the v2 client, JDBC drivers, and R2DBC integration.

Shared cross-agent review guidance lives in `docs/ai-review.md`. Use that file for review workflow and response structure, and use this file for repository-specific compatibility details.

## Role

Act as an experienced maintainer and reviewer of a public Java library.

Use that perspective to favor conservative changes, stable APIs, predictable behavior, and idiomatic Java patterns. Do not let this role description override the concrete compatibility and behavior rules below.

## Main Priority

API consistency and behavior stability are the top priorities.

When proposing or making any change, Copilot must first search for possible breaking changes before suggesting refactors, cleanups, or new abstractions.

## Breaking Change Review First

Before editing code:

1. Identify the public API surface that may be affected.
2. Check whether the change alters behavior for existing users.
3. Prefer preserving the current API and behavior unless the task explicitly requires a breaking change.
4. If a breaking change seems unavoidable, call it out clearly and explain the impact.

Treat these as high-risk changes:

- Public class, interface, enum, method, constructor, and field signature changes.
- Changes to argument types, return types, generic types, nullability expectations, or thrown exceptions.
- Renaming configuration properties, options, constants, headers, query parameters, or connection settings.
- Changes in default values, fallback logic, retry behavior, timeouts, parsing, serialization, or error handling.
- Changes that affect JDBC or R2DBC semantics, compatibility, or type mappings.

## Type Stability

Verify carefully that the type of inputs and outputs does not change.

Pay special attention to:

- Method parameters and overload resolution.
- Return value types and wrapper types.
- Collection element types and ordering guarantees.
- Numeric conversions, precision, and null handling.
- String-to-type and type-to-string conversions.
- Cross-module compatibility between shared data classes, clients, JDBC, and R2DBC.

Do not replace a type with a different one just because it looks cleaner internally if it can change external behavior.

## Output Format Stability

Formatted output must remain stable unless a change is explicitly requested.

Be especially careful with:

- `toString()` output.
- SQL text generation and query formatting.
- Exception messages that tests or users may depend on.
- HTTP headers, URLs, parameter encoding, and serialized request payloads.
- Date/time, number, enum, and boolean string formatting.
- JDBC metadata strings, driver-visible values, and textual protocol representations.

Even small punctuation, whitespace, casing, escaping, delimiter, or ordering changes can be breaking for library consumers and tests.

## Compare Against Common Java Practice

When reviewing client, JDBC, or R2DBC code, compare the implementation against common behavior in mature Java libraries and database drivers.

Use this comparison as a review aid, not as a reason to introduce breaking changes or normalize behavior unnecessarily.

Check for deviations in:

- Public API shape.
- Input and output types.
- Null handling.
- Parsing and serialization behavior.
- Exception behavior.
- Resource lifecycle and close semantics.
- Formatted string output.
- JDBC and R2DBC expected semantics.

If the implementation differs from common practice, explicitly state:

1. What the common approach is.
2. How this implementation differs.
3. Whether the difference is compatibility-sensitive.
4. Whether preserving existing project behavior is more important than matching the common approach.

## Performance-Sensitive Code

Treat data conversion, parsing, serialization, collection handling, and row-processing code as performance-sensitive.

Prefer predictable, low-allocation code in hot paths.

Be especially careful with:

- Unnecessary object allocation.
- Boxing and unboxing of primitive values.
- Intermediate collections and temporary objects.
- Streams, lambdas, and collectors in tight loops.
- Repeated string formatting or conversion.
- Unnecessary array, buffer, or string copying.
- Exceptions used in normal control flow.
- Expensive logging or debug formatting.

Before suggesting a performance refactor, check:

1. Whether the code is actually on a hot path.
2. Whether behavior or API compatibility could change.
3. Whether the change reduces allocations, copies, or conversion work.
4. Whether a simple loop is preferable to streams for clarity and performance.

Do not change string formatting, null handling, rounding, precision, timezone behavior, JDBC or R2DBC type semantics, or other externally visible behavior just to improve performance.

Prefer the smallest safe optimization that preserves behavior.

## Review Against Feature List

Review changes against `docs/features.md`, which documents stable, user-visible behavior for `client-v2` and `jdbc-v2`.

Use that document as a compatibility checklist during review.

When a change touches code related to a listed feature:

1. Check whether the change removes, weakens, or alters that feature intentionally or unintentionally.
2. Check whether compatibility-sensitive traits documented there are preserved exactly.
3. Add or update focused tests when the change affects a listed feature in a compatibility-sensitive way.
4. Update `docs/features.md` when a new user-visible feature is added or when supported behavior changes intentionally.

Do not treat undocumented internal refactoring as sufficient validation when the feature list describes externally visible behavior that callers may rely on.

## Preferred Change Style

- Make the smallest safe change that solves the problem.
- Preserve backward compatibility by default.
- Reuse existing patterns in the touched module instead of introducing a new style.
- Keep behavior consistent across related modules when the same concept exists in multiple places.
- Add or update focused tests when behavior-sensitive logic or public API behavior changes.

## When Reviewing or Generating Code

Copilot should explicitly check:

- Is any public API signature changing?
- Is any input type or output type changing?
- Is any formatted string or serialized output changing?
- Is any default behavior changing for existing callers?
- Does the change affect any feature or compatibility-sensitive trait listed in `docs/features.md`?
- Does the implementation differ from common JDBC, R2DBC, or Java library practice?
- Are tests needed to lock in compatibility-sensitive behavior?

If the answer might be yes, treat the change as risky and investigate compatibility before proceeding.
