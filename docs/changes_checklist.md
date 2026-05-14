Use this checklist for small, localized code changes during review.

For each change:

- run the common-sense check first
- then run the context check in the exact place where the change happened
- if the change affects public API, config keys, defaults, output format, serialization, retries, or documented features, treat it as compatibility-sensitive even if the diff is small
- when the change touches `client-v2` or `jdbc-v2`, also cross-check `docs/features.md`

# Logging added or changed

## Common Sense / Lint Check

- Log level matches the situation: `error` for real failures, `warn` for degraded but recoverable behavior, `info` for important lifecycle events, `debug` or `trace` for diagnostics.
- The log is not emitted in a hot loop, row-processing loop, retry loop, or polling loop unless it is rate-limited or guarded.
- Message text is actionable and stable enough to be useful in support cases.
- Exceptions are logged with the throwable attached, not flattened into a string.
- The log does not expose secrets, credentials, tokens, SQL values, or large payloads.
- Expensive string building is avoided when the level may be disabled.

## Context Check

- In request, parsing, and row-processing code, logging does not create noticeable allocation or throughput regressions.
- In retry paths, the level and frequency do not turn one failure into many noisy messages.
- In connection setup or shutdown code, `info` logs stay low-volume and do not fire for every normal operation.
- In library code used by applications, logs do not duplicate errors that are already propagated to callers with enough context.

# Enum constant added

## Common Sense / Lint Check

- New constant is appended, not inserted in the middle, unless order is proven irrelevant.
- Existing constant names and ordinals are not changed casually.
- Call sites that switch over the enum are checked for missing branches or fallback behavior.
- Parsing, formatting, and serialization code is checked for assumptions about a fixed set of values.

## Context Check

- If the enum is externally visible, adding the constant is treated as a compatibility-sensitive behavior change and should be backed by focused tests.
- If the enum participates in serialization, JDBC metadata, config parsing, or protocol text, verify that the new value round-trips correctly.
- If the enum is `ClientConfigProperties`, the new constant key is unique, the value type is correct, and the default value parses to the declared type.
- If the enum is used in persisted configs, wire format, or stored values, confirm that adding the constant does not break older readers.

# Enum constant changed or removed

## Common Sense / Lint Check

- Treat this as a red flag by default.
- Renaming, reordering, or removing constants is assumed risky until proven otherwise.
- All parsing, serialization, `valueOf`, switch statements, and persisted-value usage are reviewed carefully.

## Context Check

- If the enum is public or crosses module boundaries, assume a breaking change unless there is a clear migration path.
- If the enum is `ClientConfigProperties`, no property key change is allowed without an explicit compatibility decision and documentation update.
- If a default changes indirectly because a constant changed, review existing caller behavior and tests, not just the enum file.

# New method added

## Common Sense / Lint Check

- Name, parameters, and return type fit existing API patterns in the same module.
- Visibility is the smallest needed for the change.
- Nullability expectations are clear from surrounding conventions.
- The method does one thing and does not duplicate existing helper behavior unnecessarily.
- If public, it has behavior-focused tests.

## Context Check

- If the method is added to a public class or interface, review API surface growth and long-term support cost.
- If added to an interface or abstract class, check binary and source compatibility for implementors.
- If added near overload-heavy APIs, verify it does not create ambiguous calls or surprising method selection.
- If added in `client-v2` or `jdbc-v2`, verify whether it exposes a new user-visible feature that should be reflected in `docs/features.md`.

# Method signature changed

## Common Sense / Lint Check

- Treat parameter type, return type, checked exception, and visibility changes as compatibility-sensitive.
- Verify that overload resolution, autoboxing, and `null` call sites do not change behavior unexpectedly.
- Check whether fluent chaining, generics, or covariant returns change caller expectations.

## Context Check

- Public API signature changes are presumed breaking unless clearly internal.
- In JDBC and R2DBC-facing code, even small type or exception changes can alter framework integration behavior.
- In helper methods used across modules, check all downstream callers instead of relying on local compilation only.

# New configuration property added

## Common Sense / Lint Check

- Property name is clear, consistent with existing naming, and not overly specific to one caller.
- Type and default value are sensible, documented in code, and easy to reason about.
- Invalid values fail predictably.
- The change does not silently overlap with an existing property or alias.

## Context Check

- If the property is represented by `ClientConfigProperties`, the key must be unique and stable.
- The declared `valueType` must match parsing behavior and the default value must parse into that type.
- If the property affects connection, retry, timeout, compression, SQL rendering, or authentication behavior, treat it as compatibility-sensitive and add focused tests.
- If the property becomes user-visible in `client-v2` or `jdbc-v2`, consider whether `docs/features.md` should be updated.

# Existing configuration property changed

## Common Sense / Lint Check

- Treat key, default, parsing, units, and semantic meaning changes as high risk.
- Backward-compatible aliases are preferred over replacing a widely used property.
- Unit changes such as milliseconds versus seconds are called out explicitly.

## Context Check

- For `ClientConfigProperties`, no key change is allowed accidentally.
- If the default changes, review behavior for callers who never set the property explicitly.
- If the value type changes, verify parsing, stored defaults, docs, and all call sites that cast or compare values.
- If the property is passed through JDBC URLs, HTTP parameters, or headers, check the full end-to-end path.

# Default value added or changed

## Common Sense / Lint Check

- New default is safe for existing users, not just for the new code path.
- The default is consistent with current documentation, tests, and surrounding constants.
- The diff is checked for silent behavior change hidden behind a "cleanup" or refactor.

## Context Check

- In config enums and builders, verify the default is parsed once and used consistently everywhere.
- In timeout, retry, compression, and pool settings, review performance and failure-mode impact, not only correctness.
- In JDBC or client defaults, confirm that existing URLs and properties still behave as users expect.

# Exception handling changed

## Common Sense / Lint Check

- Exceptions are not swallowed unless there is a clear fallback and enough context remains for debugging.
- Exception type still matches the contract and caller expectations.
- The change does not convert a deterministic failure into a silent partial success.
- Logging and rethrow behavior does not duplicate the same failure multiple times.

## Context Check

- In public APIs, changing the exception type or message format may affect caller handling and tests.
- In retry paths, check whether the exception is still classified correctly for retry or no-retry decisions.
- In JDBC code, confirm that `SQLException` semantics and vendor error context remain appropriate.

# Retry or timeout behavior changed

## Common Sense / Lint Check

- Retries do not mask permanent failures or multiply side effects.
- Timeout changes have a clear unit and are not accidental constant drift.
- Error messages and metrics still make it clear whether a request failed, timed out, or was retried.

## Context Check

- In `client-v2`, verify the change against documented retry and connection behavior in `docs/features.md`.
- Check whether the change alters defaults, per-request overrides, or only internal transport tuning.
- Review logging volume, backoff behavior, and failure classification together, not in isolation.

# String, SQL, or serialized output changed

## Common Sense / Lint Check

- Treat any output-format change as externally visible unless proven otherwise.
- Verify escaping, quoting, whitespace, casing, delimiters, and precision carefully.
- Do not assume a formatting simplification is safe just because tests still compile.

## Context Check

- In SQL helpers, confirm compatibility with the documented escaping and quoting rules.
- In JDBC result formatting or parameter rendering, check the stable output expectations described in `docs/features.md`.
- In protocol, header, and query parameter code, verify end-to-end behavior rather than local string equality only.

# Null handling changed

## Common Sense / Lint Check

- New null checks do not hide genuine bugs or change public behavior unexpectedly.
- Removed null checks are validated against all existing call paths.
- `null`, empty string, empty collection, and zero are not conflated.

## Context Check

- In public API methods, nullability behavior is part of the contract even without annotations.
- In data conversion and result mapping code, null handling can affect JDBC, POJO binding, and serialization behavior.
- In config parsing, verify how missing value, explicit null, and defaulted value differ.

# New field added to a request, response, or settings object

## Common Sense / Lint Check

- Field name, type, and default follow the local pattern.
- Equality, hash code, copy, builder, and string representation behavior are updated if relevant.
- The field is initialized consistently in all constructors or factories.

## Context Check

- In settings objects, verify propagation from public setter to actual request execution.
- In DTOs or metadata objects, confirm that serialization, deserialization, and tests cover the new field.
- In JDBC or client settings, check whether the new field changes feature surface and needs documentation.

# Conditional logic or guard changed

## Common Sense / Lint Check

- Boundary conditions are reviewed explicitly: empty input, one item, zero, negative values, null, and already-closed state.
- The new branch does not invert success and failure paths accidentally.
- Early returns and fallback branches preserve previous cleanup and resource handling.

## Context Check

- In parser, conversion, and row-reading code, small condition changes can alter output for many inputs; add focused regression tests.
- In connection lifecycle code, verify close, reuse, and concurrent access behavior.
- In auth, SSL, and proxy logic, check that secure defaults are preserved.
