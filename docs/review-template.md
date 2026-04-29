# Review Template

Use this template when writing a review summary for a pull request or patch.

## Findings

List findings first and order them by severity.

### High

- None.

### Medium

- None.

### Low

- None.

## Human Review Instructions for Important Changes

Ask a human reviewer to inspect important changes directly when the diff touches any of the following:

- public API surface such as public classes, interfaces, methods, constructors, fields, or enums
- behavioral compatibility such as defaults, retries, timeouts, parsing, serialization, formatting, or exception behavior
- JDBC or R2DBC semantics, driver metadata, or type mappings
- documented features in `docs/features.md`
- security-sensitive code, authentication, credentials, or network-facing behavior
- cross-module contracts where the same concept exists in multiple modules

When human review is needed, call out the exact risk and what should be checked, for example:

- verify that no public signature changed
- verify that output format and serialized values are unchanged
- verify that existing callers keep the same default behavior
- verify that focused tests cover compatibility-sensitive paths
- verify whether `docs/features.md` or other docs need updates

## Verdict

Choose exactly one verdict:

- `ready to merge`: no blocking issues found and no additional human sign-off is needed beyond normal review flow
- `ready for human review`: no blocking issues found by AI, but the change is important enough that a human should confirm compatibility, behavior, or product intent
- `need changes`: blocking issues, regressions, compatibility risk, or missing validation were found and should be addressed before merge

## Copy/Paste Template

```md
## Findings

### High
- None.

### Medium
- None.

### Low
- None.

## Human Review Instructions for Important Changes
- None.

## Verdict
`ready to merge`
```
