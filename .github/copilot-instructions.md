# Copilot Instructions for `clickhouse-java`

Shared AI instructions live in `docs/ai-review.md`.

Keep review guidance here short and conservative:

- check for breaking changes before suggesting refactors or cleanup
- treat public API consistency and user-visible behavior stability as top priorities
- call out changes to signatures, defaults, parsing, serialization, type mappings, and JDBC or R2DBC semantics as high risk
- treat `BinaryStreamReader` as a fast path with minimal conversion; Java type mapping is not final, and nested reads (`Array`, `Map`, `Tuple`, `Nested`, `Dynamic`, `Variant`) must stay consistent with the primitive read
- prefer the smallest safe change and focused tests for compatibility-sensitive behavior

Use `docs/ai-review.md` for the full review workflow, compatibility checklist, and response structure.
