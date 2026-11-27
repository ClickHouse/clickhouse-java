# Extra read cases

Worked examples and the V1 request-to-`QuerySettings` map are in [SKILL.md](SKILL.md). Use those. This file only lists calls with no direct substitute.

| V1 | What to do |
|---|---|
| `external(ClickHouseExternalTable...)` | Comment. No V2 external-table method on this path. |
| `executeWithinTransaction` / `ClickHouseTransaction` | Comment. No silent substitute. |
| `output(OutputStream)` / `pipe` | Copy `QueryResponse.getInputStream()` to that sink and say so. |
| `response.firstRecord()` | First element from `queryRecords`, or one `reader.next()`. |
| Multi-node `read(nodes)` | `migrate-v1-client-instantiation`. |
