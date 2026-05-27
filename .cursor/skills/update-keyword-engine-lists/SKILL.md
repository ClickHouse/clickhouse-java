---
name: update-keyword-engine-lists
description: Update ALLOWED_KEYWORD_ALIASES in ClickHouseSqlUtils.java and ENGINE_TO_TABLE_TYPE in DatabaseMetaDataImpl.java from failing test output. Use when StatementSQLTest.testAllowedKeywordAliasesMatchSystemKeywords or DatabaseMetaDataTest.testAllTableEnginesFromSystemTableEnginesAreMapped fails.
---

# Update keyword and engine lists from test failures

## Files

**Keywords:**
`jdbc-v2/src/main/java/com/clickhouse/jdbc/internal/parser/javacc/ClickHouseSqlUtils.java`
— `initAllowedKeywordAliases()`, append after the last `// Appended` comment block.

**Engines:**
`jdbc-v2/src/main/java/com/clickhouse/jdbc/metadata/DatabaseMetaDataImpl.java`
— `ENGINE_TO_TABLE_TYPE` static block, insert a new group before `// Special`.

## Steps

1. Parse the failing test message to extract the list of missing items.

2. **For keywords** (`StatementSQLTest` failure — e.g. `["CURSOR", "DETERMINISTIC", ...]`):
   Append a new dated comment + entries at the end of `buildKeywordSet(...)`, after the last existing `// Appended` block:
   ```java
   // Appended MM/DD/YYYY
   "KEYWORD1", "KEYWORD2", ...
   ```
   The previous entry must have a trailing comma. Keep alphabetical order within the new block.

3. **For engines** (`DatabaseMetaDataTest` failure — e.g. `[Paimon, PaimonAzure, ...]`):
   Add a new named group before `// Special` in the static block:
   ```java
   // <GroupName> (appended MM/DD/YYYY)
   map.put("Engine1", TableType.REMOTE_TABLE.getTypeName());
   ```
   Choose `TableType` by analogy:
   - External storage (S3/Azure/HDFS variants, lake formats) → `REMOTE_TABLE`
   - MergeTree family or local engines → `TABLE`

## Output

After editing, output this git commit command:

```
git commit -m "jdbc-v2: update <keywords|engines|keywords and engines> in JDBC"
```
