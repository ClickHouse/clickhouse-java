# Source Map

A structural map of the repository for fast, offline triage. Use it to pick the
**module label** and **`area:*` labels** and to jump straight to the relevant
source instead of grepping the whole tree.

Keep this map structural (modules, packages, entry points, label → location).
Do not add line numbers or exhaustive class lists — they go stale. When a
mapping below is wrong because the code moved, fix the boundary here.

## Modules at a glance

| Module label | Directory | Root package | What it is |
| --- | --- | --- | --- |
| `client-api-v2` | `client-v2/` | `com.clickhouse.client.api` | Current HTTP client (the "v2" client). |
| `client-v1` | `clickhouse-client/`, `clickhouse-http-client/` | `com.clickhouse.client` (no `.api`) | Legacy v1 client stack. |
| `client-v1` (data) | `clickhouse-data/` | `com.clickhouse.data` | Shared data types, codecs, formats, streams. Used by v1 and indirectly elsewhere. |
| `jdbc-v2` | `jdbc-v2/` | `com.clickhouse.jdbc` | Current JDBC driver (built on client-v2). |
| `jdbc-v1` | `clickhouse-jdbc/` | `com.clickhouse.jdbc` | Legacy JDBC driver (built on client-v1). |
| (r2dbc) | `clickhouse-r2dbc/` | `com.clickhouse.r2dbc` | R2DBC integration. No dedicated label — use `area:integration`. |


## Disambiguating v1 vs v2 (important)

Package prefixes overlap between versions, so use class-name cues:

- **Client v2 vs v1**: v2 lives under `com.clickhouse.client.api.*` and its entry
  point is `Client` (`client-v2/.../api/Client.java`). v1 lives under
  `com.clickhouse.client.*` / `com.clickhouse.client.http.*` with no `.api`
  segment (e.g. `ClickHouseClient`, `ClickHouseHttpClient`).
- **JDBC v2 vs v1**: both use `com.clickhouse.jdbc`. v2 uses `*Impl` classes
  (`ConnectionImpl`, `StatementImpl`, `PreparedStatementImpl`, `ResultSetImpl`,
  `DatabaseMetaDataImpl`, `Driver`, `DataSourceImpl`) under `jdbc-v2/`.
  v1 uses `ClickHouse*` classes (`ClickHouseConnection`, `ClickHouseStatement`,
  `ClickHouseDriver`, `ClickHouseDataSource`) under `clickhouse-jdbc/`.
- If the issue does not say which version, note it as a question for the user,
  but make a best guess from the class names / JDBC URL / Maven coordinates in
  the report.


## `area:*` label → source location

Use these to attach `area:` labels and to find the implementing code.

- **`area:client-insert`** — `client-v2/.../api/insert/` (`InsertResponse`,
  `InsertSettings`) and `Client.insert*`. v1: `clickhouse-client`/`http-client`.
- **`area:client-read`** — `client-v2/.../api/query/` (`QueryResponse`,
  `QuerySettings`, `GenericRecord`, `Records`) and `Client.query*`.
- **`area:client-pojo-serde`** — `client-v2/.../api/serde/` (`POJOSerDe`,
  `POJOFieldSerializer`, `POJOFieldDeserializer`) and
  `client-v2/.../api/metadata/` matching strategies (`ColumnToMethodMatchingStrategy`).
- **`area:data-type`** — `clickhouse-data/.../data/` core (`ClickHouseDataType`,
  `ClickHouseColumn`, `ClickHouseValue(s)`, `value/`) and client-v2
  `api/data_formats/` readers/writers. jdbc-v2 type wrappers in
  `jdbc-v2/.../jdbc/types/` (`Array`, `Struct`).
- **`area:format`** — `clickhouse-data/.../data/format/` (RowBinary, TSV, JSON
  processors) and `client-v2/.../api/data_formats/` (`RowBinary*FormatReader/Writer`,
  `NativeFormatReader`). Compression: `clickhouse-data/.../data/compress/`.
- **`area:network`** — `client-v2/.../api/transport/` (`Endpoint`, `HttpEndpoint`),
  `api/http/`, connection/config (`ClientConfigProperties`,
  `ConnectionInitiationException`, `ConnectionReuseStrategy`). v1:
  `clickhouse-http-client/.../client/http/`.
- **`area:error-handling`** — exception types across modules: client-v2
  `ClientException`, `ServerException`, `ClickHouseException`, `ClientFaultCause`,
  `DataTransferException`; jdbc `SqlExceptionUtils` (v1); data `ClickHouseChecker`.
- **`area:jdbc-insert`** — `jdbc-v2/.../jdbc/` `PreparedStatementImpl`,
  `WriterStatementImpl`. v1: `clickhouse-jdbc/.../jdbc/ClickHousePreparedStatement`.
- **`area:jdbc-read`** — `jdbc-v2/.../jdbc/` `ResultSetImpl`, `StatementImpl`.
  v1: `ClickHouseResultSet`, `ClickHouseStatement`.
- **`area:jdbc-metadata`** — `jdbc-v2/.../jdbc/metadata/` (`DatabaseMetaDataImpl`,
  `ResultSetMetaDataImpl`, `ParameterMetaDataImpl`). v1:
  `ClickHouseDatabaseMetaData`, `ClickHouseResultSetMetaData`, `JdbcTypeMapping`.
- **`area:sql-parser`** — new parser in `jdbc-v2/.../jdbc/internal/parser/`
  (incl. `javacc/`).
- **`area:old-stmt-parsing`** — legacy parser in
  `clickhouse-jdbc/.../jdbc/parser/` (`ClickHouseSqlParser`, `ClickHouseSqlUtils`,
  `ClickHouseSqlStatement`).
- **`area:integration`** — `clickhouse-r2dbc/`, or third-party framework glue
  (Spring, Hibernate, etc.) referenced in the issue.
- **`area:packaging`** — `packages/clickhouse-jdbc-all/`, Maven `pom.xml` build/
  shading/distribution concerns.
- **`area:dependencies`** — dependency version bumps in `pom.xml` files.
- **`area:docs`** — documentation gaps (no code area implied).
- **`area:general`** — use only when nothing above fits.


## Entry points (start reading here)

- **client-v2**: `client-v2/.../api/Client.java` (builder + query/insert API),
  `ClientConfigProperties` (config keys).
- **jdbc-v2**: `jdbc-v2/.../jdbc/Driver.java`, `ConnectionImpl`, `StatementImpl`,
  `DriverProperties`.
- **client-v1**: `clickhouse-client/.../client/` and
  `clickhouse-http-client/.../client/http/`.
- **jdbc-v1**: `clickhouse-jdbc/.../jdbc/ClickHouseConnection.java`,
  `ClickHouseDriver`.
- **data**: `clickhouse-data/.../data/ClickHouseDataProcessor`,
  `ClickHouseColumn`, `format/`.


## Stacktrace → module heuristics

Map the top app-owned frames (ignore JDK/third-party frames) by package:

- `com.clickhouse.client.api.*` → **client-api-v2** (then pick area by sub-package:
  `insert`/`query`/`serde`/`data_formats`/`transport`).
- `com.clickhouse.client.*` (no `.api`) / `com.clickhouse.client.http.*` →
  **client-v1**.
- `com.clickhouse.jdbc.*Impl` or `com.clickhouse.jdbc.internal.parser.*` →
  **jdbc-v2**.
- `com.clickhouse.jdbc.ClickHouse*` or `com.clickhouse.jdbc.parser.*` →
  **jdbc-v1**.
- `com.clickhouse.data.*` → **clickhouse-data** (shared); attach `area:data-type`
  / `area:format` / compression by sub-package. Identify the *caller* module to
  set the module label.
- `com.clickhouse.r2dbc.*` → r2dbc (`area:integration`).
