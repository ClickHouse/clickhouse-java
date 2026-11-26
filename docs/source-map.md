# Source Map

A structural map of the repository. Use it to find the module and the code of an
issue, a stack trace, or a change, instead of grepping the whole tree. Issue
triage uses it to pick the **module label**.

Keep this map structural (modules, packages, entry points, code area → location).
Do not add line numbers or exhaustive class lists — they go stale. When a
mapping below is wrong because the code moved, fix the boundary here.

## Modules at a glance

| Module label | Directory | Root package | What it is |
| --- | --- | --- | --- |
| `client` | `client-v2/` | `com.clickhouse.client.api` | Current HTTP client (the "v2" client). |
| `client` + `legacy` | `clickhouse-client/`, `clickhouse-http-client/` | `com.clickhouse.client` (no `.api`) | Legacy v1 client stack. |
| label of the calling module | `clickhouse-data/` | `com.clickhouse.data` | Shared data types, codecs, formats, streams. Used by v1 and by client-v2. |
| `jdbc` | `jdbc-v2/` | `com.clickhouse.jdbc` | Current JDBC driver (built on client-v2). |
| `jdbc` + `legacy` | `clickhouse-jdbc/` | `com.clickhouse.jdbc` | Legacy JDBC driver (built on client-v1). The module also holds the proxies `ClickHouseDriver` and `ClickHouseDataSource`, which run jdbc-v2 by default: the artifact `com.clickhouse:clickhouse-jdbc` is jdbc-v2. |
| `r2dbc` | `clickhouse-r2dbc/` | `com.clickhouse.r2dbc` | R2DBC integration (built on client-v1). |

Add `legacy` only when only the v1 stack has the problem. An issue in v1 and v2
gets only the component label.


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
  `ClickHousePreparedStatement`, `ClickHouseResultSet`) and `DriverV1`,
  `DataSourceV1` under `clickhouse-jdbc/`. `ClickHouseDriver` and
  `ClickHouseDataSource` in the same module are proxies: they run jdbc-v2 unless
  the application selects v1 (`clickhouse.jdbc.v1=true` or
  `clickhouse.jdbc.v2=false`), so they do not show the version.
- If the issue does not say which version, note it as a question for the user,
  but make a best guess from the class names / JDBC URL / Maven coordinates in
  the report.


## Code area → source location

Use these to find the code that implements an area. The JDBC metadata and SQL
parser areas (v1 and v2) also have a detail label: `jdbc-metadata` and `sql-parser`.

- **Client insert** — `client-v2/.../api/insert/` (`InsertResponse`,
  `InsertSettings`) and `Client.insert*`. v1: `clickhouse-client`/`http-client`.
- **Client read** — `client-v2/.../api/query/` (`QueryResponse`,
  `QuerySettings`, `GenericRecord`, `Records`) and `Client.query*`.
- **Client POJO serialization** — `client-v2/.../api/serde/` (`POJOSerDe`,
  `POJOFieldSerializer`, `POJOFieldDeserializer`) and
  `client-v2/.../api/metadata/` matching strategies (`ColumnToMethodMatchingStrategy`).
- **Data types** — `clickhouse-data/.../data/` core (`ClickHouseDataType`,
  `ClickHouseColumn`, `ClickHouseValue(s)`, `value/`) and client-v2
  `api/data_formats/` readers/writers. jdbc-v2 type wrappers in
  `jdbc-v2/.../jdbc/types/` (`Array`, `Struct`).
- **Data formats** — `clickhouse-data/.../data/format/` (RowBinary, TSV, JSON
  processors) and `client-v2/.../api/data_formats/` (`RowBinary*FormatReader/Writer`,
  `NativeFormatReader`). Compression: `clickhouse-data/.../data/compress/`.
- **Network** — `client-v2/.../api/transport/` (`Endpoint`, `HttpEndpoint`),
  `api/http/`, connection/config (`ClientConfigProperties`,
  `ConnectionInitiationException`, `ConnectionReuseStrategy`). v1:
  `clickhouse-http-client/.../client/http/`.
- **Error handling** — exception types across modules: client-v2
  `ClientException`, `ServerException`, `ClickHouseException`, `ClientFaultCause`,
  `DataTransferException`; jdbc `SqlExceptionUtils` (v1); data `ClickHouseChecker`.
- **JDBC insert** — `jdbc-v2/.../jdbc/` `PreparedStatementImpl`,
  `WriterStatementImpl`. v1: `clickhouse-jdbc/.../jdbc/ClickHousePreparedStatement`.
- **JDBC read** — `jdbc-v2/.../jdbc/` `ResultSetImpl`, `StatementImpl`.
  v1: `ClickHouseResultSet`, `ClickHouseStatement`.
- **JDBC metadata** — `jdbc-v2/.../jdbc/metadata/` (`DatabaseMetaDataImpl`,
  `ResultSetMetaDataImpl`, `ParameterMetaDataImpl`). v1:
  `ClickHouseDatabaseMetaData`, `ClickHouseResultSetMetaData`, `JdbcTypeMapping`.
- **SQL parser (jdbc-v2)** — new parser in `jdbc-v2/.../jdbc/internal/parser/`
  (incl. `javacc/`).
- **SQL parser (jdbc-v1)** — legacy parser in
  `clickhouse-jdbc/.../jdbc/parser/` (`ClickHouseSqlParser`, `ClickHouseSqlUtils`,
  `ClickHouseSqlStatement`).
- **Integrations** — `clickhouse-r2dbc/`, or third-party framework glue
  (Spring, Hibernate, etc.) referenced in the issue.
- **Packaging** — `packages/clickhouse-jdbc-all/`, Maven `pom.xml` build/
  shading/distribution concerns.
- **Dependencies** — dependency version bumps in `pom.xml` files.


## Entry points (start reading here)

- **client-v2**: `client-v2/.../api/Client.java` (builder + query/insert API),
  `ClientConfigProperties` (config keys).
- **jdbc-v2**: `jdbc-v2/.../jdbc/Driver.java`, `ConnectionImpl`, `StatementImpl`,
  `DriverProperties`.
- **client-v1**: `clickhouse-client/.../client/` and
  `clickhouse-http-client/.../client/http/`.
- **jdbc-v1**: `clickhouse-jdbc/.../jdbc/ClickHouseConnection.java`,
  `DriverV1`.
- **data**: `clickhouse-data/.../data/ClickHouseDataProcessor`,
  `ClickHouseColumn`, `format/`.


## Stacktrace → module heuristics

Map the top app-owned frames (ignore JDK/third-party frames) by package:

- `com.clickhouse.client.api.*` → **client-v2** (then the code area by sub-package:
  `insert`/`query`/`serde`/`data_formats`/`transport`).
- `com.clickhouse.client.*` (no `.api`) / `com.clickhouse.client.http.*` →
  **client-v1**.
- `com.clickhouse.jdbc.*Impl` or `com.clickhouse.jdbc.internal.parser.*` →
  **jdbc-v2**.
- `com.clickhouse.jdbc.ClickHouse*` (not the proxies `ClickHouseDriver` and
  `ClickHouseDataSource`), `DriverV1`, `DataSourceV1`, or
  `com.clickhouse.jdbc.parser.*` → **jdbc-v1**.
- `com.clickhouse.data.*` → **clickhouse-data** (shared); then the code area
  (data types, data formats, compression) by sub-package. Identify the *caller*
  module to set the module label.
- `com.clickhouse.r2dbc.*` → **r2dbc**.
