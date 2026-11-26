---
name: issue-priority
description: >
  Decide the priority of a `ClickHouse/clickhouse-java` issue: `Urgent`,
  `High`, `Medium`, or `Low`, for the org issue field **Priority**. Use this
  skill when the task is to "prioritize", "set the priority of", "rate", or
  "triage" an issue, or to explain why an issue has a given priority. It
  defines the rule for legacy issues, the reach tiers of the public API, the
  severity rows, the base-level matrix, the modifiers, and the confidence.
---

# Issue priority

This skill is the single source of truth for the **Priority** of
`ClickHouse/clickhouse-java` issues. People and automated triage use the same
rules.

The value goes into the org issue field **Priority** (`Urgent`, `High`,
`Medium`, `Low`). The field is visible to org members only. An automated tool
sets the value once, never changes a value that is already set, and keeps the
level and its reason out of public comments.

## When to set no priority

Set no priority when one of these is true:

- A maintainer can close the issue with no further action: it is already fixed
  on `main`, a duplicate, works as intended, already supported, answered, or
  obsolete.
- Only a deprecated public API of client-v2 or jdbc-v2 reaches the defect: an
  API with a `@Deprecated` annotation, or an API that the docs call
  deprecated. Examples: `Client.getEndpoints()`, the client-v2 property
  `product_name`, and the jdbc-v2 properties `custom_http_params`,
  `custom_settings`, and `typeMappings`. The rule looks at the API that reaches
  the defect: a deprecated class that a v2 path calls (for example
  `ClickHouseCityHash` in `CompressedBlockInputStream`) does not count. The v1
  stack is not deprecated code for this rule: see "Legacy issues".
- The issue is a question, or it is about CI, the release process, or a
  discussion.

## Vulnerabilities that are not public yet

A security issue must not stay public with a description of how to reproduce
it. This section applies when a reproduction or code inspection confirms a
vulnerability that is not public yet, as the classification skill of this
repository defines it
([`../issue-classification/SKILL.md`](../issue-classification/SKILL.md), "A
vulnerability that is not public yet"). It applies to every module, also to
the v1 stack.

- Report it in a private GitHub vulnerability report for this repository
  (**Security** → **Report a vulnerability**), never in a public issue,
  comment, or pull request. If a public issue reports it, copy the title, the
  text, and the comments of the issue into the report first.
- Set no Priority on the public issue. Close it with a comment that gives no
  details, and ask a repository administrator to delete it: closing does not
  hide the text of the issue.
- Rate the vulnerability with the rules below, and put the level and its reason
  into the private report as the proposed severity.

A vulnerability that is already public (a published CVE or GHSA, for example in
a dependency such as Apache HttpClient, Jackson, or `lz4-java`), and an
authentication method that does not work or is missing, get a Priority on the
issue as usual.

## Legacy issues (the v1 stack)

An issue is **legacy** when only the v1 stack has the problem: the v1 client
(`clickhouse-client`, `clickhouse-http-client`: `ClickHouseClient`,
`ClickHouseHttpClient`) or the v1 JDBC driver in `clickhouse-jdbc`
(`DriverV1`, `DataSourceV1`, `ClickHouseConnection`, `ClickHouseStatement`,
`ClickHousePreparedStatement`, `ClickHouseResultSet`). The v1 driver runs only
when the application asks for it (`clickhouse.jdbc.v1=true` or
`clickhouse.jdbc.v2=false`). The artifact `com.clickhouse:clickhouse-jdbc` and
its classes `ClickHouseDriver` and `ClickHouseDataSource` run jdbc-v2 by
default, so these names do not make an issue legacy. A `clickhouse-data`
defect that only the v1 stack reaches is legacy. The label `legacy` shows the
class. When the label and the code that runs do not agree, use the code.

A legacy issue gets `Low`, with one exception: a legacy issue that is
**critical for customers** gets `Urgent`. It is critical for customers when
both conditions are true:

1. The severity (section 2) is "Wrong data or security" or "Broken, no
   workaround".
2. The issue shows that customers are affected: it links a Linear customer
   request (`linear.app`), or a maintainer or a ClickHouse employee says that
   customers are affected.

The reach tiers, the base level, and the modifiers (sections 1, 3, and 4) do
not apply to a legacy issue. This includes modifier 1: a v1 vulnerability gets
`Urgent` only when it is critical for customers.

An issue that the v1 stack and client-v2 or jdbc-v2 both have is not legacy:
rate it by the client-v2 or jdbc-v2 path. `clickhouse-r2dbc` is built on the
v1 client, but an r2dbc issue (label `r2dbc`) is not legacy, also when the
cause is in the v1 code that r2dbc calls, unless a maintainer says so. Rate it
with the sections below, by its own reach.

## 1. Reach

**Reach** tells how many applications can hit the defect. It comes from the
public API that leads to the code with the defect: an API that almost every
application calls has a high reach, an API that few applications use has a low
reach. There are four tiers, highest first:

- **Core:** almost every application calls this API.
- **Common:** many applications use it, but not all.
- **Narrow:** few applications use it (rare types, experimental APIs, special
  setups).
- **Satellite:** applications do not import it (examples, tests, docs, tools).

To find the tier, find the most-used public API that reaches the defect, and
look it up in the table below. Use the API, not the file that contains the
defect: a defect in a `BinaryStreamReader` method that only
`NativeFormatReader` calls (`readGeoNative`) is Narrow, and a defect in a
method that the reader of `RowBinaryWithNamesAndTypes` also calls
(`readValue`) is Core. A method or a property that the table puts into Common
or Narrow has that tier, also when its class is in Core.
[`docs/source-map.md`](../../../docs/source-map.md) maps classes and stack
traces to the modules.

jdbc-v2 is built on client-v2: both use the same HTTP transport, connection
pool, readers, and type-name parser (`client-v2` `api/internal/`,
`api/data_formats/`, and `clickhouse-data`). A defect in that code has the
tier of the most-used path that reaches it, through either module. These
defaults put a path into Core: the result format `RowBinaryWithNamesAndTypes`
(`format`), compressed responses (`compress` is `true`), the connection pool
(`connection_pool_enabled`), retries (`retry`), HTTP Basic authentication
(`http_use_basic_auth`), the server time zone (`use_server_time_zone`), JDBC
escape processing, and the JavaCC SQL parser (`jdbc_sql_parser`). A defect of
the shaded jar (`packages/clickhouse-jdbc-all`) has the tier of the API that
fails with it.

| Tier      | Public paths |
| --------- | ------------ |
| Core      | client-v2: `Client.Builder` and `build()`, `Client.query`, `queryAll`, `queryRecords`, `execute`, `ping`, and `insert` with an `InputStream` and a `ClickHouseFormat`; `QueryResponse`, `Records`, `GenericRecord`, and `ClickHouseBinaryFormatReader` for `RowBinaryWithNamesAndTypes`. jdbc-v2: `ClickHouseDriver` and `ClickHouseDataSource` of the `clickhouse-jdbc` artifact (they select jdbc-v2), `Driver`, `DataSourceImpl`, `ConnectionImpl`, `StatementImpl`, JDBC escape processing (`StatementImpl.escapedSQLToNative`), `PreparedStatementImpl` with the `?` values that it writes into the SQL text (`encodeObject`), batches (`addBatch`, `executeBatch`), the getters of `ResultSetImpl`, and these `ResultSetMetaData` methods: `getColumnCount`, `getColumnName`, `getColumnLabel`, `getColumnType`, `getColumnClassName`; the JavaCC SQL parser (`SqlParserFacade`), which reads each statement. Both: the HTTP transport (`HttpAPIClientHelper`), the connection pool (`max_open_connections`, `connection_request_timeout`), timeouts (`connection_timeout`, `socket_timeout`, `max_execution_time`), retries (`retry`, `client_retry_on_failures`), TLS (`ssl_mode`, `trust_store`, `sslrootcert`), user name and password authentication, the `User-Agent` header (`client_name`, `WorkloadIdentitySupport`); the parsing and validation of `ClientConfigProperties`, `DriverProperties`, and the JDBC URL; `QuerySettings` and `InsertSettings`, which each request uses; compressed responses (`CompressedBlockInputStream`), errors (`ClientException`, `ServerException`, `SQLException`), the reader of `RowBinaryWithNamesAndTypes` (`BinaryStreamReader`), the type-name parser (`ClickHouseColumn`), the server time zone for date and time values (`use_server_time_zone`), and these column types: integers up to 64 bits, `Float32`, `Float64`, `Bool`, `String`, `FixedString`, `Date`, `Date32`, `DateTime`, `DateTime64`, `Decimal`, `UUID`, `Enum8`, `Enum16`, `Nullable`, `LowCardinality`, `Array`, `Map`, `Tuple` |
| Common    | compressed requests (`decompress`, `compression.method` LZ4 and ZSTD, `CompressedBlockOutputStream`), HTTP compression (`client.use_http_compression`), `app_compressed_data`, the Java LZ4 implementation (`disable_native_compression`); POJO serialization (`Client.register`, `insert` with a `List` of POJOs, `queryAll` with a `Class`, `POJOSerDe`, `ColumnToMethodMatchingStrategy`), `RowBinaryFormatWriter`, `DataStreamWriter`, the readers of `RowBinary` and `RowBinaryWithNames`, `getTableSchema`; sessions (`Session`, `session_id`, `session_check`) and roles (`session_db_roles`, `setDBRoles`, `SET ROLE` with `remember_last_set_roles`); server-side query parameters (`Client.query(sql, params)`, `param_<name>`); server settings that the application sets (`serverSetting`, `clickhouse_setting_*`, `default_query_settings`), custom HTTP headers (`httpHeader`, `http_header_*`), the query ID (`query_id`, `jdbc_query_id_generator`); a time zone that the application sets (`use_time_zone`); asynchronous operations (`async`); `Statement.setQueryTimeout`, `cancel`, and `setMaxRows`; the other methods of `ResultSetMetaData`, `DatabaseMetaData` (also `ShowStatementsMetaData`), and `ParameterMetaData`; the JDBC `Array` and `Struct` (`createArrayOf`, `createStruct`); authentication with HTTP headers (`http_use_basic_auth` is `false`); more than one endpoint (a second `addEndpoint`, `ClientNodeSelector`); logging (SLF4J); the column types `JSON`, `Dynamic`, `IPv4`, `IPv6`, `Int128`, `Int256`, `UInt128`, `UInt256`, `Nested`, `SimpleAggregateFunction` |
| Narrow    | the other column types: `Variant`, Geo (`Point`, `Ring`, `LineString`, `MultiPoint`, `MultiLineString`, `Polygon`, `MultiPolygon`, `Geometry`), `QBit`, `Time`, `Time64`, `BFloat16`, `AggregateFunction`, `Interval*`, `Nothing`; the other result formats: `Native` (`NativeFormatReader`), `JSONEachRow` (`JSONEachRowFormatReader`, `jdbc_json_parser_factory`); `binary_string_support`, a `type_hint_mapping` that the application sets (the default `Array` to `List` hint of jdbc-v2 has the tier of `Array`), `jdbc_type_mappings`, `ResultSet.getObject` with a type map; the RowBinary writer of jdbc-v2 (`beta.row_binary_for_simple_insert`, `WriterStatementImpl`); the ANTLR4 SQL parsers (`jdbc_sql_parser` `ANTLR4`, `ANTLR4_PARAMS_PARSER`); the other authentication methods (`bearer_token`, `access_token`, `ssl_authentication` with `sslcert` and `ssl_key`) and credential updates at run time (`updateUserAndPassword`, `updateBearerToken`, `updateAccessToken`); the proxy (`proxy_type`, `addProxy`); HTTP cookies (`client.http.cookies_enabled`), form data for queries (`client.http.use_form_request_for_query`), the socket options (`socket_rcvbuf`, `socket_sndbuf`, `socket_linger`, `socket_tcp_nodelay`), `ssl_socket_sni`, `ssl_cipher_suites`, `connection_reuse_strategy`, `connection_ttl`, `http_keep_alive_timeout`; `jdbc_cluster_name`; OpenTelemetry (`SpanRecorder`, `OpenTelemetrySpanRecorder`) and Micrometer (`MetricsRecorder`, `MicrometerMetricsRecorder`, `registerClientMetrics`, `jdbc_metrics_recorder`); the GraalVM native-image files (`META-INF/native-image`); `clickhouse-r2dbc` |
| Satellite | `examples/`, `performance/` (benchmarks), the test code (`src/test/` of each module, `tests/`), `docs/`, `README.md`, `CHANGELOG.md`, `history/`, `migration-helpers/`, the build tools (`clickhouse-annotation-processor`, `release.sh`, `build_examples.sh`), the agent files in `.claude/` and `.cursor/` |

## 2. Severity

First name the **impacts** of the issue with the classification skill of this
repository
([`../issue-classification/SKILL.md`](../issue-classification/SKILL.md)): none,
one, or more of Security (part vulnerability or part authentication), Data
integrity, Stability, and Performance. Most issues have none.

Then use the first severity row that fits. With more than one impact, the
highest row applies:

| Severity                | Use when |
| ----------------------- | -------- |
| Wrong data or security  | impact **Data integrity**: the client returns or stores wrong or incomplete data with no error, or reports success for a query that failed; or impact **Security** with a vulnerability (for a vulnerability in a dependency: only when the client calls the vulnerable code) |
| Broken, no workaround   | the feature fails (an exception, a hang, a leak that ends in an `OutOfMemoryError` or in exhausted connections), and the caller has no reasonable workaround; also a wrong or incomplete error with no workaround: the failure is reported, but its type, code, or message is wrong or lost. The impact **Stability** (a hang, a `StackOverflowError` or `OutOfMemoryError`, such a leak) uses this row when the caller has no reasonable workaround |
| Workaround exists       | the feature fails, but the caller can avoid the failure with a small change in their own code (name the workaround in the reason); also impact **Performance** of a defect: a result that is correct but slow or uses too much memory (a request to make an operation faster is a feature request: last row) |
| Cosmetic, docs, feature | wrong log or message text, documentation, a feature request; also a request to make an operation faster, and a dependency update (also one for a vulnerability in a dependency, in code that the client does not call) |

**An exception of a type that the API does not declare.** The JDBC methods of
jdbc-v2 must throw `SQLException`, and client-v2 throws the subclasses of
`ClickHouseException` (`ClientException`, `ServerException`, and others).
Sometimes the code of the driver or the client throws a different unchecked
exception, for example a `NullPointerException`, `ClassCastException`,
`IllegalStateException`, or `ArrayIndexOutOfBoundsException`. A caller that
catches the declared type does not catch it. To catch all
`RuntimeException`s is not a reasonable workaround, so such a failure uses the
row "Broken, no workaround". When the caller can prevent the failure with a
different small change, use "Workaround exists". These are not this case: an
exception that the Javadoc or `docs/features.md` documents for the call (for
example the `IllegalArgumentException` for an endpoint that is not a valid
URL), and an exception from the caller's own code (for example from a POJO
setter).

An issue that has the impact Security with no vulnerability (an authentication
method that does not work or is missing) does not use the first row. Rate an
authentication method that does not work by its failure ("Broken, no
workaround" or "Workaround exists"), and a request for a missing method as a
feature request.

## 3. Base level

| Severity ↓ / Reach →    | Core   | Common | Narrow | Satellite |
| ----------------------- | ------ | ------ | ------ | --------- |
| Wrong data or security  | Urgent | Urgent | High   | Medium    |
| Broken, no workaround   | High   | High   | Medium | Low       |
| Workaround exists       | Medium | Medium | Low    | Low       |
| Cosmetic, docs, feature | Low    | Low    | Low    | Low       |

## 4. Modifiers

Apply them in this order:

1. **Security with a shown path from untrusted input.** The issue or a
   reproduction shows how a value that an application takes from its users
   reaches the defect: `Urgent` at any reach. Skip the other modifiers. A
   defect that puts a caller-supplied value into SQL without quoting or
   escaping is a candidate: look for such a path. Examples: the `?` values
   that `PreparedStatementImpl` writes into the SQL text (`encodeObject`), the
   helpers `SQLUtils.enquoteLiteral`, `enquoteIdentifier`, and
   `escapeSingleQuotes` (also through `Statement.enquoteLiteral` and
   `enquoteIdentifier`), and the SQL that `DatabaseMetaDataImpl` and
   `ShowStatementsMetaData` (`quoteIdentifier`) make from the names and
   patterns of the caller. If none is shown, rate the defect by the failure
   that it shows. A vulnerability that is not public yet goes to a private
   vulnerability report, not to a public issue (see "Vulnerabilities that are
   not public yet").
2. **Ingestion.** The defect is on the insert path (`Client.insert` with
   POJOs, with an `InputStream`, or with a `DataStreamWriter`;
   `RowBinaryFormatWriter`; `Client.execute` with an `INSERT`; `INSERT`
   through `PreparedStatement` or `Statement` in jdbc-v2, with any `execute*`
   method; `WriterStatementImpl`), and the client stores data that is lost,
   corrupted, or different from the given value, with no error. This includes
   a missing field, and a column that gets a default value in place of the
   given value, or does not get its `DEFAULT` when the value is not given: up
   one level. This can give `Urgent`. Wrong data in a table stays wrong after
   the fix; a defect on the read path does not change the stored data.
3. **Regression in the latest published release:** up one level. This can give
   `Urgent`. It is a regression only when the latest release shows the problem
   and the release before it does not. The tags are `vX.Y.Z` (for example
   `v0.11.2`). The latest release is the highest version that is not a
   pre-release (`vX.Y.Z-rcN`); a patch release of an older line (for example
   `v0.9.9`, published after `v0.10.0`) is not the latest release. The release
   before it is the next lower version that is not a pre-release.
4. **Customer request in Linear.** The issue links (`linear.app`) a Linear
   issue for a customer request: up one level, but never to `Urgent` (`High`
   stays `High`).

A defect on the query path (`Client.query`, `queryAll`, `queryRecords`,
`Statement.executeQuery`, the getters of `ResultSet`) gets no ingestion
modifier. So at the same reach and severity, a query defect ranks below an
ingestion defect.

## 5. Confidence

A missing reproduction lowers the confidence, not the level. Record the
confidence with the value when the tool that sets it supports it:

| Confidence | Use when |
| ---------- | -------- |
| `HIGH`     | a run shows the problem (or, for a feature, code inspection confirms its absence on `main`) |
| `MEDIUM`   | code inspection shows a defect, but no reproduction was run (for example, it needs ClickHouse Cloud) |
| `LOW`      | the report lacks what a reproduction needs, or a run did not show the problem |

## 6. Reason

Write one line with the value:

`<tier> (<path>) · <impacts, or "no impact"> · <severity> · <modifiers, or "no modifier"> → <level>.`

For a legacy issue, the line is:

`Legacy (<path>) · <impacts, or "no impact"> · <severity> · <"critical for customers: <evidence>", or "not critical for customers"> → <level>.`

Name the workaround when the severity is "Workaround exists". When the
confidence is `LOW`, say what is missing.
