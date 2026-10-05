# 0.11.0-rc1, (2026-10-02)

[Release Migration Guide](/docs/releases/0_11_0.md)

## Breaking changes 

- **[client-v2, jdbc-v2]** The socket buffer options `socket_rcvbuf` and
  `socket_sndbuf` have no default value anymore. Now the options are applied only when the application sets them
  (`Client.Builder#setSocketRcvbuf`,
  `Client.Builder#setSocketSndbuf`, or the properties of the same name). This is done to let OS TCP Stack auto-tune
  values. Do not set them until absolutely needed. (https://github.com/ClickHouse/clickhouse-java/issues/3121)

- **[client-v2, jdbc-v2]** Server error `159` (`TIMEOUT_EXCEEDED`) is not retried by default anymore. It has its own
  retry cause, `ClientFaultCause.ServerTimeoutExceeded`, which `ServerRetryable` does not include and which is not in
  the default `client_retry_on_failures` list. Add `ServerTimeoutExceeded` to the list to retry this error.
  (https://github.com/ClickHouse/clickhouse-java/issues/3072)

- **[r2dbc]** Pre `1.0.0` artifact is not supported anymore because R2DBC API reached stable `1.0.0` version.

- **[jdbc-v2]** `DatabaseMetaData#getTables` now returns `null` in `REMARKS` (table comment) and `TYPE_SCHEM` by
  default, because metadata is read with `SHOW` statements (see the `jdbc_metadata_use_show_statements` entry in New
  Features). Set `jdbc_metadata_use_show_statements=false` to get the table comments.
  (https://github.com/ClickHouse/clickhouse-java/issues/2907)

- **[client-v2,jdbc-v2]** Added ZSTD compression support for Block compression stream and used by default not to
  compress client requests. Previously only LZ4 was supported in this case. Note: Added ZSTD library and native
  libraries to `-all` JDBC package because it is now required to work with server.
  (https://github.com/ClickHouse/clickhouse-java/issues/3105).


## Important Changes 

- **[client-v2]** `com.clickhouse.client.api.observability.SpanSupport` now uses `QUERY` and `INSERT` operation
  constants (`QUERY <database>` and `INSERT <database>.<table>` span names). `db.operation.name` attribute is set to
  `INSERT` for insert operations and left unset for queries because SQL statements are not parsed on the client.

- **[client-v2]** `com.clickhouse.client.api.metrics.OperationMetrics` now has a single constructor,
  `OperationMetrics(ClientStatisticsHolder, OperationType)`; the constructor without an operation type was removed.
  Metrics are created by the client, which always knows the kind of the operation it runs, and the constructor takes
  an internal type (`com.clickhouse.client.api.internal.ClientStatisticsHolder`), so application code is not expected
  to call it. (https://github.com/ClickHouse/clickhouse-java/issues/2974)

## New Features 

### Data Types 

- **[client-v2, jdbc-v2]** Added support for the `MultiPoint` geo data type (ClickHouse `26.8+`). Previously the type
  was unknown to the client, so reading or writing a `MultiPoint` column failed with `Unknown data type: MultiPoint`,
  and a `MultiPoint` value inside a `Geometry` column failed with an out-of-range variant discriminator. `MultiPoint` is
  `Array(Point)` on the wire, exactly like `Ring` and `LineString`, so it is read and written as `double[][]` through
  generic records, binary readers, POJO binding, and SQL parameter formatting, and is read from `Dynamic` columns. In
  the JDBC driver (`jdbc-v2`) `MultiPoint` maps to `java.sql.Types.ARRAY`, is returned as `double[][]` from `getObject`
  and as a `java.sql.Array` from `getArray`, and is reported by `ResultSetMetaData` and `DatabaseMetaData`. ClickHouse
  `26.8` also adds `MultiPoint` to the `Geometry`
  variant; the server appends it after the existing six variants instead of ordering it by type name, so the client now
  keeps that order and decodes a `MultiPoint` held in a `Geometry` column. Because `MultiPoint` shares its Java
  representation (`double[][]`) with `Ring` and `LineString`, it is not selectable through the shape-based `Geometry`
  write path — a 2D value keeps resolving to `Ring` as before, and writing `MultiPoint` requires a concrete `MultiPoint`
  column. (https://github.com/ClickHouse/clickhouse-java/issues/3048)

- **[client-v2, jdbc-v2]** Added support for the `BFloat16` data type (ClickHouse `24.11+`). `BFloat16` columns are read as
  Java `float` values (widening is lossless) and written from `float`/`Float` values, including through generic records, POJO
  binding, `Nullable(BFloat16)`, and `BFloat16` values held in `Dynamic`/`Variant` columns. On write the client keeps the
  high 16 bits of the `float`, matching the ClickHouse server's own `Float32` → `BFloat16` conversion. In the JDBC driver
  (`jdbc-v2`) `BFloat16` maps to `java.sql.Types.FLOAT` / `java.lang.Float` and is read and written through the standard
  `getFloat`/`setFloat` and `getObject` accessors, and reported as such by `ResultSetMetaData` and `DatabaseMetaData`.
  Previously reading or writing a `BFloat16` column failed with an
  unsupported-data-type error. (https://github.com/ClickHouse/clickhouse-java/issues/2279)

- **[client-v2, jdbc-v2]** Added support for the experimental `QBit(element_type, dimension[, stride])` vector data type
  (ClickHouse `25.10+`; the `allow_experimental_qbit_type` server setting is required to create a column). The type-name
  parser accepts two or three parameters (the optional third is the stride) and recognizes the documented element types
  `Int8`, `BFloat16`, `Float32`, and `Float64`; an element type outside that set is parsed with a warning rather than
  rejected, so a newer server-side element type keeps parsing. A `QBit` value is transmitted over `RowBinary` exactly like
  `Array(element_type)`, so it is read and written as a Java array of the element type (`float[]` for
  `BFloat16`/`Float32`, `double[]` for `Float64`) through generic records, binary readers, and POJO binding, via a
  dedicated `QBit` read/serialize path. A `QBit` held inside a `Dynamic`/`Variant`/`JSON` column is also decoded (its
  binary type encoding is read back to the concrete `QBit(...)` type). In the
  JDBC driver (`jdbc-v2`) `QBit` maps to `java.sql.Types.ARRAY` and is returned as a `java.sql.Array` from
  `getObject`/`getArray`. Previously `QBit` was an unimplemented type constant and reading or writing such a column
  failed. A plain top-level `QBit` column with a `Float32`, `Float64`, or `BFloat16` element type is also read through
  the `Native` output format: there the server transmits it using its internal bit-plane-transposed
  `Tuple(FixedString(...))` layout, and the client reverses that transposition to reconstruct the same
  `float[]`/`double[]` vector as `RowBinary`. A `QBit` that is strided (`QBit(element_type, dimension, stride)`),
  wrapped in `Nullable`/`LowCardinality`, nested inside another type (e.g. `Array`/`Tuple`/`Map(String, QBit(...))`),
  or carrying any other element type is not yet decoded over `Native` and fails fast with a clear error directing you
  to a `RowBinary` format such as `RowBinaryWithNamesAndTypes`.
  (https://github.com/ClickHouse/clickhouse-java/issues/2610)


### Observability

- **[client-v2, jdbc-v2]** Added a Micrometer implementation of the metrics SPI.
  `Client.Builder.setMetricsRecorder(new MicrometerMetricsRecorder(meterRegistry))` reports the metrics of every client
  operation to a Micrometer `MeterRegistry`: a timer `db.client.operation.duration` per completed operation, a timer
  `clickhouse.client.operation.serialization.duration` when the client measured the serialization step, a counter
  `clickhouse.client.operation.count` per completed operation, and a counter `clickhouse.client.operation.retries` per
  retried attempt. Previously the client could bind only its connection-pool gauges to Micrometer, so exporting the
  metrics of the operations themselves was left to the application. Meter names, units, descriptions and tag keys are
  the standard ones of the SPI - the recorder derives them through `MetricsSupport`, so they are the names of
  `MetricName` and the keys of `MetricAttribute` and mean the same as for every other recorder. A successful operation
  carries no `error.type` tag and a failed one does, so the outcomes are separate time series of the same meter and a
  failure the server reported also carries `db.response.status_code`; a duration the client did not measure is not
  recorded, so no operation is reported with a made-up duration. The seconds of the SPI are handed to the registry as
  nanoseconds, because a Micrometer timer keeps its own time unit, so a backend publishes the duration in the unit it
  expects. The no-argument constructor reports to `Metrics.globalRegistry`, which is what the jdbc-v2
  `jdbc_metrics_recorder` property needs, so a JDBC connection exports its metrics to Micrometer by naming the class -
  `jdbc_metrics_recorder=com.clickhouse.client.api.observability.micrometer.MicrometerMetricsRecorder` - without
  application code. `micrometer-core` stays an optional dependency of `client-v2` and is not shaded into the `all`
  artifacts, so a client that does not use this recorder needs no Micrometer on the classpath.
  (https://github.com/ClickHouse/clickhouse-java/issues/2975)

- **[client-v2, jdbc-v2]** Added a metrics SPI that lets an application export the metrics of client operations to any
  metrics backend. `Client.Builder.setMetricsRecorder(MetricsRecorder)` registers a backend-agnostic recorder from the
  `com.clickhouse.client.api.observability` package, and the jdbc-v2 property `jdbc_metrics_recorder` names the recorder
  class a connection registers with its own client. Previously the client collected operation metrics but only returned
  them to the caller, so exporting them was left to the application. Each completed operation reports exactly one
  success or one failure event, and each retried attempt reports a retry event, which gives the operation duration, the
  serialization duration, the number of operations by outcome and the number of retries. The SPI follows the pattern of
  the span SPI: an implementation extends the `DefaultMetricsRecorder` base class and overrides only what it cares
  about, so it keeps working when the client starts reporting an event it does not know about, and the reusable
  `MetricsSupport` class derives the standard values from the same structures, so its logic is opt-in and overridable.
  Metric names, units and attribute keys follow the OpenTelemetry semantic conventions for database clients where a
  convention exists and are placed under `clickhouse.` where it does not; they are defined by the `MetricName` and
  `MetricAttribute` enums, durations are reported in seconds, and a duration the client did not measure is reported as
  `MetricsSupport.DURATION_UNKNOWN` instead of a made-up value. The metric attributes are deliberately a smaller set
  than the span attributes, because an attribute of a metric becomes a time series: the statement text, the query id and
  the statement parameters stay on spans. Nothing is recorded and no metrics-related work is done when no recorder is
  registered. (https://github.com/ClickHouse/clickhouse-java/issues/2975)

- **[client-v2]** Added an OpenTelemetry implementation of the observability SPI.
  `Client.Builder.setSpanRecorder(new OpenTelemetrySpanRecorder(openTelemetry))`
  reports every client operation and every transport request as an OpenTelemetry `CLIENT` span: an operation span is
  started as a child of the current OpenTelemetry context, so it joins the application's own trace, and each request
  span - including one per retry - is a child of its operation span. Span names and attribute keys are the standard ones
  of the SPI (the recorder derives them through `SpanSupport`), every value is recorded with the OpenTelemetry attribute
  type that matches it, and a failure sets the span status to `ERROR` and is recorded as an OpenTelemetry exception
  event next to the `error.type` and `db.response.status_code` attributes. The recorder reports to a supplied
  `OpenTelemetry` instance, to a `Tracer` given to `new OpenTelemetrySpanRecorder(Tracer)`, or to
  `GlobalOpenTelemetry` - read when a span is started - when constructed without arguments. Previously an application
  that wanted OpenTelemetry spans had to write that mapping itself. The OpenTelemetry API is a compile-only dependency
  of
  `client-v2`: the recorder is used only by an application that already provides `opentelemetry-api` at runtime, so
  nothing is added to the classpath of a client that does not use it.
  (https://github.com/ClickHouse/clickhouse-java/issues/2974)

- **[client-v2]** Added an observability SPI that lets an application observe client operations as spans.
  `Client.Builder.setSpanRecorder(SpanRecorder)` registers a backend-agnostic recorder from the new
  `com.clickhouse.client.api.observability` package: each operation (a query, a command or an insert - including the
  `ping` and `getTableSchema` calls, which run a query) starts one operation span, and every transport request made for
  it - including each retry - starts a child request span. `SpanRecorder` and `Span` are plain interfaces; an
  implementation extends the
  `DefaultSpanRecorder` base class and overrides only what it cares about, so it keeps working when the client starts a
  kind of span it does not know about. The registered recorder is called first and receives everything the client knows
  about the operation (its `QuerySettings`/`InsertSettings`, the statement, the target table, the batch size, the
  endpoint, the metrics of the completed operation and the failure), so it is free to record whatever it needs and in
  whatever form; the reusable `SpanSupport` class derives the standard span names and attribute values from those same
  structures and is called by a recorder implementation that wants them, so its logic is opt-in and overridable. Span
  names and attribute keys follow the OpenTelemetry semantic conventions for database and HTTP client spans; the keys
  are defined by the `SpanAttribute` enum and the values are derived by
  `SpanSupport`, so all recorders that use it report the same information (statement text, target database and table,
  query id, statement parameters, batch size, the first configured endpoint on the operation span and the per-attempt
  server address and port on the request spans, HTTP status, returned rows, and the error type and ClickHouse error code
  on failure). The outcome of a completed operation is reported per operation kind - `recordQuerySuccess`
  for a read and `recordInsertSuccess` for an insert - because the metrics that describe a read are not the ones that
  describe a write: a query reports `db.response.returned_rows`, `clickhouse.response.read_rows` and
  `clickhouse.response.read_bytes`, an insert reports `clickhouse.response.written_rows` and
  `clickhouse.response.written_bytes`. The same distinction is available on the metrics themselves through the new
  `OperationMetrics#getOperationType()`, which returns the new `com.clickhouse.client.api.metrics.OperationType` - the
  kind of the call the application made, so a command that writes is reported as a query. An operation span is started
  on the calling thread, so it joins the caller's ambient trace even when the operation runs on the client's executor,
  and it is ended exactly once for every operation that starts. Previously the client exposed no hook for tracing, so an
  application could not attribute a query or a retried request to its own trace. When no recorder is registered nothing
  is recorded and no span-related work is done, so the default path is unchanged. An OpenTelemetry implementation of the
  SPI is available as `OpenTelemetrySpanRecorder`. (https://github.com/ClickHouse/clickhouse-java/issues/2974)

## Improvements

- **[migration-helpers]** Added `migration-helpers` module containing `ConfigurationMigrationHelper` and
  `ConfigPropertyCache` to convert configuration properties and connection URLs from v1 (0.7.1) format to v2 (0.9.8+)
  format (automatically prefixing ClickHouse server settings with `clickhouse_setting_`, custom headers with
  `http_header_`, and mapping renamed property keys).

- **[client-v2, jdbc-v2]** Added TLS cipher suite selection. `Client.Builder.setSSLCipherSuites(String...)` (client-v2)
  and the comma-separated `ssl_cipher_suites` connection property (client-v2 and jdbc-v2) restrict the cipher suites
  enabled on secure connections; when unset, the transport defaults are used. Cipher-suite selection is independent of
  the trust configuration and `ssl_mode`. (https://github.com/ClickHouse/clickhouse-java/issues/2882)

- **[client-v2, jdbc-v2]** Added support for un-flattened `Nested(...)` columns (tables created with
  `flatten_nested = 0`). Previously the `RowBinary` writer threw `UnsupportedOperationException: Unsupported
  data type: Nested` when inserting such a column. `client-v2` now serializes a `Nested(f1 T1, ..., fN TN)`
  column the same way it is read — identically to `Array(Tuple(T1, ..., TN))` (a var-uint row count followed by one
  tuple per nested row) — so it can be written through the insert path / `RowBinaryFormatWriter`. In
  `jdbc-v2` an un-flattened `Nested(...)` column is exposed as a JDBC `ARRAY` whose element type is
  `Tuple(f1 T1, ..., fN TN)`: it can be inserted through `Connection#createArrayOf`/`setArray` or `setObject`
  and read back through `getArray`/`getObject`, and `java.sql.Array#getResultSet()` iterates the nested rows as
  `(INDEX, VALUE)` pairs where each `VALUE` is the tuple. (https://github.com/ClickHouse/clickhouse-java/issues/2477)

- **[client-v2, jdbc-v2]** Added logging on previously-silent error and diagnostic paths (no functional or public-API
  change). (https://github.com/ClickHouse/clickhouse-java/issues/2969)

- **[jdbc-v2]** `DatabaseMetaData#getSchemas`, `#getTables` and `#getColumns` now read metadata with `SHOW DATABASES`,
  `SHOW TABLES` and `DESCRIBE TABLE` instead of the `system.databases`, `system.tables` and `system.columns` tables. The
  server does not show some tables in the system tables by default (for example, tables of `DataLakeCatalog`
  databases), so tools that use `DatabaseMetaData` did not find them. The new driver property
  `jdbc_metadata_use_show_statements` (default `true`) selects the implementation; set it to `false` to use the system
  tables. With the `SHOW` statements, `getTables()` returns `null` in `REMARKS` (table comment) and `TYPE_SCHEM`;
  `getColumns()` sends one `DESCRIBE TABLE` query for each table that matches and skips a table that was dropped
  meanwhile, that the user cannot describe, or whose data lake metadata cannot be read. All other columns and values are
  the same. (https://github.com/ClickHouse/clickhouse-java/issues/2907)

## Bug Fixes 

- **[jdbc-v2]** Fixed an `INSERT ... VALUES` statement whose values list holds a literal, an expression (e.g. `? + 1`)
  or, with the `JAVACC` parser, a JDBC escape sequence or a query parameter, being written with the RowBinary writer
  when `beta.row_binary_for_simple_insert` is enabled. The writer takes one bound value per column, so the bound values
  were shifted to other columns: the statement failed with a misleading error, or stored wrong data with no error. The
  parsers did not report such values: the `JAVACC` parser discarded the result of its values-list check, and the
  `ANTLR4` parsers reported only function calls. Now the writer is used only for a values list of `?` placeholders, and
  other statements use the standard `PreparedStatement` path.
  (https://github.com/ClickHouse/clickhouse-java/issues/3083)

- **[client-v2,jdbc-v2]** - Replaced slow HTTP LZ4 compression with compressing stream from
  `lz4-java` library. Client uses Apache Compress to handle HTTP compression (because it has convenient factory for many
  compressions methods. However, Apache Compress uses slow LZ4 implementation what causes very slow inserts. Now
  `lz4-java` used for insert. Query still slow but will be fix in future releases (need refactoring and custom
  implementation to solve it). Using `ZSTD` for queries should solve the issue.
  (https://github.com/ClickHouse/clickhouse-java/issues/2273).

- **[client-v2]** Fixed writing a `String` value into a `UUID` column (also as an `Array`/`Tuple` element or a `Map`
  key) failing with `ClassCastException`. The string is now parsed with `UUID.fromString`; a string it cannot parse is
  rejected on the client with an `IllegalArgumentException`. (https://github.com/ClickHouse/clickhouse-java/issues/3132)

- **[client-v2]** Fixed reading a `Nullable` column in the `Native` format failing with `Failed to read block ...
  End of stream reached before reading all data`, or returning values of the wrong rows. The reader consumed a null
  marker before every value, which is the RowBinary layout. `Native` is columnar: it stores a null map of one byte per
  row before the values of the whole column, and a null row still has a placeholder value. The markers were therefore
  taken from the value bytes and the column was read out of alignment. The null map is now read as a block.
  (https://github.com/ClickHouse/clickhouse-java/issues/3137)

- **[client-v2]** Fixed geo columns (`Point`, `Ring`, `LineString`, `MultiPoint`, `Polygon`, `MultiLineString`,
  `MultiPolygon`) being misread from the `Native` format. The reader decoded a geo column row by row with the RowBinary
  decoders, while `Native` writes it column-major: a `Point` block came back with its coordinates scrambled across rows
  and no error, and every other geo type desynchronized the block and failed with
  `Non-empty typeName is required`. A geo column is now decoded from the Native layout - the two `Float64`
  sub-columns of a point, and cumulative offsets plus the flattened elements for the array levels - and returns the same
  values as `RowBinaryWithNamesAndTypes`, which is unchanged.
  (https://github.com/ClickHouse/clickhouse-java/issues/3088)

- **[client-v2, jdbc-v2]** Fixed a column type with a `JSON` element that is followed by a parameterized type, for
  example `Tuple(JSON, FixedString(3))`, being parsed wrongly. `JSON` is valid with and without a parameter list, and
  the parser looked for the opening bracket of that list anywhere after the keyword, so it took the brackets of the next
  element for the parameters of the `JSON` element. Everything up to those brackets was consumed, which dropped the
  elements between them, or failed with `Unknown data type: <parameter>` when the following type had more than one
  parameter, for example `Tuple(JSON, Decimal(10, 2))`. Reading such a column, and `Client#getTableSchema` of a table
  that has one, failed or returned an incomplete type. A parameter list is now recognized only when it immediately
  follows the `JSON` keyword. (https://github.com/ClickHouse/clickhouse-java/issues/3098)

- **[jdbc-v1]** Fixed `WITH RECURSIVE <name> AS (...)` failing to parse. The JavaCC grammar did not know the
  `RECURSIVE` keyword, so it was taken for the name of the first common table expression and the statement was rejected.
  The query itself still ran, because the driver falls back to sending the original SQL, but every
  `createStatement` / `prepareStatement` call logged a `WARN` parse failure and the statement was classified as unknown.
  `RECURSIVE` is now accepted after `WITH` and stays usable as an ordinary identifier (column, alias, table or CTE
  name). (https://github.com/ClickHouse/clickhouse-java/issues/3122)

- **[jdbc-v2]** Fixed `WITH RECURSIVE <name> AS (...)` failing to parse. Neither SQL grammar knew the `RECURSIVE`
  keyword, so it was taken for the name of the first common table expression and the statement was rejected. The query
  itself still ran, because the driver falls back to sending the original SQL, but every `createStatement` /
  `prepareStatement` call logged a parse failure (`WARN` with the JavaCC backend) and the statement was classified as
  unknown. `RECURSIVE` is now accepted after `WITH` by both the JavaCC and the ANTLR4 grammars, and stays usable as an
  ordinary identifier (column, alias, table or CTE name). (https://github.com/ClickHouse/clickhouse-java/issues/3122)

- **[jdbc-v2]** Fixed `ArrayResultSet#next()` leaving the cursor before-first for empty arrays or on the last row for
  non-empty arrays after exhaustion. The cursor now moves to the after-last state when `next()` returns `false`.

- **[jdbc-v2]** Added the non-reserved keywords `AGGREGATE`, `BOUNDED`, `EXTEND`, `HANDLER`, `IDLE`, `PROTOCOL`,
  `RECENT`, `TIMEOUT` and `UNORDERED` (ClickHouse `26.8+`; `IDLE`, `TIMEOUT` and `RECENT` come from the multi-word
  keywords `IDLE TIMEOUT` and `RECENT SAMPLES`) to the list of keywords allowed in identifier positions. The server
  accepts all of them as a column or table alias, so a query using one of them as an identifier must parse.
  (https://github.com/ClickHouse/clickhouse-java/issues/3113)

- **[client-v2]** Fixed a query with statement parameters sent in the request body
  (`client.http.use_form_request_for_query=true`) failing with `LZ4 decompression failed ... (LZ4_DECODER_FAILED)`
  when client request compression and HTTP compression were both enabled. The multipart body is always sent
  uncompressed, but the request still declared `Content-Encoding: lz4`; ClickHouse `26.8+` honours that header for
  multipart requests and tried to decompress a plain body. The header is now omitted for multipart requests, like the
  `decompress` query parameter already was. Response compression (`Accept-Encoding`,
  `enable_http_compression`) is unchanged. (https://github.com/ClickHouse/clickhouse-java/issues/3075)

- **[client-v1]** Fixed the `DateTime64` case of `testReadWriteSimpleTypes` failing against ClickHouse 26.8. From 26.8
  an unquoted number written to a `DateTime64` column in the `Values`/`Quoted` and `JSON` paths is a Unix timestamp in
  seconds instead of the raw scaled value - the server setting `input_format_read_datetime_number_as_raw_value`
  changed its default from `1` to `0` - so the `insert into ... values(1)` of the test stored `1970-01-01 00:00:01`
  instead of the expected `1970-01-01 00:00:00.001`. The test now writes a quoted date-time literal for `DateTime64`, as
  it already does for `FixedString` and `UUID`, so the written value means the same on every server version and the
  sub-second round-trip stays covered. No client code is affected: both clients quote a date-time value in a text
  statement or send it in `RowBinary`. (https://github.com/ClickHouse/clickhouse-java/issues/3114)

- **[jdbc-v2, client-v2]** Fixes issue with `FORMAT` in query unable to override format set by client when used with
  ClickHouse 26.8+. Default format is `RowBinaryWithNamesAndTypes` set at client level. For JDBC, recommend using
  `format=JSONEachRow` to query JSON. Setting `format=` (empty or `null`) omits the format request header so explicit
  query `FORMAT` clauses take effect; note that on JDBC any statement without a `FORMAT` clause will fail because the
  server falls back to `default_format` (`TabSeparated`). `DatabaseMetaData` is unaffected: every statement it runs
  internally pins `RowBinaryWithNamesAndTypes` in its own settings, so metadata keeps working regardless of the
  connection's `format` property. (https://github.com/ClickHouse/clickhouse-java/issues/3086)

- **[jdbc-v2]** Fixed `Connection#prepareStatement` and `PreparedStatement#addBatch` throwing
  `StringIndexOutOfBoundsException` for an `INSERT ... VALUES (...)` statement containing a JDBC escape sequence
  (`{d '...'}`, `{ts '...'}`, ...) or a ClickHouse query parameter whose name starts with `d`/`t` (e.g. `{d:Int32}`).
  The default `JAVACC` parser records the values list positions as offsets into the SQL it rebuilds from the token
  stream, where such sequences are rewritten or dropped, while the driver slices the original SQL with them — so the
  slice was taken at the wrong offsets or past the end of the statement. The positions are now checked against the
  original SQL and discarded when they do not address its values list, in which case the driver falls back to its
  generic parameter substitution path. Such a statement is now prepared without error; the escape sequence itself is
  still sent to the server unchanged. The `ANTLR4` parser backends were not affected.
  (https://github.com/ClickHouse/clickhouse-java/issues/3017)

- **[jdbc-v2]** Fixed a `?` inside a `//` line comment or inside a heredoc (dollar quoted string, e.g. `$$...$$` or
  `$tag$...$tag$`) being counted as a `PreparedStatement` parameter. Such a statement expected a value the application
  could not supply, so `executeQuery()` failed with `Parameter at position 'N' is not set` for a query the server
  executes fine. The placeholder scan now skips both token kinds, like the server lexer does; a `$` that does not open a
  heredoc is still treated as an ordinary character (it is a valid identifier character).
  (https://github.com/ClickHouse/clickhouse-java/issues/3009)

- **[jdbc-v2]** Fixed `INSERT INTO [TABLE] FUNCTION f(...) VALUES (?)` failing with
  `Code: 60 ... does not exist. (UNKNOWN_TABLE)` when the `beta.row_binary_for_simple_insert` feature was enabled.
  Neither SQL parser reported a table-function insert target as a function, so the statement was routed to the
  `RowBinary` writer, which looked the function name (or a placeholder such as `unknown`) up as a table. Both parsers
  now report such a statement as using a function, so it stays on the regular SQL path; additionally the JavaCC grammar
  no longer mis-parses `INSERT INTO TABLE FUNCTION f(...)` by consuming
  `FUNCTION` as the table name. Inserts into a plain table are unaffected and still use the `RowBinary`
  writer. (https://github.com/ClickHouse/clickhouse-java/issues/3015)

- **[jdbc-v2]** Fixed `Connection#prepareStatement` throwing a `NullPointerException` for an
  `INSERT ... VALUES (...)` statement whose values list the default JavaCC parser cannot parse — most commonly one
  containing a heredoc string (`$$...$$`), which the grammar has no token for, but also any other unparsable token
  inside the list. The parser's error recovery left the values list's start position recorded without its matching end
  position, which was then unboxed unguarded. Both positions are now dropped together, so the driver falls back to its
  generic parameter-substitution path and such statements are prepared and executed successfully. The `ANTLR4`
  parser backends were not affected. (https://github.com/ClickHouse/clickhouse-java/issues/3013)

- **[client-v2]** Fixed reading a `JSON` or named `Tuple` value nested in a `Dynamic` column when a typed path or
  element name requires quoting (it contains a space, a comma or a bracket). Names read from the binary type encoding
  were appended to the reconstructed type name unquoted, so e.g. ``JSON(`a b` Int64)`` inside a `Dynamic` column
  produced a malformed type name and the whole query failed with `IllegalArgumentException: Unknown data type: b Int64`.
  Such names are now back-quoted (with inner back-quotes escaped) exactly as the server renders them, and `JSON` skip
  paths and path regexps are emitted with their `SKIP` / `SKIP REGEXP` markers. Names that need no quoting are rendered
  as before. Top-level `JSON` columns and `JSON` nested in `Map`/`Tuple`/`Array` were not affected — their type comes
  from the `RowBinaryWithNamesAndTypes` header, which the server already quotes.
  (https://github.com/ClickHouse/clickhouse-java/issues/3001)

- **[client-v2]** Fixed reading a `Variant`, `Nested`, `Decimal` or `Enum` value held in a `Dynamic` column. The
  concrete type rebuilt from the binary type encoding did not match what the server encoded: `Variant` was wrapped twice
  (so the discriminator selected the wrong element), `Nested` read only the element names and left the element type
  encodings in the stream, and `Decimal`/`Enum` lost their precision and scale / their constants whenever the value sat
  inside another type, so a decimal read back unscaled (`1.2500` as `12500`) and every enum value read back as
  `<unknown>`. The constant width of an enum is now taken from the type tag rather than from the number of constants,
  which also fixes reading an `Enum16` with fewer than 128 constants and negative `Enum8` constants.
  (https://github.com/ClickHouse/clickhouse-java/issues/3003)

- **[client-v2]** Fixed a `Nullable(T)` column bound to a **primitive** POJO field silently corrupting a row on the POJO
  read path. The compiled setter went straight to a primitive read method without consuming the `Nullable`
  null-marker byte, which is on the wire for every value of a nullable column regardless of the value, so the stream
  stayed shifted by one byte per row and the nullable column and every column after it decoded from the wrong offset
  without any error being raised. The generated setter now consumes the marker; a value that is actually `NULL` cannot
  be held by a primitive field and is reported with a `NullValueException`. Boxed POJO fields are unaffected.
  (https://github.com/ClickHouse/clickhouse-java/issues/2993)

- **[client-v2]** Fixed the `Native` format reader (`NativeFormatReader`) misreading `Array` columns in multi-row
  results whose rows have different lengths. Native encodes an array column as cumulative row offsets followed by the
  flattened elements, but the reader used the first row's offset as the element count for every row — truncating later
  rows and desyncing the columns that follow the array in the same block. Each row's length is now derived from the
  difference between consecutive offsets, and empty array rows (`len == 0`) no longer read a phantom element. Results
  with uniform array lengths were unaffected. (https://github.com/ClickHouse/clickhouse-java/issues/2955)

- **[jdbc-v2]** Fixed `SQLException#getSQLState()` returning the generic data-exception state `22000`
  when ClickHouse reports an unknown table. The driver now returns `42S02` (base table or view not found) while
  preserving the ClickHouse error code and original exception.
  (https://github.com/ClickHouse/clickhouse-java/issues/3104)

- **[client-v2]** Fixed truncated LZ4 stream errors reporting literal `{0}` and `{1}` placeholders instead of the number
  of bytes read and expected. (https://github.com/ClickHouse/clickhouse-java/issues/3108)

- **[jdbc-v2]** Fixed `DatabaseMetaData#getTables` reporting `TABLE_TYPE = TABLE` for a table with the `BigQuery`
  engine (present in `system.table_engines` since ClickHouse `26.8`). The engine was missing from the
  engine-to-table-type mapping, so it fell back to the default `TABLE`, and `getTables(..., types = {"REMOTE TABLE"})`
  returned no row for such a table. `BigQuery` is now mapped to `REMOTE TABLE`, like the other external-storage engines.
  (https://github.com/ClickHouse/clickhouse-java/issues/3049)

- **[jdbc-v2]** Fixed `PreparedStatement.getMetaData()` losing the result-set schema for a statement whose SQL contains
  a comment. The `DESCRIBE` query used to resolve the metadata was built by re-scanning the SQL with a regex that knew
  only quoted tokens, so a `?` inside a `--` / `#` / `/* */` comment was rewritten to `NULL` and an odd `'` inside a
  comment mis-paired the quote alternative, leaving a real placeholder unreplaced — the
  `DESCRIBE` then failed and the driver silently returned untyped metadata. The metadata query is now built from the
  placeholder positions the statement parser already computed, so it always matches the SQL that a parameterized
  execution produces. (https://github.com/ClickHouse/clickhouse-java/issues/3011)

- **[client-v2]** Fixed reading a `UInt64` column into a **primitive** POJO field (e.g. `long`) always failing with
  `ClassCastException: BinaryStreamReader cannot be cast to java.math.BigInteger`. The compiled setter for that
  combination never emitted a read call, so it cast the reader itself instead of a value and consumed nothing from the
  stream, which made every primitive field bound to a `UInt64` column unusable. The value is now read and converted to
  the target primitive with the matching `Number` accessor, which narrows a value that does not fit the same way a Java
  narrowing conversion does (a `long` holds every `UInt64` value bit-for-bit and can be read back with
  `Long.toUnsignedString(long)`; a `boolean` is `true` for any non-zero value). Boxed fields (`BigInteger`, `Long`) are
  unaffected. (https://github.com/ClickHouse/clickhouse-java/issues/2996)

- **[jdbc-v2]** Fixed `ResultSetMetaData.getPrecision()` and `getScale()` returning `0` for columns wrapped in
  `SimpleAggregateFunction(func, T)`. The wrapper is transparent on the read path (values are read as plain `T`), but
  both accessors described the wrapper itself, which carries no precision or scale — so a
  `SimpleAggregateFunction(sum, Decimal(18, 4))` column looked like a scale-0 value and
  `SimpleAggregateFunction(any, DateTime64(3, tz))` looked like second precision. They now describe the nested type.
  `AggregateFunction` columns are unchanged, since their values are aggregation states rather than values of the nested
  type. (https://github.com/ClickHouse/clickhouse-java/issues/3042)

- **[clickhouse-jdbc]** Fixed `Connection#prepareStatement` throwing a `NullPointerException` for an
  `INSERT ... VALUES (...)` statement whose values list the JavaCC parser cannot parse — most commonly one containing a
  heredoc string (`$$...$$`), which the grammar has no token for, but also any other unparsable token inside the list.
  The parser's error recovery left the values list's start position recorded without its matching end position, which
  was then unboxed unguarded. Both positions are now dropped together, so the driver falls back to its generic
  parameter-substitution path instead of failing, and a statement such as
  `insert into t values ($$a@b$$, ?)` is prepared and executed successfully.
  (https://github.com/ClickHouse/clickhouse-java/issues/3033)

- **[jdbc-v2]** Fixed the ANTLR4 lexer not nesting `/* */` block comments. ClickHouse (and the JavaCC parser backend)
  raise the nesting level on an inner `/*` and close the comment only at the matching `*/`, while the ANTLR4 lexer ended
  the comment at the first `*/` and lexed the rest of it as SQL. With the `ANTLR4` / `ANTLR4_PARAMS_PARSER` backends
  this made statements the server accepts (e.g. `SELECT 1 /* ) /* ) */ ) */, 2`) report syntax errors, and made
  `ANTLR4_PARAMS_PARSER` count a `?` inside the nested part of a comment as a bind parameter. Comments that do not nest
  are unaffected; an unterminated block comment is now skipped to the end of the statement instead of being lexed as
  stray tokens. (https://github.com/ClickHouse/clickhouse-java/issues/3021)

- **[client-v2]** Fixed reading a `SimpleAggregateFunction(func, T)` value held in a `Dynamic` column. The binary type
  encoding of such a value (`0x2E <function_name> <parameters> <arguments> <argument_type_encodings>`) was not consumed
  at all, so the read failed with `IndexOutOfBoundsException`, and the unconsumed encoding bytes would otherwise have
  been interpreted as row data and desynchronized the rest of the `RowBinary` stream. The concrete type is now
  reconstructed from the encoding and the value is read as its argument type `T`, so it reads exactly like the same
  value in a plain `SimpleAggregateFunction` column. (https://github.com/ClickHouse/clickhouse-java/issues/3005)

- **[jdbc-v2]** Fixed `PreparedStatement#executeBatch` sending a syntactically broken `INSERT` when an `ANTLR4` parser
  backend is selected (`jdbc_sql_parser=ANTLR4` / `ANTLR4_PARAMS_PARSER`) and the values list contains a value
  expression the bundled grammar cannot parse - a JDBC escape sequence (`{d '...'}`), or valid ClickHouse syntax the
  grammar does not cover such as a hex string literal (`hex(x'AB')`). Such a statement is still given a parse tree,
  completed by error recovery, and the values list positions and the value group count were read from it: the values
  list was reported to stop at the closing parenthesis of a nested function call, so the batch template lost its own
  closing parenthesis, and a two-group values list could be reported as a single group. Both are now discarded when the
  statement could not be parsed without errors, so the driver uses its generic parameter substitution path instead -
  and, with the beta `RowBinary` writer enabled, such a statement is no longer routed to it. The default `JAVACC`
  backend is not affected by this. (https://github.com/ClickHouse/clickhouse-java/issues/3019)

- **[jdbc-v2]** Fixed the ANTLR4 lexer rejecting `//` line comments, which the ClickHouse server and the driver's JavaCC
  grammar both accept. Because `/` is also the division operator, `// comment` was lexed as two operator tokens, so a
  statement containing a `//` comment was reported as a syntax error by the ANTLR4-based parser backends (`ANTLR4`,
  `ANTLR4_PARAMS_PARSER`), and an `INSERT` preceded by such a comment was misclassified as a statement with a result
  set. `//` is now skipped like `--`, `#` and `#!`; a single `/` and `//` inside a string literal or a quoted identifier
  are unaffected. Placeholder counting inside `//` comments for the backends that scan the raw SQL separately (`JAVACC`,
  `ANTLR4`) is fixed by
  https://github.com/ClickHouse/clickhouse-java/issues/3009. (https://github.com/ClickHouse/clickhouse-java/issues/3023)

- **[jdbc-v2]** Fixed `?` parameter placeholders being lost when `jdbc_sql_parser=ANTLR4_PARAMS_PARSER` is selected and
  the bundled grammar cannot match part of the statement - a JDBC escape sequence (`{d '...'}`), or valid ClickHouse
  syntax the grammar does not cover such as a hex string literal (`hex(x'AB')`). That backend read the placeholders only
  from the parse tree, and the tokens error recovery skips are not part of it, so a placeholder inside such an
  expression was dropped: `getParameterMetaData().getParameterCount()` was too low, `setXxx` for a dropped placeholder
  failed, and the remaining values were substituted at the wrong offsets. The placeholders are now re-derived from the
  original SQL when the statement could not be parsed without errors, as the other two backends always do.
  (https://github.com/ClickHouse/clickhouse-java/issues/3025)

- **[client-v2, jdbc-v2]** Fixed `Client.getTableSchema(...)`, `Client.getTableSchemaFromQuery(...)` and `ping()`
  failing against ClickHouse `26.8+`, where the `X-ClickHouse-Format` header the client sends wins over a `FORMAT`
  clause in the query. These internal queries now set their format in the settings instead of a `FORMAT` clause.
  (https://github.com/ClickHouse/clickhouse-java/issues/3068)

- **[jdbc-v2]** Fixed an `INSERT` whose values list holds a function call the bundled `ANTLR4` grammar cannot match -
  such as `hex(x'AB')`, valid ClickHouse the grammar has no hex string literal for - being reported to hold no function
  call when an `ANTLR4` parser backend is selected (`jdbc_sql_parser=ANTLR4` / `ANTLR4_PARAMS_PARSER`). Function calls
  in a values list are reported by a callback on the parse tree, and such a statement is still given a parse tree,
  completed by error recovery, which skips the tokens the parser recovered on - the function call among them. With the
  beta
  `RowBinary` writer enabled (`beta.row_binary_for_simple_insert=true`) the statement was then routed to it, where a
  literal function-call column cannot be written; it now takes the generic parameter substitution path, as it already
  did for a function call the grammar matches. Since such a parse tree cannot tell, any insert that could not be parsed
  without errors is now assumed to hold a function call in its values list, so none of them is written with the
  `RowBinary` writer. The default `JAVACC` backend is not affected by this.
  (https://github.com/ClickHouse/clickhouse-java/issues/3027)

- **[clickhouse-client]** Fixed JPMS/module-path service loading for `ClickHouseRequestManager` by loading client
  services from the `com.clickhouse.client` module, which declares the required `uses` directives. This avoids
  `ServiceConfigurationError` failures from `com.clickhouse.data` when applications run on the module path.
  (https://github.com/ClickHouse/clickhouse-java/issues/2669)

- **[client-v2]** Fixed `Client.cancelTransportRequest(queryId)` being silently dropped when it landed between two
  attempts of a retried operation (query, POJO insert and stream insert): the operation issued the next attempt anyway
  and could complete successfully. The request of an attempt now stays registered until the whole operation is over, and
  the cancellation is checked before every attempt, so a cancelled operation stops instead of sending another request.
  (https://github.com/ClickHouse/clickhouse-java/issues/2989)

- **[client-v1]** Fixed `BlockingPipedOutputStream.close()` not being idempotent under concurrency: the check of the
  `closed` flag and the closing handshake were not atomic, so two threads closing the same stream (e.g. a writer thread
  and a try-with-resources block) could both put the end-of-stream marker into the queue, and the second one failed with
  `Close stream timed out after <n> ms` once the reader had stopped consuming. Exactly one caller now performs the
  handshake and runs the post-close action; a concurrent or repeated `close()` returns immediately. A
  `close()` which fails while flushing the remaining data also marks the stream closed and runs the post-close action,
  so the stream cannot stay half-closed. (https://github.com/ClickHouse/clickhouse-java/issues/3055)

- **[client-v1]** Fixed `NonBlockingPipedOutputStream.close()` not being idempotent under concurrency. Two threads could both
  flush and mutate the same pending buffer before the reader consumed it, silently replacing the payload with an empty
  buffer and running the post-close action twice. Exactly one caller now flushes the pending data, enqueues the
  end-of-stream marker, and runs the post-close action; concurrent or repeated `close()` calls return immediately.
  (https://github.com/ClickHouse/clickhouse-java/issues/3057)

- **[jdbc-v2]** Fixed JDBC escape processing rewriting text inside string literals and quoted identifiers. Because
  `PreparedStatement` inlines bound parameters into the statement text, a bound value containing `{fn ` (or `{d '...'}`
  / `{ts '...'}`) was re-read as SQL syntax: the `{fn ` was removed together with the next `}` found anywhere in the
  statement — usually the closing brace of an unrelated `Map`/`Tuple` literal in another value or row — corrupting the
  inserted data or failing with a server-side `SYNTAX_ERROR`. Escape sequences are now recognized only outside of quoted
  text, and a `{fn ...}` escape is unwrapped at its matching closing brace, so nested braces (e.g. a `{name:Type}` query
  parameter or a nested escape) stay balanced. (https://github.com/ClickHouse/clickhouse-java/issues/2995)

- **[jdbc-v2]** Fixed prepared statements losing parameter markers after an empty `--` comment line or after
  `SELECT * EXCEPT (...)`, which caused parameter binding to fail with `ArrayIndexOutOfBoundsException` for the affected
  SQL parser backends. (https://github.com/ClickHouse/clickhouse-java/issues/3052)

- **[client-v2]** Fixed LZ4 input streams not closing their underlying HTTP response stream. Closing an LZ4 stream
  returned by `QueryResponse.getInputStream()` now releases the wrapped transport stream, including after a partial
  read. (https://github.com/ClickHouse/clickhouse-java/issues/2985)

- **[jdbc-v2]** Fixed the default JavaCC SQL parser aborting on a heredoc string (`$$body$$`, `$tag$body$tag$`)
  whose body contains a character that is not a valid SQL token on its own, such as `!`, `&`, `|` or `~`. The lexer had
  no heredoc token, so such a body raised a lexer error that left the statement classified as
  `UNKNOWN` — an INSERT was reported as a result-set-bearing statement with no table name and no values-list positions,
  which disables the batch values template and the table-name based paths. A heredoc is now lexed as a single string
  literal. (https://github.com/ClickHouse/clickhouse-java/issues/3029)

- **[client-v2, jdbc-v2]** Reduced noisy and potentially sensitive logging; SQL that fails to parse is no longer logged
  at `WARN` (it could contain credentials/PII). (https://github.com/ClickHouse/clickhouse-java/issues/2970)

- **[client-v2]** Fixed `BigDecimal` values written into a `Dynamic` column being silently truncated when the value's
  scale exceeded the inferred width's maximum scale, and throwing an overflow error when the value carried an integer
  part (e.g. `19.99`). The `Dynamic` type inference now sizes the `Decimal` width to hold both the integer digits and
  the value's scale, keeps the scale as wide as the width allows without stealing room from the integer part, and writes
  the actual column scale into the `Dynamic` type tag. Values that already round-tripped losslessly are unchanged.
  (https://github.com/ClickHouse/clickhouse-java/issues/2966)

- **[client-v2]** Fixed the `RowBinary` writer throwing
  `UnsupportedOperationException: Unsupported data type: SimpleAggregateFunction` when inserting into a
  `SimpleAggregateFunction(func, T)` column (the reader already supported these columns). The value is now serialized
  identically to its underlying type `T`, writing the `Nullable` null-marker byte when the underlying type is nullable
  (e.g. `SimpleAggregateFunction(anyLast, Nullable(String))`), mirroring the read path.
  (https://github.com/ClickHouse/clickhouse-java/issues/2477)

- **[client-v2]** Fixed the `Dynamic` type tag for a `SimpleAggregateFunction` type being written as a bare
  `0x2E` byte. The binary type encoding also carries the function name, its parameters and its argument types, so the
  server read the function name out of the value bytes that followed and failed with
  `ATTEMPT_TO_READ_AFTER_EOF`. Since the client never infers a `SimpleAggregateFunction` from a Java value and the
  reader cannot read one back out of a `Dynamic` column, this now fails fast with a clear
  `ClientException` instead of producing a corrupt `RowBinary` stream (the same treatment `QBit` already gets).
  (https://github.com/ClickHouse/clickhouse-java/issues/3007)

- **[client-v2, jdbc-v2]** Fixed several logging-layer defects. In `client-v2`, `HttpAPIClientHelper.shouldRetry`
  threw a `ClassCastException` when a retryable `ServerException` was wrapped as the *cause* of another exception (the
  branch matched on the cause but the cast used the outer exception); the retry decision is now taken from whichever
  exception is the `ServerException`. Also in `client-v2`, a failure to build the HTTP client version string is now
  logged at `WARN` with the throwable attached instead of a bare `INFO` message that discarded the cause. In `jdbc-v2`,
  a failure to close the response after a query error now logs the close failure itself instead of the
  already-propagated outer exception. (https://github.com/ClickHouse/clickhouse-java/issues/2968)

- **[client-v2]** Fixed scalar `String` query parameters containing a tab (`0x09`), newline (`0x0a`) or backslash being
  mishandled through the server's `param_<name>` interface. A `{name:String}`
  parameter value is parsed by the server with `deserializeTextEscaped`, which treated a raw tab or newline as a field
  delimiter (failing the query with `BAD_QUERY_PARAMETER: ... isn't parsed completely`)
  and a raw backslash as the start of an escape sequence (silently corrupting the value, e.g. `C:\temp`
  became `C:<tab>emp`). `Client.query(sql, params, ...)` now escapes the backslash, tab and newline in a scalar `String`
  parameter so any value round-trips; every other character the server reads verbatim — including the single quote and
  carriage return — is left unchanged, so `Identifier` values and pre-formatted `Array`/`Map` literals passed as a
  `String` still round-trip. The JDBC driver (`jdbc-v2`), which inlines parameters as SQL literals and already escaped
  the backslash and single quote, is unchanged and covered by a new regression test.
  (https://github.com/ClickHouse/clickhouse-java/issues/2781)

- **[client-v2]** Fixed a `null` query-parameter value being sent as the literal string `"null"`, so
  `Client.query(sql, params, ...)` binding a Java `null` to a scalar placeholder such as
  `{x:Nullable(Decimal128(8))}` was rejected by the server with `BAD_QUERY_PARAMETER`
  (`Value null cannot be parsed as Nullable(...)`). A top-level scalar `null` is now sent as the ClickHouse
  `\N` NULL sentinel so it binds SQL `NULL`; a `null` nested inside an `Array`/`Map` parameter value continues to render
  as the SQL `NULL` keyword. (https://github.com/ClickHouse/clickhouse-java/issues/2977)

- **[client-v2]** Fixed binary array decoding for nullable element types so `Array(Nullable(Float64))` and similar
  columns now return boxed arrays such as `Double[]` instead of `Object[]`. This keeps null-supporting arrays aligned
  with their element type while preserving the existing `Object[]` fallback for Variant/Dynamic/Geometry arrays.
  (https://github.com/ClickHouse/clickhouse-java/issues/2846)

- **[client-v2]** Fixed `Float32`/`Float64` columns throwing `ClassCastException` when a value of a non-matching boxed
  numeric type was supplied through the `Object`-typed insert surface — for example a
  `Double` (the natural type of Java literal like `1.5`) for a `Float32` column, or a `Float` for a
  `Float64` column. The `RowBinary` serializer now narrows any `Number` (and, like the `Int*` columns, a `String`/
  `Boolean`) through `Number#floatValue()`/`Number#doubleValue()`, so the float columns accept the same value types the
  integer columns already did. (https://github.com/ClickHouse/clickhouse-java/issues/2930)

- **[client-v2]** Fixed a `NullPointerException` when serializing a `null` value into a non-nullable
  `Enum8`/`Enum16` column. `SerializerUtils.serializeEnumData` had no `null` guard, so a `null` in a non-nullable enum
  column reached `value.getClass()` and failed the RowBinary insert path with a confusing NPE instead of a clear error.
  It now throws `IllegalArgumentException` naming the column, consistent with the existing `IllegalArgumentException`
  for other unsupported enum values. Nullable enum columns are unaffected.
  (https://github.com/ClickHouse/clickhouse-java/issues/2931)

- **[client-v2]** Fixed silent data corruption when serializing a Java `null` into a non-nullable
  `Array(...)` column via `RowBinaryFormatWriter`. `RowBinaryFormatSerializer.writeValuePreamble`
  special-cased `Array`, emitting a stray marker byte on top of the array length; the server read the extra byte as a
  phantom extra row (single-column inserts) or as a column shift that failed the insert with `CANNOT_READ_ALL_DATA`
  (multi-column inserts). A non-nullable `Array` cannot represent a `null`, so it now throws `IllegalArgumentException`
  naming the column — consistent with every other non-nullable type — in both the `RowBinary` and
  `RowBinaryWithDefaults` paths. Empty arrays (`[]`) still serialize correctly, and `Dynamic` columns, which can hold a
  `null` as the implicit `Nothing` type, are unaffected. (https://github.com/ClickHouse/clickhouse-java/issues/2938)

- **[client-v2]** Fixed POJO insert error classification so transport write failures such as java.net.SocketException:
  Broken pipe (Write failed) are now surfaced as transfer/network errors instead of being wrapped as
  DataSerializationException. This only changes the exception type reported for request-body transport failures during
  `Client.insert (...);` actual POJO reflection/serialization failures are still reported as DataSerializationException.
  (https://github.com/ClickHouse/clickhouse-java/issues/2729)

- **[client-v2]** Fixed binary varint decoding for length and count fields so overflowing or overlong values fail with
  an `IOException` instead of being decoded into corrupted or negative `int` values.
  (https://github.com/ClickHouse/clickhouse-java/issues/2902)

- **[client-v2]** Fixed container query parameters being sent unquoted, so `Client.query(sql, params, settings)` binding
  a `List<LocalDate>` (or an array/`Map`) to a placeholder like `{ids:Array(Date)}` was rejected by the server with
  `CANNOT_PARSE_INPUT_ASSERTION_FAILED`. Parameter values are now formatted by
  `DataTypeConverter#convertParameterToString(Object)` before being sent: pass the raw Java value and the client renders
  it into the text the server's `param_<name>` interface expects — a `Collection`, array (object or primitive), or `Map`
  becomes ClickHouse `Array` (`['2026-05-13']`) / `Map` (`{'k':'v'}`) text with `String`/temporal leaves single-quoted
  and numeric/boolean leaves left unquoted, while a scalar is passed through unquoted as before. No manual
  pre-formatting of container parameters is needed. (https://github.com/ClickHouse/clickhouse-java/issues/2897)

- **[client-v2]** Fixed `DateTime`/`DateTime64` columns declared with a synthetic fixed-offset timezone name
  (`Fixed/UTC±HH:MM:SS`, e.g. `Fixed/UTC+05:30:00`) being silently read in UTC instead of the declared offset. The
  `RowBinary` reader now recovers the offset from the column's declared type.
  (https://github.com/ClickHouse/clickhouse-java/issues/2876)

- **[jdbc-v2]** Fixed the ANTLR4 SQL parser backends (`jdbc_sql_parser=ANTLR4` and `ANTLR4_PARAMS_PARSER`) lexing the
  body of a heredoc string (`$$body$$`, `$tag$body$tag$`) as ordinary SQL. The lexer had no heredoc token, so every `$`
  was dropped as an unrecognized character and the body was parsed as identifiers, operators and statement separators: a
  body that still looked like valid SQL was silently mis-parsed (wrong table name and VALUES-list positions), and a body
  containing `;` — as well as the empty heredoc `$$$$` — was reported as a parse error, which classifies an INSERT as a
  result-set-bearing statement with no values-list positions. A heredoc is now lexed as a single string literal and
  accepted as a literal value (`INSERT ... VALUES` lists, column expressions, settings), and a `$` inside an identifier
  (`a$b`, or an unterminated tag such as
  `$foo$bar`) is part of the identifier, as the server reads it.
  (https://github.com/ClickHouse/clickhouse-java/issues/3031)

- **[jdbc-v2]** Fixed the beta RowBinary writer (`DriverProperties.BETA_ROW_BINARY_WRITER`) throwing
  `NoSuchColumnException` for `INSERT` statements whose column names are backtick-quoted, in particular the canonical
  `Nested` sub-column wire form `` `directory`.`id` ``. The SQL parser now unescapes each backtick-quoted `INSERT`
  column-name component before the by-name server-schema lookup, matching how the table and database identifiers are
  already handled. (https://github.com/ClickHouse/clickhouse-java/issues/2896)

## Documentation 

- **[examples]** - New `demo-spring-service` added to demonstrate usage of observability SPI.

## Dependencies

- **[client-v1,client-v2]** Upgraded `org.apache.httpcomponents.client5:httpclient5` from `5.4.4` to `5.6.4` in `client-v2` and
  `clickhouse-http-client` to pick up the fixes of the newer 5.x releases, including known vulnerabilities.
  (https://github.com/ClickHouse/clickhouse-java/issues/3078)

- **[client-v2]** Upgraded `org.bouncycastle:bcprov-jdk18on` from `1.84` to `1.85` in `client-v2` (https://github.com/ClickHouse/clickhouse-java/pull/3141).

