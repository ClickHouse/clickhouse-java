# `client-v2` and `jdbc-v2` Features

This document lists stable, user-visible behavior in `client-v2` and `jdbc-v2` that should be considered during review and regression testing.

Use it as a compact checklist:

- Confirm the change does not remove or alter any listed feature unintentionally.
- Add or update tests when a change touches a listed feature in a compatibility-sensitive way.
- Update this document when a new user-visible feature is added or when supported behavior changes intentionally.

## `client-v2`

- HTTP and HTTPS connectivity: Connects to ClickHouse over HTTP(S), supports endpoint paths, and exposes a basic `ping` health check.
- TLS configuration: Supports trust stores, client certificates/keys, SSL certificate authentication, and SNI for HTTPS connections.
- Authentication modes: Supports username/password credentials, ClickHouse auth headers, bearer tokens, and optional HTTP Basic authentication.
- Proxy support: Can send requests through configured HTTP proxies, including proxy credentials.
- Connection and socket tuning: Exposes pool sizing, keep-alive, reuse strategy, connect/request/socket timeouts, and low-level socket options.
- Query execution: Executes SQL asynchronously and returns streaming query responses with response metadata and metrics.
- Query settings: Supports per-query database selection, output format, execution limits, roles, log comments, headers, server settings, and network timeout overrides.
- Parameterized SQL: Accepts named query parameters and can send them through supported HTTP request encodings.
- Result materialization helpers: Provides streaming `Records`, generic row access, and convenience APIs that materialize all rows into generic records or typed POJOs.
- Binary format readers: Reads ClickHouse binary result formats including `Native`, `RowBinary`, `RowBinaryWithNames`, and `RowBinaryWithNamesAndTypes`.
- Data type conversion: Maps ClickHouse types to Java values for binary reads, POJO binding, and SQL parameter formatting, including date/time handling.
- Insert APIs: Supports inserting registered POJOs, raw streams, and callback-driven writers, with optional column lists and format selection.
- Insert controls: Supports insert-specific settings such as deduplication token, query id, compression behavior, and request headers.
- Command execution: Executes DDL or other non-result commands and exposes response summaries and operation metrics.
- Metadata discovery: Loads table schemas from table names or queries and allows schema registration for typed read/write operations.
- Server information loading: Can refresh server version, current user, and server time zone information.
- Compression support: Supports response compression, ClickHouse LZ4 request/response compression, HTTP content compression, and caller-supplied precompressed insert bodies.
- Retry behavior: Can retry failed operations for configured failure causes and retry limits.
- Metrics and observability: Exposes client/server operation metrics and optionally integrates connection-pool gauges with Micrometer.
- Configuration surface: Supports arbitrary client options, cookies, custom headers, server-setting prefixes, client naming, query id suppliers, and buffer sizing.
- SQL helpers: Includes SQL quoting and temporal formatting helpers used by callers building SQL text safely.

Compatibility-sensitive traits:

- Named parameter typing is part of the contract: placeholders are written as `{name:Type}` and the supplied value must match the expected ClickHouse textual representation for that type.
- String query parameters are expected to round-trip correctly for ordinary text, Unicode, slashes, dashes, and leading or trailing spaces.
- String escaping behavior in `SQLUtils` is compatibility-sensitive: `enquoteLiteral()` uses SQL-style doubled single quotes, while `escapeSingleQuotes()` escapes both backslashes and single quotes with backslashes.
- Identifier quoting behavior is stable API for helper callers: identifiers are double-quoted, embedded double quotes are doubled, and optional quoting keeps simple identifiers unchanged.
- Instant formatting is type-sensitive and should not drift: `Date` formatting depends on an explicit timezone, `DateTime` is serialized as epoch seconds, and higher-precision timestamps preserve up to 9 fractional digits.
- Timezone conversion helpers preserve nanoseconds and can intentionally shift local date or time when interpreted in a different timezone; this behavior is covered by tests and should not be normalized away.


## `jdbc-v2`

- JDBC driver registration: Registers through the standard JDBC service mechanism and is available through `DriverManager`.
- JDBC URL parsing: Accepts `jdbc:clickhouse:` and `jdbc:ch:` URLs with host, port, optional HTTP path, optional database, and query parameters.
- SSL URL support: Supports HTTPS connections through URL and property configuration, including default protocol and port handling.
- Driver and client properties: Separates JDBC-specific properties from passthrough client options used by the underlying `client-v2` transport.
- DataSource support: Provides a JDBC `DataSource` implementation backed by the same driver configuration model.
- Connection lifecycle: Supports connection close, validity checks, ping-based health checks, and network timeout management.
- Schema and database context: Supports database selection through URL, `setSchema`, `USE`, and statement-level settings.
- Non-transactional operation: Exposes ClickHouse-appropriate transaction behavior with auto-commit semantics and unsupported transactional features.
- Statement execution: Supports `execute`, `executeQuery`, `executeUpdate`, large update counts, and forward-only/read-only statements.
- Query cancellation and timeout: Supports JDBC query timeout handling and query cancellation through server-side `KILL QUERY`.
- Batch execution: Supports batched statements and prepared-statement batches, including multi-row rewrite for eligible `INSERT ... VALUES` statements.
- Prepared statements: Supports `?` parameters through client-side SQL rendering and validates that all parameters are bound before execution.
- SQL parsing and classification: Classifies SQL to distinguish queries, updates, inserts, `USE`, and role-changing statements, with selectable parser backends.
- JDBC escape processing: Translates supported JDBC escape syntax for dates, timestamps, and functions before execution.
- Result set streaming: Streams result sets from ClickHouse binary formats, enforces max-row limits, and manages result-set lifecycle correctly.
- Result-set metadata: Exposes JDBC `ResultSetMetaData` backed by ClickHouse column schema.
- Database metadata: Implements JDBC `DatabaseMetaData` for ClickHouse catalogs, schemas, tables, columns, and related capability reporting.
- Parameter metadata: Reports prepared-statement parameter counts.
- Type mapping and conversions: Maps ClickHouse types to JDBC types and Java classes, including date/time handling and `java.time` support.
- Arrays and tuples: Supports JDBC arrays plus ClickHouse tuple values through custom `Array` and `Struct` implementations.
- Client info propagation: Supports JDBC client info such as `ApplicationName` and forwards it to the underlying client name.
- Wrapper support: Implements standard JDBC `Wrapper` and `unwrap` behavior on major JDBC objects.
- Packaging and runtime compatibility: Ships as a JDBC 4.2 driver, depends on `client-v2`, and includes native-image metadata for GraalVM users.

Compatibility-sensitive traits:

- Prepared statements are client-side SQL rendering, not server-side prepared statements. Changes to literal encoding or placeholder parsing are externally visible behavior.
- String parameters are escaped with backslash-based escaping: backslashes are doubled and single quotes are backslash-escaped before values are wrapped in single quotes.
- `?` placeholder detection is SQL-aware and should not treat question marks inside quoted strings, quoted identifiers, comments, casts, or similar syntax as bind parameters.
- String-like ClickHouse values have stable JDBC expectations: `String`, `FixedString`, and `Enum` values are returned as strings, while `UUID` is available both as `getString()` and `getObject(..., UUID.class)`.
- Binary parameters passed through `setBytes()` are encoded as ClickHouse `unhex(...)` expressions rather than text literals; empty byte arrays map to an empty string expression.
- Stream and reader setters (`setAsciiStream`, `setUnicodeStream`, `setBinaryStream`, `setCharacterStream`, `setNCharacterStream`) are treated as text input encoded with the same string-escaping rules, including length-based truncation when a length is supplied.
- `getString()` formatting for temporal values is stable output: `Date` uses `yyyy-MM-dd`, `DateTime` uses `yyyy-MM-dd HH:mm:ss`, and `DateTime64` preserves fractional precision, all interpreted in server timezone context where applicable.
- Date and timestamp setters with `Calendar` are timezone-sensitive by design. Preserving the current day-shift and instant-preserving behavior is important for compatibility.
- `setObject()` temporal behavior is specific and should not drift: `LocalDateTime` and `Instant` are rendered through `fromUnixTimestamp64Nano(...)`, while `Timestamp` and `Date` use quoted textual forms.
