# JSONEachRow Support in `client-v2` and `jdbc-v2`

This document specifies the `JSONEachRow` output-format support introduced in
`client-v2` and exposed through `jdbc-v2`. It defines the public API,
configuration properties, runtime dependencies, type mapping, and current
limitations.

## Motivation

ClickHouse provides several JSON-oriented column types (`JSON`, `Variant`,
`Dynamic`) and structured types (`Array`, `Tuple`, `Map`). When such values
are returned through binary formats they are commonly materialized as
serialized strings, which requires every caller to embed and configure its own
JSON parser and complicates the propagation of nested objects through JDBC.

`JSONEachRow` is the row-oriented JSON output format of ClickHouse: each row
is emitted as a self-contained JSON object separated by line breaks. With the
appropriate server settings, numeric values are emitted as JSON numbers and
nested objects are preserved without additional encoding. Supporting this
format directly in the Java clients enables:

- materializing columns of type `JSON` as `Map<String, Object>` instances;
- preserving nested objects, arrays, and tuples without an additional parsing
  step;
- exposing structured JSON payloads through the standard JDBC `ResultSet`
  contract.

Combining `JSONEachRow` output with a pluggable Jackson or Gson parser
provides additional advantages beyond what the format alone delivers:

- **Streaming row parsing.** Jackson's `JsonParser` and Gson's `JsonReader`
  consume the response stream incrementally. `JSONEachRowFormatReader`
  materializes one row at a time, so peak memory consumption is bounded by
  the size of the current row rather than by the size of the result set.
- **Reuse of an existing JSON dependency on the classpath.** Applications
  that already depend on Jackson or Gson for unrelated purposes can instantiate
  the matching `JsonParserFactory` and avoid contributing a second JSON library
  to the runtime classpath. Only the library JARs are shared; the default
  factories use their own `ObjectMapper` or `Gson` instance unless callers
  provide a subclass with custom settings.
- **Choice between processors.** Jackson and Gson are selected independently
  and can be swapped at deployment time. Applications may pick the processor
  that best matches their existing classpath and operational constraints,
  without changing application code that consumes the reader or the JDBC
  `ResultSet`.

## Scope and non-goals

This section defines, in one place, what the JSONEachRow support is built
to do and what it intentionally leaves out.

### The problem

`JSONEachRow` itself is a simple line-oriented format, but mapping its
contents to Java values is not. JSON has fewer primitive types than
ClickHouse, applications disagree on how large integers, decimals, dates,
and other domain values should surface in Java, and the two mainstream
JSON libraries (Jackson and Gson) make different default choices for many
of these mappings. Bundling a single fixed JSON-to-Java mapping in the
client would force every application into one library's conventions and
would re-implement work the application's existing JSON dependency
already does.

At the same time, application code that already consumes the binary
readers should not have to fork into a separate read loop just because
the result is in a text format.

### Goals

1. Provide a row-by-row reader over `JSONEachRow` that delegates parsing
   and per-token type decisions to whichever JSON library the application
   already runs.
2. Let applications customize that mapping through the chosen library
   (Jackson modules / features, Gson `TypeAdapter`s, number policies,
   custom domain types) by extending the bundled factory and overriding
   its single protected hook.
3. Expose a single accessor surface (`ClickHouseFormatReader`) shared
   with the binary readers so caller code can be format-agnostic.
4. Keep the parser SPI (`JsonParser`, `JsonParserFactory`) narrow and
   library-agnostic so applications with parser requirements outside
   Jackson or Gson can plug in their own implementation without changes
   to `client-v2`.

### What is covered

- Streaming row parsing of `JSONEachRow` through a pluggable
  `JsonParserFactory`. Each row materializes as `Map<String, Object>`;
  leaf value types are whatever the configured library chose.
- Bundled `JacksonJsonParserFactory` and `GsonJsonParserFactory` with
  sensible defaults and one protected hook each for customization
  (`createMapper()` and `customize(GsonBuilder)`).
- A common reader hierarchy — `ClickHouseFormatReader` for the shared
  accessor surface, `ClickHouseTextFormatReader` as the text-format
  family marker, and `JSONEachRowFormatReader` as the concrete reader.
- An opt-in server-setting bundle that disables JSON number quoting for
  `JSONEachRow` requests (see
  [JSON number output settings](#json-number-output-settings)).
- JDBC integration: `FORMAT JSONEachRow` is accepted by
  `Statement.executeQuery(...)` in `jdbc-v2`, with parser-factory
  selection through the `jdbc_json_parser_factory` connection property.

### What is not covered

- **Library-native row objects.** The reader does not expose Jackson's
  `JsonNode` or Gson's `JsonElement` for the current row, and there is
  no `currentRowAsObject` / `currentRowAsString` accessor on the reader
  interface. The `Map<String, Object>` view is the contract.
  Applications that need the library-native representation should
  implement their own `JsonParserFactory` / `JsonParser` that retains
  the native object internally and exposes it through their own
  accessor — the SPI is intentionally narrow so this kind of extension
  lives in application code rather than in `client-v2`.
- **Uniform cross-library semantics.** The reader does not normalize Java
  type choices across libraries. Jackson and Gson disagree on, for
  example, whether an unquoted JSON integer larger than `2^53` is
  materialized as an integral `Number` or as a `Double`. The reader
  respects whatever the configured library chose; consistent behavior
  across processors is the application's responsibility, via factory
  customization or post-processing.
- **A built-in JSON tokenizer.** The reader does not include its own
  parser. At least one of the bundled libraries (Jackson or Gson) must
  be on the runtime classpath, or the application must supply its own
  `JsonParserFactory`, for `JSONEachRow` support to function.
- **Full accessor parity with the binary readers.** Several advanced
  accessors (temporal/inet/geo/bitmap families) are not implemented for
  `JSONEachRow` and throw `UnsupportedOperationException`. See
  [Typed accessors](#typed-accessors) for the current list. Callers
  needing these conversions should read the value through
  `readValue(...)` / `getList(...)` / `getString(...)` and convert at
  the application boundary, or use the binary default format where
  these accessors are natively supported.

## Summary of changes

`client-v2`:

- Introduces a common `com.clickhouse.client.api.data_formats.ClickHouseFormatReader`
  interface that declares all row navigation, schema access, and typed
  accessors. The pre-existing `ClickHouseBinaryFormatReader` becomes a
  format-family sub-interface for binary output formats and inherits its
  full method set unchanged from `ClickHouseFormatReader`.
- Adds `com.clickhouse.client.api.data_formats.ClickHouseTextFormatReader`,
  a sibling sub-interface for text output formats.
- Adds `com.clickhouse.client.api.data_formats.JSONEachRowFormatReader`,
  which implements `ClickHouseTextFormatReader` over a streaming JSON
  parser.
- `Client.newBinaryFormatReader(...)` continues to construct the binary
  readers (`Native`, `RowBinary`, `RowBinaryWithNames`,
  `RowBinaryWithNamesAndTypes`) and rejects text formats with
  `IllegalArgumentException`. `JSONEachRow` callers construct
  `JSONEachRowFormatReader` directly from a `JsonParser`.
- Introduces a JSON parser SPI under
  `com.clickhouse.client.api.data_formats`, consisting of `JsonParser`,
  `JsonParserFactory`, `JacksonJsonParserFactory`, and
  `GsonJsonParserFactory`.
- Adds an opt-in client flag for `JSONEachRow` requests that asks ClickHouse
  to emit large integers, floats, and decimals as plain JSON numbers (see
  [JSON number output settings](#json-number-output-settings)).
- Declares Jackson and Gson as `provided` Maven dependencies, so that
  applications must include the chosen processor on their own classpath.

`jdbc-v2`:

- Modifies `StatementImpl.executeQuery(...)` to accept `JSONEachRow` as a
  valid output format. All other text formats remain unsupported.
- Adds `DriverProperties.JSON_PARSER_FACTORY`
  (key `jdbc_json_parser_factory`) for selecting the `JsonParserFactory`
  implementation by fully-qualified class name.
- Declares Jackson and Gson as `provided` dependencies, consistent with
  `client-v2`.

Two runnable examples are included in the repository:
`examples/client-v2-json-processors` and `examples/jdbc-v2-json-processors`.

## Public API

### `ClickHouseFormatReader`, `ClickHouseBinaryFormatReader`, `ClickHouseTextFormatReader`

```java
package com.clickhouse.client.api.data_formats;

public interface ClickHouseFormatReader extends AutoCloseable { ... }

public interface ClickHouseBinaryFormatReader extends ClickHouseFormatReader { }

public interface ClickHouseTextFormatReader extends ClickHouseFormatReader { }
```

`ClickHouseFormatReader` is the common contract for row-by-row format
readers regardless of the underlying wire encoding. The two sub-interfaces
specialize that contract by output-format family: callers receive a
`ClickHouseBinaryFormatReader` when the response is in a binary format and a
`ClickHouseTextFormatReader` when it is in a text format. All accessor
methods declared today live on the common parent; future format-specific
extensions are expected to be added on the corresponding sub-interface
without changing the shared surface, so code written against
`ClickHouseBinaryFormatReader` continues to compile against the same
inherited methods.

### `JSONEachRowFormatReader`

```java
package com.clickhouse.client.api.data_formats;

public class JSONEachRowFormatReader implements ClickHouseTextFormatReader { ... }
```

The reader is instantiated from an `InputStream`-backed `JsonParser`. Callers
usually create that parser through `JacksonJsonParserFactory`,
`GsonJsonParserFactory`, or an application subclass of one of those factories.

`JSONEachRow` is a text format, so the reader implements
`ClickHouseTextFormatReader`. Callers that need to handle both binary and
text readers uniformly can program against `ClickHouseFormatReader`.

### `JsonParser` SPI

```java
package com.clickhouse.client.api.data_formats;

public interface JsonParser extends AutoCloseable {
    Map<String, Object> nextRow() throws Exception;
}
```

Two factories are provided:

- `JacksonJsonParserFactory` uses `com.fasterxml.jackson.core` and
  `com.fasterxml.jackson.databind` to stream JSON objects.
- `GsonJsonParserFactory` uses `com.google.gson` with a lenient `JsonReader`,
  which accepts a sequence of top-level JSON objects separated by whitespace,
  as produced by `JSONEachRow`.

`JsonParserFactory.createJsonParser(InputStream)` creates a parser for each
response stream. The factory may hold reusable parser configuration, but the
returned `JsonParser` is request-scoped and owns the stream reader it creates.

The shipped implementations construct their own `ObjectMapper` and `Gson`
instances with default settings. To customize the underlying library
(Jackson modules, feature flags, Gson `TypeAdapter`s, number policies, etc.)
extend the corresponding factory and override its protected hook:

- `JacksonJsonParserFactory` exposes `protected ObjectMapper createMapper()`.
  Override it to return a fully configured `ObjectMapper`; the factory uses
  the returned instance for all subsequent row parsing.
- `GsonJsonParserFactory` exposes `protected void customize(GsonBuilder
  builder)`. Override it to configure the `GsonBuilder` before the factory
  applies `setLenient()` and calls `build()`.

Customization that does not need to influence the underlying parser can
still be performed on the caller side, after the row has been materialized
as `Map<String, Object>`.

### JDBC parser factory property

`jdbc-v2` selects the parser factory through
`DriverProperties.JSON_PARSER_FACTORY`:

| Property key              | Value |
| ------------------------- | ----- |
| `jdbc_json_parser_factory` | Fully-qualified class name implementing `JsonParserFactory` |

The named class is loaded reflectively when the connection is created and must
have a public no-argument constructor. There is no equivalent `client-v2`
configuration key; direct client users pass a factory instance to their own
reader construction code.

### JSON number quoting flag

`client-v2` can opt in to numeric JSON output settings through
`ClientConfigProperties.JSON_DISABLE_NUMBER_QUOTING`:

| Property key                                  | Default | Effect |
| --------------------------------------------- | ------- | ------ |
| `json_disable_number_quoting`                 | `false` | When `true` and the resolved request format is `JSONEachRow`, sets `output_format_json_quote_64bit_integers=0`, `output_format_json_quote_64bit_floats=0`, and `output_format_json_quote_decimals=0` for that request. |

The flag can be set on the client builder or on a specific `QuerySettings`
instance. It does not change `output_format_json_quote_denormals`.

## Runtime dependencies

`client-v2` and `jdbc-v2` declare the JSON libraries with `provided` scope,
so that they are not contributed to the runtime classpath of applications
that do not require them. Applications must add the JSON library used by the
selected factory to their runtime classpath:

- Jackson — `com.fasterxml.jackson.core:jackson-databind`,
  `com.fasterxml.jackson.core:jackson-core`,
  `com.fasterxml.jackson.core:jackson-annotations`
  (required when using `JacksonJsonParserFactory`); or
- Gson — `com.google.code.gson:gson`
  (required when using `GsonJsonParserFactory`).

The repository builds against Jackson `2.17.2` and Gson `2.10.1`. The parser
implementations rely only on the streaming token API and a single
`Map<String, Object>` materialization call, so other recent versions of
either library are expected to be compatible.

When the selected JSON library is not present on the classpath, construction
or first use of the corresponding factory fails with the dependency-loading
error raised by the JVM or the JSON library.

## Usage in `client-v2`

```java
Client client = new Client.Builder()
        .addEndpoint("http://localhost:8123")
        .setUsername("default")
        .setPassword("")
        .setDefaultDatabase("default")
        .serverSetting("allow_experimental_json_type", "1")
        .build();

JsonParserFactory parserFactory = new JacksonJsonParserFactory();

QuerySettings settings = new QuerySettings()
        .setFormat(ClickHouseFormat.JSONEachRow)
        .setOption(ClientConfigProperties.JSON_DISABLE_NUMBER_QUOTING.getKey(), true);

try (QueryResponse response = client.query(
        "SELECT id, name, active, score, payload FROM events ORDER BY id",
        settings).get();
     ClickHouseTextFormatReader reader = new JSONEachRowFormatReader(
             parserFactory.createJsonParser(response.getInputStream()))) {
    while (reader.next() != null) {
        int id              = reader.getInteger("id");
        String name         = reader.getString("name");
        boolean active      = reader.getBoolean("active");
        double score        = reader.getDouble("score");
        Map<String, Object> payload = reader.readValue("payload"); // JSON column
        // ...
    }
}
```

Notes:

- Set `ClickHouseFormat.JSONEachRow` in `QuerySettings`. Do not rely on an SQL
  `FORMAT JSONEachRow` clause for direct `client-v2` examples when you also
  want client-side JSON number output settings, because those settings are
  applied only when the request settings identify the format as `JSONEachRow`.
- `client.newBinaryFormatReader(response)` continues to return a
  `ClickHouseBinaryFormatReader` for binary output formats and rejects text
  formats such as `JSONEachRow` with `IllegalArgumentException`. Callers that
  need to handle both can program against the shared `ClickHouseFormatReader`
  parent interface.
- `Map<String, Object>` is the canonical materialization for JSON columns
  and for the row itself, as produced by the selected library. JSON arrays
  are returned as `List<Object>`; nested JSON objects are returned as nested
  `Map<String, Object>` instances. The exact Java types of leaf values are
  whatever Jackson or Gson chose during parsing.

## Usage in `jdbc-v2`

### Document-oriented JSON output

The standard ClickHouse `FORMAT JSON` response is a single JSON document that
contains metadata, data, row counts, and statistics together. It is not mapped
to a JDBC `ResultSet` by `Statement.executeQuery(...)`; callers that need the
server-rendered JSON should use the underlying `client-v2` instance exposed by
the JDBC connection and consume the `QueryResponse` stream directly.

```java
try (Connection conn = DriverManager.getConnection(
        "jdbc:clickhouse://localhost:8123/default", props);
     QueryResponse response = conn.unwrap(ConnectionImpl.class)
             .getClient()
             .query("SELECT 1 AS x FORMAT JSON")
             .get();
     BufferedReader reader = new BufferedReader(new InputStreamReader(
             response.getInputStream(), StandardCharsets.UTF_8))) {
    String json = reader.lines().collect(Collectors.joining("\n"));
    // consume the JSON document
}
```

The returned `Client` is owned by the JDBC connection. Close each
`QueryResponse`, as shown above, but do not close the client returned by
`ConnectionImpl#getClient()`.

### Row-oriented JSONEachRow output

The output format is selected by appending `FORMAT JSONEachRow` to the SQL
statement. The driver does not rewrite the SQL and does not apply a default
format on the caller's behalf.

```java
Properties props = new Properties();
props.setProperty("user", "default");
props.setProperty("password", "");
props.setProperty(DriverProperties.JSON_PARSER_FACTORY.getKey(),
        JacksonJsonParserFactory.class.getName());
// The JSON column type is experimental on the server side.
props.setProperty(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
props.setProperty(ClientConfigProperties.serverSetting("output_format_json_quote_64bit_integers"), "0");
props.setProperty(ClientConfigProperties.serverSetting("output_format_json_quote_64bit_floats"), "0");
props.setProperty(ClientConfigProperties.serverSetting("output_format_json_quote_decimals"), "0");

try (Connection conn = DriverManager.getConnection(
        "jdbc:clickhouse://localhost:8123/default", props);
     Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery(
        "SELECT id, name, active, score, payload "
      + "FROM events ORDER BY id FORMAT JSONEachRow")) {

    while (rs.next()) {
        int id          = rs.getInt("id");
        String name     = rs.getString("name");
        boolean active  = rs.getBoolean("active");
        double score    = rs.getDouble("score");
        Object payload  = rs.getObject("payload"); // Map / List / scalar
    }
}
```

Behavior:

- When `FORMAT JSONEachRow` is not specified, `jdbc-v2` continues to use the
  binary default. `StatementImpl` accepts only `RowBinaryWithNamesAndTypes`
  and `JSONEachRow` as output formats; any other text format causes
  `SQLException("Only RowBinaryWithNameAndTypes and JSONEachRow are supported
  for output format. ...")` to be thrown.
- `ResultSet.getObject(...)` returns parser-native `Map`, `List`, and scalar
  values without an additional string round-trip. JSON arrays are returned as
  the `List` implementation produced by the selected JSON library. Because
  `JSONEachRow` has no array element metadata, `ResultSet.getArray(...)` is
  not supported for these inferred JSON arrays.
- Temporal typed JDBC accessors follow the current `JSONEachRowFormatReader`
  text-accessor support. `ResultSet.getString(...)` can be used to read the
  server-formatted temporal text, but `getTimestamp(...)`,
  `getObject(..., Timestamp.class)`, and related temporal conversions are not
  guaranteed for `FORMAT JSONEachRow` result sets. Use the binary default
  format when JDBC temporal typed accessors are required, or read the value as
  a string/object and convert it in application code.
- The JSON processor is selected at the connection level through the
  `jdbc_json_parser_factory` driver property. It cannot be changed per
  statement, in line with the lifecycle of other connection options.
- Because JDBC selects `JSONEachRow` through SQL text, set the JSON output
  server settings explicitly as connection properties when integer or decimal
  numeric accessors are used.

## JSON number output settings

`Client.applyFormatSpecificSettings(...)` runs after request settings have
been merged and after the request format has been resolved. When the format
is `JSONEachRow` and
`ClientConfigProperties.JSON_DISABLE_NUMBER_QUOTING` is enabled, the
following server-side settings are set to `0` for the request:

| Setting                                   | Value | Rationale                                                                  |
| ----------------------------------------- | ------------ | -------------------------------------------------------------------------- |
| `output_format_json_quote_64bit_integers` | `0`   | Emits `Int64` and `UInt64` as JSON numbers rather than quoted strings.     |
| `output_format_json_quote_64bit_floats`   | `0`   | Emits 64-bit floating-point values as JSON numbers.                        |
| `output_format_json_quote_decimals`       | `0`   | Emits decimals as JSON numbers, allowing materialization as `BigDecimal` or `Double`. |

These overrides are scoped to the individual request and apply only when both
conditions are true: the request format in `QuerySettings` is `JSONEachRow`,
and `json_disable_number_quoting` is enabled through the client
or request settings. Explicit server settings are otherwise preserved.

Denormal floating-point values (`NaN`, `Inf`, `-Inf`) are not yet handled by
the built-in JSON reader. The client does not set
`output_format_json_quote_denormals`; keep the server default or set
`output_format_json_quote_denormals=1` so these values are quoted, then handle
them as strings at the application boundary.

JDBC callers that use SQL `FORMAT JSONEachRow` should set the same numeric
server settings explicitly through connection properties when integer or
decimal numeric accessors are used.

## Row parsing, schema, and typed accessors

### Row parsing is delegated to the chosen library

The reader does not implement its own JSON parser. Each row is materialized
by the configured library:

- the Jackson backend calls `ObjectMapper.readValue(parser, Map.class)` on
  Jackson's streaming `JsonParser`;
- the Gson backend calls `gson.fromJson(reader, TypeToken<Map<String, Object>>)`
  on a lenient Gson `JsonReader`.

The result of each call is a `Map<String, Object>` whose values have the
runtime Java types chosen by the library for the parsed JSON tokens —
typically `Number` (for example `Integer`, `Long`, `Double`, `BigDecimal`),
`Boolean`, `String`, `List<Object>` for JSON arrays, nested
`Map<String, Object>` for JSON objects, and `null` for JSON `null`. Numeric
representation, widening rules, handling of large integers, and any other
JSON-to-Java decisions are governed entirely by the library. The reader
neither inspects raw JSON tokens nor overrides the library's parsing
behavior.

### Integer precision with Gson

ClickHouse `Int64` and `UInt64` values can exceed the exactly representable
integer range of a JSON floating-point number. When
`json_disable_number_quoting` is enabled, the client asks
ClickHouse to emit them as JSON numbers for `JSONEachRow`, so the selected
JSON library's number materialization policy matters.

Jackson's default `Map.class` materialization keeps ordinary integer tokens as
integer `Number` implementations. Gson's default `Map<String, Object>`
materialization can surface JSON numbers as floating-point values, which may
round integers larger than `2^53` before `getLong(...)` sees them.

If you use Gson and need integer precision, provide a custom factory that
configures Gson's object number strategy:

```java
public final class PreciseGsonJsonParserFactory extends GsonJsonParserFactory {
    @Override
    protected void customize(GsonBuilder builder) {
        builder.setObjectToNumberStrategy(com.google.gson.ToNumberPolicy.LONG_OR_DOUBLE);
    }
}
```

Use that factory directly with `client-v2`:

```java
JsonParserFactory parserFactory = new PreciseGsonJsonParserFactory();
```

For JDBC, put the factory class name in the connection properties:

```java
props.setProperty(DriverProperties.JSON_PARSER_FACTORY.getKey(),
        PreciseGsonJsonParserFactory.class.getName());
```

`ToNumberPolicy.LONG_OR_DOUBLE` preserves values that fit in `long` as
`Long`. If exact decimal representation is more important than returning
`Long` for integer tokens, use `ToNumberPolicy.BIG_DECIMAL` and convert
explicitly at the application boundary.

### Minimal schema discovery

`JSONEachRow` does not include a schema header. To populate a minimal
`TableSchema` for the typed accessors, the reader inspects the **Java
types** of the first row's values, after the library has produced them, and
maps each to a `ClickHouseDataType`:

| Java type produced by the library                                      | Inferred ClickHouse type |
| ---------------------------------------------------------------------- | ------------------------ |
| `Integer`                                                              | `Int32`                  |
| `Long`                                                                 | `Int64`                  |
| `BigInteger`                                                           | `Int256`                 |
| `Float`                                                                | `Float32`                |
| `Double`                                                               | `Float64`                |
| `BigDecimal`                                                           | `Decimal`                |
| `Boolean`                                                              | `Bool`                   |
| `List` or Java array                                                   | `Array`                  |
| `Map`                                                                  | `Map`                    |
| Any other value (`String`, `null`, unsupported number subtypes, ...)   | `String`                 |

Implications:

- Schema discovery is performed once, on the first row. Empty result sets
  produce a schema with no columns.
- Column names are taken verbatim from the JSON keys of the first row, in
  iteration order.
- The discovered schema is intended only to support the typed accessors
  (`getInteger`, `getString`, and so on). Server-side column metadata such
  as precision, nullability, and codec is not reconstructed.
- Whether a JSON number is materialized as `Integer`, `Long`, `Double`, or
  `BigDecimal` is a property of the chosen library, not of the reader.
  Applications that need a specific numeric representation should select
  the processor whose default behavior matches their expectations.

### Typed accessors

The typed accessors declared on the read interface are implemented as
follows:

| Accessor                                      | Behavior                                                      |
| --------------------------------------------- | ------------------------------------------------------------- |
| `readValue` / `next`                          | Returns the row as a `Map<String, Object>`.                   |
| `getString`                                   | Returns `Object#toString()` of the JSON value, or `null`.     |
| `getByte` / `getShort` / `getInteger` / `getLong` / `getFloat` / `getDouble` | Casts through `Number`.        |
| `getBoolean`                                  | Accepts `Boolean`, non-zero `Number`, or parses a string.     |
| `getBigInteger` / `getBigDecimal`             | Routes through `BigDecimal(String)`.                          |
| `getLocalDate` / `getLocalTime` / `getLocalDateTime` / `getOffsetDateTime` | Uses the corresponding `parse(...)` method on the string value. |
| `getUUID`                                     | Uses `UUID.fromString(...)` on the string value.              |
| `getList`                                     | Returns the JSON array as `List<T>`.                          |
| `getTuple`                                    | Returns the row value cast to `Object[]`.                     |
| `getEnum8` / `getEnum16`                      | Delegates to `getByte` / `getShort`.                          |

Accessor limitations to keep in mind:

- `getTuple(...)` does not adapt parser-native `List` or `Map` values. Since
  JSON arrays are usually materialized as `List` and JSON objects as `Map`,
  callers should use `readValue(...)` for tuple-like JSON values and convert
  them explicitly.

The following accessors are not supported by the current implementation and
throw `UnsupportedOperationException`:

- `getInstant`, `getZonedDateTime`, `getDuration`, `getTemporalAmount`;
- `getInet4Address`, `getInet6Address`;
- `getGeoPoint`, `getGeoRing`, `getGeoPolygon`, `getGeoMultiPolygon`;
- the typed array accessors `getByteArray`, `getIntArray`, `getLongArray`,
  `getFloatArray`, `getDoubleArray`, `getBooleanArray`, `getShortArray`,
  `getStringArray`, `getObjectArray`;
- `getClickHouseBitmap`.

For these types, callers should obtain the parsed value through
`readValue(...)` or `getList(...)` and convert it explicitly.

## Streaming and lifetime

- `JacksonJsonParserFactory` and `GsonJsonParserFactory` delegate parsing to the
  underlying library and consume one row at a time from the response
  `InputStream`. Memory consumption is proportional to the size of the
  current row and is independent of the size of the result set.
- `JSONEachRowFormatReader` reads the first row eagerly during construction
  in order to inspect the Java types of its values and populate the minimal
  `TableSchema` described above. For empty result sets, the reader exposes
  an empty `TableSchema`, and `hasNext()` returns `false`.
- `close()` is propagated to the underlying parser, which closes the input
  stream it owns. Callers are responsible for closing the originating
  `QueryResponse` (or JDBC `ResultSet`).

## Compatibility considerations

- The parser factory classes are additive. Applications that do not request
  `JSONEachRow` and do not instantiate or configure a parser factory are
  unaffected.
- The default request format is unchanged. The existing binary readers
  (`Native`, `RowBinary`, `RowBinaryWithNames`, `RowBinaryWithNamesAndTypes`)
  retain their previous behavior.
- The reader hierarchy now distinguishes binary and text formats:
  `ClickHouseBinaryFormatReader` and `ClickHouseTextFormatReader` are sibling
  sub-interfaces of the new `ClickHouseFormatReader`. The accessor surface is
  unchanged; callers that hold a `ClickHouseBinaryFormatReader` reference for
  binary formats are unaffected. `Client.newBinaryFormatReader(...)` rejects
  `JSONEachRow` with `IllegalArgumentException`; construct
  `JSONEachRowFormatReader` directly for JSONEachRow streams.
- Jackson and Gson are now declared with `provided` scope in `client-v2` and
  `jdbc-v2`. Applications that previously inherited Jackson transitively from
  these modules in `test` scope must declare the chosen processor explicitly
  on their runtime classpath.
- `jdbc_json_parser_factory` is a new JDBC driver property and is only needed
  by connections that execute `FORMAT JSONEachRow` queries.

## Examples

Two runnable Gradle examples are provided under `examples/`:

- `examples/client-v2-json-processors` exercises the `client-v2` API
  directly, switching between Jackson and Gson factories against a shared sample
  table that contains primitive columns and one `payload JSON` column.
  Entry point: `ClientV2JsonProcessorsExample`.
- `examples/jdbc-v2-json-processors` performs the same flow through the JDBC
  driver, with `FORMAT JSONEachRow` appended to the `SELECT` statement and
  parser factory selection applied through connection properties. Entry point:
  `JdbcV2JsonProcessorsExample`.

Both examples include a sample dataset under
`src/main/resources/sample_data.csv` and require a running ClickHouse server
with `allow_experimental_json_type=1`.

## Tests

- `client-v2/src/test/java/com/clickhouse/client/api/data_formats/AbstractJSONEachRowFormatReaderTests.java`
  defines a parameterized integration test executed for both processors via
  the subclasses `JacksonJSONEachRowFormatReaderTests` and
  `GsonJSONEachRowFormatReaderTests`. The suite covers basic parsing, schema
  inference, primitive type accessors, and empty result sets.
- `jdbc-v2/src/test/java/com/clickhouse/jdbc/StatementTest.java` adds
  `testJSONEachRowFormat`, which exercises
  `Statement.executeQuery("... FORMAT JSONEachRow")` through the JDBC driver
  against both parser factories.
