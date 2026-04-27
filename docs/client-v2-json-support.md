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
  that already depend on Jackson or Gson for unrelated purposes can select
  the matching processor through `JSON_PROCESSOR` and avoid contributing a
  second JSON library to the runtime classpath. Only the library JARs are
  shared; the reader uses its own default `ObjectMapper` or `Gson` instance
  and does not pick up the application's configured modules, `TypeAdapter`s,
  or other customizations.
- **Choice between processors.** Jackson and Gson are selected independently
  and can be swapped at deployment time. Applications may pick the processor
  that best matches their existing classpath and operational constraints,
  without changing application code that consumes the reader or the JDBC
  `ResultSet`.

## Summary of changes

`client-v2`:

- Adds `com.clickhouse.client.api.data_formats.JSONEachRowFormatReader`,
  which implements `ClickHouseBinaryFormatReader` over a streaming JSON
  parser.
- Extends `Client.newBinaryFormatReader(...)` to construct the reader when
  `QuerySettings.getFormat() == ClickHouseFormat.JSONEachRow`.
- Introduces an internal JSON parser SPI under
  `com.clickhouse.client.api.data_formats.internal`, consisting of
  `JsonParser`, `JsonParserFactory`, `JacksonJsonParser`, and
  `GsonJsonParser`.
- Adds the client option `ClientConfigProperties.JSON_PROCESSOR`
  (key `json_processor`), with default value `JACKSON` and accepted values
  `JACKSON` and `GSON`.
- Forces a fixed set of server settings for `JSONEachRow` requests so that
  the response stream contains plain JSON numbers (see
  [Forced server settings](#forced-server-settings-for-jsoneachrow)).
- Declares Jackson and Gson as `provided` Maven dependencies, so that
  applications must include the chosen processor on their own classpath.

`jdbc-v2`:

- Modifies `StatementImpl.executeQuery(...)` to accept `JSONEachRow` as a
  valid output format. All other text formats remain unsupported.
- Forwards the `json_processor` option to the underlying `client-v2`
  configuration; it is configured through `Properties` or the JDBC URL.
- Declares Jackson and Gson as `provided` dependencies, consistent with
  `client-v2`.

Two runnable examples are included in the repository:
`examples/client-v2-json-processors` and `examples/jdbc-v2-json-processors`.

## Public API

### `JSONEachRowFormatReader`

```java
package com.clickhouse.client.api.data_formats;

public class JSONEachRowFormatReader implements ClickHouseBinaryFormatReader { ... }
```

The reader is normally instantiated by `Client.newBinaryFormatReader(...)`.
The class is public so that callers can construct it from any
`InputStream`-backed `JsonParser`.

`JSONEachRow` is a text format, but the reader currently implements
`ClickHouseBinaryFormatReader` so that existing call sites — including
`Client.newBinaryFormatReader(...)` — accept JSON output without changes.
The interface name is therefore not a precise fit for this format; a
dedicated reader interface for non-binary formats is intended to replace
this arrangement in a future release. Callers should treat the class itself
as the stable API and avoid relying on the "binary" label of the interface.

### `JsonParser` SPI

```java
package com.clickhouse.client.api.data_formats.internal;

public interface JsonParser extends AutoCloseable {
    Map<String, Object> nextRow() throws Exception;
}
```

Two implementations are provided:

- `JacksonJsonParser` uses `com.fasterxml.jackson.core` and
  `com.fasterxml.jackson.databind` to stream JSON objects.
- `GsonJsonParser` uses `com.google.gson` with a lenient `JsonReader`,
  which accepts a sequence of top-level JSON objects separated by whitespace,
  as produced by `JSONEachRow`.

`JsonParserFactory.createParser(String type, InputStream)` selects an
implementation based on the value of the `JSON_PROCESSOR` option.
Implementations are loaded reflectively. When the requested implementation is
unavailable, the factory throws a `RuntimeException` whose message identifies
the missing dependency.

The shipped implementations construct their own `ObjectMapper` and `Gson`
instances with default settings. Injecting a pre-configured parser, custom
Jackson modules, or Gson `TypeAdapter`s is not part of the public API in the
current release: neither parser exposes a constructor that accepts a
configured library instance, the `JsonParser` SPI itself resides in an
`internal` package and is not a stable extension point, and
`JsonParserFactory` does not accept caller-supplied implementation classes.
Customization of JSON binding should therefore be performed on the caller
side, after the row has been materialized as `Map<String, Object>`.

### `JSON_PROCESSOR` configuration property

Defined in `ClientConfigProperties`:

| Property key      | Default   | Accepted values   |
| ----------------- | --------- | ----------------- |
| `json_processor`  | `JACKSON` | `JACKSON`, `GSON` |

The same property is used by the `client-v2` builder
(`ClientConfigProperties.JSON_PROCESSOR.getKey()`) and by the JDBC driver,
where it is supplied through `Properties` or the JDBC URL query string.

## Runtime dependencies

`client-v2` and `jdbc-v2` declare the JSON libraries with `provided` scope,
so that they are not contributed to the runtime classpath of applications
that do not require them. Applications must add exactly one of the following
to their runtime classpath:

- Jackson — `com.fasterxml.jackson.core:jackson-databind`,
  `com.fasterxml.jackson.core:jackson-core`,
  `com.fasterxml.jackson.core:jackson-annotations`
  (required when `JSON_PROCESSOR=JACKSON`, the default); or
- Gson — `com.google.code.gson:gson`
  (required when `JSON_PROCESSOR=GSON`).

The repository builds against Jackson `2.17.2` and Gson `2.10.1`. The parser
implementations rely only on the streaming token API and a single
`Map<String, Object>` materialization call, so other recent versions of
either library are expected to be compatible.

When the configured processor is not present on the classpath at the time a
`JSONEachRow` reader is constructed, `JsonParserFactory` throws a
`RuntimeException` with the following message:

```text
JSON processor class not found: com.clickhouse.client.api.data_formats.internal.JacksonJsonParser.
Make sure you have the required library (Jackson or Gson) on your classpath.
```

## Usage in `client-v2`

```java
Client client = new Client.Builder()
        .addEndpoint("http://localhost:8123")
        .setUsername("default")
        .setPassword("")
        .setDefaultDatabase("default")
        .serverSetting("allow_experimental_json_type", "1")
        .setOption(ClientConfigProperties.JSON_PROCESSOR.getKey(), "JACKSON") // or "GSON"
        .build();

QuerySettings settings = new QuerySettings()
        .setFormat(ClickHouseFormat.JSONEachRow);

try (QueryResponse response = client.query(
        "SELECT id, name, active, score, payload FROM events ORDER BY id",
        settings).get()) {

    ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
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

- The reader is constructed only when the request format is `JSONEachRow`.
  The default request format remains `RowBinaryWithNamesAndTypes`, and
  callers that do not explicitly opt in are not affected.
- `client.newBinaryFormatReader(response)` returns a reader matching
  `response.getFormat()`; the same call site applies to both binary and JSON
  output.
- `Map<String, Object>` is the canonical materialization for JSON columns
  and for the row itself, as produced by the selected library. JSON arrays
  are returned as `List<Object>`; nested JSON objects are returned as nested
  `Map<String, Object>` instances. The exact Java types of leaf values are
  whatever Jackson or Gson chose during parsing.

## Usage in `jdbc-v2`

The output format is selected by appending `FORMAT JSONEachRow` to the SQL
statement. The driver does not rewrite the SQL and does not apply a default
format on the caller's behalf.

```java
Properties props = new Properties();
props.setProperty("user", "default");
props.setProperty("password", "");
props.setProperty(ClientConfigProperties.JSON_PROCESSOR.getKey(), "JACKSON"); // or "GSON"
// The JSON column type is experimental on the server side.
props.setProperty(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");

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
- `ResultSet.getObject(...)` returns the value produced directly by the
  selected JSON parser (`Map`, `List`, or scalar), without an additional
  string round-trip.
- The JSON processor is selected at the connection level through the
  `json_processor` option. It cannot be changed per statement, in line with
  the lifecycle of other client options.

## Forced server settings for `JSONEachRow`

`Client.applyFormatSpecificSettings(...)` runs after request settings have
been merged and after the request format has been resolved. When the format
is `JSONEachRow`, the following server-side settings are forced for the
request:

| Setting                                   | Forced value | Rationale                                                                  |
| ----------------------------------------- | ------------ | -------------------------------------------------------------------------- |
| `output_format_json_quote_64bit_integers` | `0`          | Emits `Int64` and `UInt64` as JSON numbers rather than quoted strings.     |
| `output_format_json_quote_64bit_floats`   | `0`          | Emits 64-bit floating-point values as JSON numbers.                        |
| `output_format_json_quote_denormals`      | `0`          | Avoids quoting `NaN` and `Inf`, allowing materialization as `Double`.      |
| `output_format_json_quote_decimals`       | `0`          | Emits decimals as JSON numbers, allowing materialization as `BigDecimal` or `Double`. |

These overrides are scoped to the individual request and apply only when the
request format is `JSONEachRow`. They are required for the typed accessors of
the reader to operate correctly and must not be overridden by callers.

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

### Minimal schema discovery

`JSONEachRow` does not include a schema header. To populate a minimal
`TableSchema` for the typed accessors, the reader inspects the **Java
types** of the first row's values, after the library has produced them, and
maps each to a `ClickHouseDataType`:

| Java type produced by the library                                      | Inferred ClickHouse type |
| ---------------------------------------------------------------------- | ------------------------ |
| `Integer`, `Long`, `BigInteger`                                        | `Int64`                  |
| `Double`, `Float`, `BigDecimal` whose value has no fractional part within the `long` range | `Int64` |
| Other `Number` subtypes                                                | `Float64`                |
| `Boolean`                                                              | `Bool`                   |
| Any other value (`String`, `List`, `Map`, `null`, ...)                 | `String`                 |

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

- `JacksonJsonParser` and `GsonJsonParser` delegate parsing to the
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

- The `JSON_PROCESSOR` property is additive. Applications that do not request
  `JSONEachRow` and do not set this property are unaffected.
- The default request format is unchanged. The existing binary readers
  (`Native`, `RowBinary`, `RowBinaryWithNames`, `RowBinaryWithNamesAndTypes`)
  retain their previous behavior.
- Jackson and Gson are now declared with `provided` scope in `client-v2` and
  `jdbc-v2`. Applications that previously inherited Jackson transitively from
  these modules in `test` scope must declare the chosen processor explicitly
  on their runtime classpath.
- The previously undocumented `json_processor` driver property has been
  removed from `DriverProperties`. The same value is now configured as a
  regular client option; the runtime behavior is unchanged.

## Examples

Two runnable Gradle examples are provided under `examples/`:

- `examples/client-v2-json-processors` exercises the `client-v2` API
  directly, switching between `JACKSON` and `GSON` against a shared sample
  table that contains primitive columns and one `payload JSON` column.
  Entry point: `ClientV2JsonProcessorsExample`.
- `examples/jdbc-v2-json-processors` performs the same flow through the JDBC
  driver, with `FORMAT JSONEachRow` appended to the `SELECT` statement and
  the same `JACKSON` / `GSON` selection applied through connection
  properties. Entry point: `JdbcV2JsonProcessorsExample`.

Both examples include a sample dataset under
`src/main/resources/sample_data.csv` and require a running ClickHouse server
with `allow_experimental_json_type=1`.

## Tests

- `client-v2/src/test/java/com/clickhouse/client/api/data_formats/AbstractJSONEachRowFormatReaderTests.java`
  defines a parameterized integration test executed for both processors via
  the subclasses `JacksonJSONEachRowFormatReaderTests` and
  `GsonJSONEachRowFormatReaderTests`. The suite covers basic parsing, schema
  inference, primitive type accessors, and empty result sets.
- `jdbc-v2/src/test/java/com/clickhouse/jdbc/StatementTest.java` adds the
  test methods `testJSONEachRowFormat` and `testJSONEachRowFormatGson`,
  which exercise `Statement.executeQuery("... FORMAT JSONEachRow")` through
  the JDBC driver against both processors.
