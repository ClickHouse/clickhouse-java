# JDBC V2 JSON Processors Example

## Overview

This standalone example shows how to consume `FORMAT JSONEachRow` responses
through `jdbc-v2` with the two factories shipped under
`com.clickhouse.client.api.data_formats`:

- `JacksonJsonParserFactory`
- `GsonJsonParserFactory`

### How the JDBC driver selects a factory

The driver picks the parser factory **by fully-qualified class name** from
the `JSON_PARSER_FACTORY` driver property (key: `jdbc_json_parser_factor`).
The value is the FQN of a class that implements `JsonParserFactory`; the
driver loads it reflectively and instantiates it through a **public no-arg
constructor**. There is no enum-style selector.

Selection is **connection-level**: the factory cannot be swapped on an
existing connection. The driver instantiates the named class once during
connection creation and reuses that instance for every `JSONEachRow`
response served by the connection.

### Customization is done by extending the factory

Because the driver instantiates the named class with a no-arg constructor,
customization cannot be expressed as constructor arguments. The supported
approach is:

1. Subclass `JacksonJsonParserFactory` or `GsonJsonParserFactory` in your
   own code.
2. Override the protected hook:
   - `protected ObjectMapper createMapper()` on `JacksonJsonParserFactory` —
     return any fully-configured `ObjectMapper` (modules, feature flags,
     deserializers).
   - `protected void customize(GsonBuilder builder)` on
     `GsonJsonParserFactory` — configure the `GsonBuilder` (number policy,
     `TypeAdapter`s, date format, ...). The factory still applies
     `setLenient()` on its own afterwards, which is required for
     `JSONEachRow`.
3. Set `JSON_PARSER_FACTORY` to the FQN of the subclass.

This example carries both subclasses as `public static final` nested classes
inside `JdbcV2JsonProcessorsExample` (`CustomJacksonParserFactory` and
`CustomGsonParserFactory`). The example feeds their FQNs to the driver via
`factoryClass.getName()`, which for nested classes returns the
`Outer$Inner` binary form — accepted by `Class.forName(...)` and by the
driver. If you set `JSON_PARSER_FACTORY` manually (e.g. from a config file
or JDBC URL) and your subclass is nested, you must use the same `$`-form;
top-level classes use the ordinary dot-separated FQN.

Both custom subclasses also implement a tiny `PayloadConverter` interface
defined inside the example: their configured `ObjectMapper` / `Gson` is
reused to convert the raw `payload` value produced by the underlying
library into a typed `Payload` POJO. Because the JDBC driver only exposes
the factory through the connection (not as a Java object), `readAll(...)`
detects the interface on the factory **class** and instantiates its own
converter via the same public no-arg constructor the driver uses. The
default factories do not implement the interface, so `readAll(...)` logs
the raw map for them — making the contrast between the default behavior
and the customized behavior visible in the output.

### Component shape

`JdbcV2JsonProcessorsExample` is written as a small component:

- `JdbcV2JsonProcessorsExample(String url, String user, String password)`
  holds the connection settings and exposes regular instance methods
  (`recreateTable()`, `loadSampleData()`, `readAll(label, factoryClass)`,
  `run()`), so the class can be copied as-is into another project and have
  its individual methods invoked.
- Sample rows are kept in a plain `Object[][]` constant, separate from the
  SQL, so the read path stays focused on the parser-factory wiring.

Each read call in `run()` follows the same three-step shape:

1. **Pick a factory class** — `JacksonJsonParserFactory.class` /
   `GsonJsonParserFactory.class` for defaults, or one of the nested custom
   subclasses.
2. **Customize if needed** — only inside the subclass, by overriding the
   protected hook.
3. **Execute** — `readAll(label, factoryClass)` opens a fresh connection
   with `JSON_PARSER_FACTORY=<FQN>`, runs the `SELECT ... FORMAT JSONEachRow`
   and iterates the `ResultSet`.

Because JDBC selects `JSONEachRow` through SQL text, set the JSON output
server settings explicitly on the connection when numeric accessors are used:

```java
props.setProperty(ClientConfigProperties.serverSetting("output_format_json_quote_64bit_integers"), "0");
props.setProperty(ClientConfigProperties.serverSetting("output_format_json_quote_64bit_floats"), "0");
props.setProperty(ClientConfigProperties.serverSetting("output_format_json_quote_denormals"), "0");
props.setProperty(ClientConfigProperties.serverSetting("output_format_json_quote_decimals"), "0");
```

## Integer Precision

ClickHouse 64-bit integers can be larger than the exact integer range of a
JSON floating-point number. Jackson's default map materialization preserves
ordinary integer tokens as integer `Number` values. Gson's default
`Map<String, Object>` materialization may surface numbers as floating-point
values, which can round large integers before `ResultSet.getLong(...)` sees
them.

For Gson, extend `GsonJsonParserFactory` and configure the object number
strategy:

```java
public final class PreciseGsonJsonParserFactory extends GsonJsonParserFactory {
    @Override
    protected void customize(GsonBuilder builder) {
        builder.setObjectToNumberStrategy(com.google.gson.ToNumberPolicy.LONG_OR_DOUBLE);
    }
}
```

Then configure JDBC with the factory class name:

```java
props.setProperty(DriverProperties.JSON_PARSER_FACTORY.getKey(),
        PreciseGsonJsonParserFactory.class.getName());
```

The included `CustomGsonParserFactory` uses this pattern. Use
`ToNumberPolicy.BIG_DECIMAL` instead when exact decimal representation matters
more than receiving integer tokens as `Long`.

## Requirements

- JDK 17 or newer
- A running ClickHouse server reachable from the machine running the example
- A locally installed `jdbc-v2` snapshot from this repository

## How to Run

From this directory:

```shell
gradle run
```

Connection properties can be supplied as system properties:

- `-DchUrl` — JDBC URL (default: `jdbc:clickhouse://localhost:8123/default`)
- `-DchUser` — ClickHouse user name (default: `default`)
- `-DchPassword` — ClickHouse user password (default: empty)

Example with custom connection properties:

```shell
gradle run \
  -DchUrl=jdbc:clickhouse://localhost:8123/default \
  -DchUser=default \
  -DchPassword=
```

## Executable Example

`com.clickhouse.examples.jdbc_v2.json_processors.JdbcV2JsonProcessorsExample`

Steps performed by `run()`:

1. `recreateTable()` — drops and re-creates `jdbc_v2_json_processors_example`
   with primitive columns and one `payload JSON` column.
2. `loadSampleData()` — inserts the rows from the `SAMPLE_ROWS` array as a
   single batched `INSERT`.
3. `readAll(...)` is invoked four times, each time pointing
   `JSON_PARSER_FACTORY` at a different class:
   - `JacksonJsonParserFactory` — default Jackson;
   - `JdbcV2JsonProcessorsExample.CustomJacksonParserFactory` — nested
     subclass overriding `createMapper()` to tolerate unknown properties
     and preserve big integers and decimals exactly, also implementing
     `PayloadConverter` to convert each row's `payload` `Map` into a
     `Payload` POJO via `ObjectMapper.convertValue(...)`;
   - `GsonJsonParserFactory` — default Gson;
   - `JdbcV2JsonProcessorsExample.CustomGsonParserFactory` — nested
     subclass overriding `customize(GsonBuilder)` to use a `LONG_OR_DOUBLE`
     number policy and disable HTML escaping, also implementing
     `PayloadConverter` to convert each row's `payload` `Map` into a
     `Payload` POJO via `gson.fromJson(gson.toJsonTree(...))`.

Logged rows include the payload value's runtime class so the difference
between the default factories (which surface a `LinkedHashMap` /
`LinkedTreeMap`) and the customized factories (which surface a `Payload`)
shows up directly in the output.

The build keeps both `jackson-databind` and `gson` on the classpath so the
example can switch between processors at runtime. Production applications
only need to keep the processor they actually use.
