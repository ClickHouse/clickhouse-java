# Client V2 JSON Processors Example

## Overview

This standalone example shows how to consume a `JSONEachRow` response with the
`client-v2` `JSONEachRowFormatReader` and a `JsonParserFactory`. The two
factories shipped with the client are the customization points:

- `JacksonJsonParserFactory` exposes a `protected ObjectMapper createMapper()`
  hook — override it to return a fully configured `ObjectMapper` (modules,
  feature flags, custom deserializers, etc.).
- `GsonJsonParserFactory` exposes a `protected void customize(GsonBuilder)`
  hook — override it to configure the `GsonBuilder` (number policy, type
  adapters, etc.). The factory still applies `setLenient()` on its own
  afterwards, which is required for the stream-of-objects shape of
  `JSONEachRow`.

The example is structured as a small component:

- `ClientV2JsonProcessorsExample(Client client)` holds the shared `Client` and
  exposes regular instance methods (`recreateTable()`, `loadSampleData()`,
  `readAll(label, factory)`, `run()`), so the class can be copied as-is into
  another project and have its individual methods invoked.
- Sample rows are kept in a plain `Object[][]` constant, separate from the
  SQL, so the read path stays focused on the parser factory.
- Two small subclasses, `CustomJacksonParserFactory` and
  `CustomGsonParserFactory`, demonstrate the protected-hook customization.
  Both also implement a tiny `PayloadConverter` interface defined inside the
  example: their configured `ObjectMapper` / `Gson` is reused to convert the
  raw `payload` `Map` produced by the underlying library into a typed
  `Payload` POJO. The default factories do not implement the interface, so
  `readAll(...)` logs the raw map for them — making the contrast between the
  default behavior and the customized behavior visible in the output.

Each read call in `run()` follows the same three-step shape:

1. **Create the factory** — `new JacksonJsonParserFactory()` /
   `new GsonJsonParserFactory()` for defaults, or an instance of a custom
   subclass.
2. **Customize if needed** — only inside the subclass, by overriding the
   protected hook.
3. **Execute** — `readAll(label, factory)` runs the `SELECT` and feeds the
   response stream through
   `new JSONEachRowFormatReader(factory.createJsonParser(...))`.

## Requirements

- JDK 17 or newer
- A running ClickHouse server reachable from the machine running the example
- A locally installed `client-v2` snapshot from this repository

## How to Run

From this directory:

```shell
gradle run
```

Connection properties can be supplied as system properties:

- `-DchEndpoint` — endpoint to connect to (default: `http://localhost:8123`)
- `-DchUser` — ClickHouse user name (default: `default`)
- `-DchPassword` — ClickHouse user password (default: empty)
- `-DchDatabase` — ClickHouse database name (default: `default`)

Example with custom connection properties:

```shell
gradle run \
  -DchEndpoint=http://localhost:8123 \
  -DchUser=default \
  -DchPassword= \
  -DchDatabase=default
```

## Executable Example

`com.clickhouse.examples.client_v2.json_processors.ClientV2JsonProcessorsExample`

Steps performed by `run()`:

1. `recreateTable()` — drops and re-creates `client_v2_json_processors_example`
   with primitive columns and one `payload JSON` column.
2. `loadSampleData()` — inserts the rows from the `SAMPLE_ROWS` array as a
   single batched `INSERT`.
3. `readAll(...)` is invoked four times, each time with a different
   `JsonParserFactory`:
   - default `JacksonJsonParserFactory`;
   - `CustomJacksonParserFactory`, which overrides `createMapper()` to
     tolerate unknown properties and preserve big integers and decimals
     exactly, and implements `PayloadConverter` to convert each row's
     `payload` `Map` into a `Payload` POJO via
     `ObjectMapper.convertValue(...)`;
   - default `GsonJsonParserFactory`;
   - `CustomGsonParserFactory`, which overrides `customize(GsonBuilder)` to
     use a `LONG_OR_DOUBLE` number policy and disable HTML escaping, and
     implements `PayloadConverter` to convert each row's `payload` `Map`
     into a `Payload` POJO via `gson.fromJson(gson.toJsonTree(...))`.

Logged rows include the payload value's runtime class so the difference
between the default factories (which surface a `LinkedHashMap` /
`LinkedTreeMap`) and the customized factories (which surface a `Payload`)
shows up directly in the output.

The build keeps both `jackson-databind` and `gson` on the classpath so the
example can switch between processors at runtime. Production applications
only need to keep the processor they actually use.
