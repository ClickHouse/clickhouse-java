# Client V2 JSON Processors Example

## Overview

This standalone example shows how to configure `client-v2` to read `JSONEachRow`
responses with both supported JSON processors using one shared table and one
shared dataset:

- `JACKSON`
- `GSON`

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

- `-DchEndpoint` - Endpoint to connect to (default: `http://localhost:8123`)
- `-DchUser` - ClickHouse user name (default: `default`)
- `-DchPassword` - ClickHouse user password (default: empty)
- `-DchDatabase` - ClickHouse database name (default: `default`)

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

- Runs the following steps in order:
  1. defines table `client_v2_json_processors_example` with primitive columns
     and one `payload JSON` column;
  2. loads sample rows from `src/main/resources/sample_data.csv` into that table;
  3. reads the same rows with `runGsonExample(...)`;
  4. reads the same rows again with `runJacksonExample(...)`.
- Reads rows back through `client.newBinaryFormatReader(response)` and logs the
  primitive columns together with the parsed JSON object from `payload`.

The build keeps both `jackson-databind` and `gson` on the classpath so the
example can switch between processors at runtime. Production applications only
need to keep the processor they actually use.
