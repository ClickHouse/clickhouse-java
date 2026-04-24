# Client V2 JSON Processors Example

## Overview

This standalone example shows how to configure `client-v2` to read `JSONEachRow`
responses with either supported JSON processor:

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

The example runs both processors by default. To run only one of them:

```shell
gradle run -DjsonProcessor=GSON
```

Connection properties can be supplied as system properties:

- `-DchEndpoint` - Endpoint to connect to (default: `http://localhost:8123`)
- `-DchUser` - ClickHouse user name (default: `default`)
- `-DchPassword` - ClickHouse user password (default: empty)
- `-DchDatabase` - ClickHouse database name (default: `default`)
- `-DjsonProcessor` - One processor to run: `JACKSON` or `GSON`

Example with custom connection properties:

```shell
gradle run \
  -DchEndpoint=http://localhost:8123 \
  -DchUser=default \
  -DchPassword= \
  -DchDatabase=default \
  -DjsonProcessor=JACKSON
```

## Executable Example

`com.clickhouse.examples.client_v2.json_processors.ClientV2JsonProcessorsExample`

- Creates a `client-v2` instance with `json_processor` set to `JACKSON` or
  `GSON`.
- Executes a simple `SELECT ... FROM numbers(3)` query in `JSONEachRow` format.
- Reads rows back through `client.newBinaryFormatReader(response)` and logs the
  parsed values.

The build keeps both `jackson-databind` and `gson` on the classpath so the
example can switch between processors at runtime. Production applications only
need to keep the processor they actually use.
