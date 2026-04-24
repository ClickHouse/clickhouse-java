# JDBC V2 JSON Processors Example

## Overview

This standalone example shows how to configure `jdbc-v2` to read
`FORMAT JSONEachRow` results with either supported JSON processor:

- `JACKSON`
- `GSON`

## Requirements

- JDK 17 or newer
- A running ClickHouse server reachable from the machine running the example
- A locally installed `jdbc-v2` snapshot from this repository

## How to Run

From this directory:

```shell
gradle run
```

The example runs both processors by default. To run only one of them:

```shell
gradle run -DjsonProcessor=JACKSON
```

Connection properties can be supplied as system properties:

- `-DchUrl` - JDBC URL (default: `jdbc:clickhouse://localhost:8123/default`)
- `-DchUser` - ClickHouse user name (default: `default`)
- `-DchPassword` - ClickHouse user password (default: empty)
- `-DjsonProcessor` - One processor to run: `JACKSON` or `GSON`

Example with custom connection properties:

```shell
gradle run \
  -DchUrl=jdbc:clickhouse://localhost:8123/default \
  -DchUser=default \
  -DchPassword= \
  -DjsonProcessor=GSON
```

## Executable Example

`com.clickhouse.examples.jdbc_v2.json_processors.JdbcV2JsonProcessorsExample`

- Creates a JDBC connection with the `json_processor` connection property set to
  `JACKSON` or `GSON`.
- Executes a simple `SELECT ... FORMAT JSONEachRow` query.
- Reads rows back through `ResultSet` and logs the parsed values.

The build keeps both `jackson-databind` and `gson` on the classpath so the
example can switch between processors at runtime. Production applications only
need to keep the processor they actually use.
