# JDBC V2 JSON Processors Example

## Overview

This standalone example shows how to configure `jdbc-v2` to read
`FORMAT JSONEachRow` results with both supported JSON processors using one
shared table and one shared dataset:

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

Connection properties can be supplied as system properties:

- `-DchUrl` - JDBC URL (default: `jdbc:clickhouse://localhost:8123/default`)
- `-DchUser` - ClickHouse user name (default: `default`)
- `-DchPassword` - ClickHouse user password (default: empty)

Example with custom connection properties:

```shell
gradle run \
  -DchUrl=jdbc:clickhouse://localhost:8123/default \
  -DchUser=default \
  -DchPassword=
```

## Executable Example

`com.clickhouse.examples.jdbc_v2.json_processors.JdbcV2JsonProcessorsExample`

- Runs the following steps in order:
  1. defines table `jdbc_v2_json_processors_example` with primitive columns and
     one `payload JSON` column;
  2. loads one fixed dataset into that table;
  3. reads the same rows with `runGsonExample(...)`;
  4. reads the same rows again with `runJacksonExample(...)`.
- Reads rows back through `ResultSet` and logs the primitive columns together
  with the parsed JSON object from `payload`.

The build keeps both `jackson-databind` and `gson` on the classpath so the
example can switch between processors at runtime. Production applications only
need to keep the processor they actually use.
