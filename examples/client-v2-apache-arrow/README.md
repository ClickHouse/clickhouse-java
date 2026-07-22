# Client V2 Apache Arrow Example

## Overview

This module contains a runnable example demonstrating how to use `client-v2`
to insert and read data using the [Apache Arrow Stream](https://clickhouse.com/docs/en/interfaces/formats/#arrowstream)
format (`ClickHouseFormat.ArrowStream`).

The example shows:

- writing a batch into ClickHouse from an Arrow `VectorSchemaRoot` via
  `ArrowStreamWriter`, using `Decimal256Vector` and `TimeStampMilliVector`;
- reading rows back from ClickHouse with `ArrowStreamReader` and copying them
  into another table by streaming the same `VectorSchemaRoot` straight back to
  `client.insert(...)`.

Like the other examples in this repository, this one is built with **Maven**
(targeting JDK 17). It is a standalone project and is not part of the Maven
multi-module build.

## Requirements

- JDK 17 or newer.
- A running ClickHouse server reachable from the machine running the example.

Apache Arrow needs access to direct memory and a few internal JDK APIs. The
required `--add-opens` flags are wired into `.mvn/jvm.config`, so running the
example through `mvn ... exec:java` (which runs in the Maven JVM) just works:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
```

If you run the produced jar manually, pass these flags to the JVM yourself.

## How to Run

From this directory:

```shell
mvn compile exec:java
```

Connection properties can be supplied as system properties:

- `-DchEndpoint` - Endpoint to connect in the format of URL (default: `http://localhost:8123`)
- `-DchUser` - ClickHouse user name (default: `default`)
- `-DchPassword` - ClickHouse user password (default: empty)
- `-DchDatabase` - ClickHouse database name (default: `default`)

Example with custom connection properties:

```shell
mvn compile exec:java \
  -DchEndpoint=http://localhost:8123 \
  -DchUser=default \
  -DchPassword= \
  -DchDatabase=default
```

To see the wire-level data flow, raise the SLF4J log level:

```shell
mvn compile exec:java -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG
```

## Executable Example

`com.clickhouse.examples.arrow_format.ReadWriteArrow`

- Creates table `arrow_example (ts DateTime64, val1 Decimal(76,39))` and
  inserts a batch of 10 000 rows from Arrow vectors using
  `ClickHouseFormat.ArrowStream`.
- Creates tables `arrow_read_example` and `arrow_read_example_copy`
  (`ts DateTime(3), val1 Decimal(76,62)`), populates the first one, reads it
  back with `ArrowStreamReader`, and streams each batch into the copy table.

The example truncates the demo tables on every run, so it is safe to rerun.
