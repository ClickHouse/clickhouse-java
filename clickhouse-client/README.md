# ClickHouse Java Client

Async Java client for ClickHouse. `clickhouse-client` is an abstract module, so it does not work by itself until being used together with an implementation like `clickhouse-http-client`, `clickhouse-grpc-client` or `clickhouse-cli-client`.

## Documentation
See the [ClickHouse website](https://clickhouse.com/docs/en/integrations/language-clients/java/client) for the full documentation entry.

## Configuration

You can pass any client option([common](https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-client/src/main/java/com/clickhouse/client/config/ClickHouseClientOption.java), [http](https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-http-client/src/main/java/com/clickhouse/client/http/config/ClickHouseHttpOption.java), [grpc](https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-grpc-client/src/main/java/com/clickhouse/client/grpc/config/ClickHouseGrpcOption.java), and [cli](https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-cli-client/src/main/java/com/clickhouse/client/cli/config/ClickHouseCommandLineOption.java)) to `ClickHouseRequest.option()` and [server setting](https://clickhouse.com/docs/en/operations/settings/) to `ClickHouseRequest.set()` before execution, for instance:

```java
client.connect("http://localhost/system")
    .query("select 1")
    // short version of option(ClickHouseClientOption.FORMAT, ClickHouseFormat.RowBinaryWithNamesAndTypes)
    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
    .option(ClickHouseClientOption.SOCKET_TIMEOUT, 30000 * 2) // 60 seconds
    .set("max_rows_to_read", 100)
    .set("read_overflow_mode", "throw")
    .execute()
    .whenComplete((response, throwable) -> {
        if (throwable != null) {
            log.error("Unexpected error", throwable);
        } else {
            try {
                for (ClickHouseRecord rec : response.records()) {
                    // ...
                }
            } finally {
                response.close();
            }
        }
    });
```

[Default value](https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-client/src/main/java/com/clickhouse/client/config/ClickHouseDefaults.java) can be either configured via system property or environment variable.

## Examples
For more example please check [here](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/client).
