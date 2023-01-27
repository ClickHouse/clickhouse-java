# ClickHouse Java Client

Async Java client for ClickHouse. `clickhouse-client` is an abstract module, so it does not work by itself until being used together with an implementation like `clickhouse-http-client`, `clickhouse-grpc-client` or `clickhouse-cli-client`.

## Configuration

You can pass any client option([common](https://github.com/ClickHouse/clickhouse-jdbc/blob/main/clickhouse-client/src/main/java/com/clickhouse/client/config/ClickHouseClientOption.java), [http](https://github.com/ClickHouse/clickhouse-jdbc/blob/main/clickhouse-http-client/src/main/java/com/clickhouse/client/http/config/ClickHouseHttpOption.java), [grpc](https://github.com/ClickHouse/clickhouse-jdbc/blob/main/clickhouse-grpc-client/src/main/java/com/clickhouse/client/grpc/config/ClickHouseGrpcOption.java), and [cli](https://github.com/ClickHouse/clickhouse-jdbc/blob/main/clickhouse-cli-client/src/main/java/com/clickhouse/client/cli/config/ClickHouseCommandLineOption.java)) to `ClickHouseRequest.option()` and [server setting](https://clickhouse.com/docs/en/operations/settings/) to `ClickHouseRequest.set()` before execution, for instance:

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

[Default value](https://github.com/ClickHouse/clickhouse-jdbc/blob/main/clickhouse-client/src/main/java/com/clickhouse/client/config/ClickHouseDefaults.java) can be either configured via system property or environment variable.

## Quick Start

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-http-client</artifactId>
    <version>0.4.0</version>
</dependency>
```

```java
// declare a list of servers to connect to
ClickHouseNodes servers = ClickHouseNodes.of(
    "jdbc:ch:http://server1.domain,server2.domain,server3.domain/my_db"
    + "?load_balancing_policy=random&health_check_interval=5000&failover=2");

// execute multiple queries in a worker thread one after another within same session
CompletableFuture<List<ClickHouseResponseSummary>> future = ClickHouseClient.send(servers.get(),
    "create database if not exists test",
    "use test", // change current database from my_db to test
    "create table if not exists test_table(s String) engine=Memory",
    "insert into test_table values('1')('2')('3')",
    "select * from test_table limit 1",
    "truncate table test_table",
    "drop table if exists test_table");

// block current thread until queries completed, and then retrieve summaries
// List<ClickHouseResponseSummary> results = future.get();

try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP)) {
    ClickHouseRequest<?> request = client.connect(servers).format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
    // load data into a table and wait until it's completed
    request.write()
        .query("insert into my_table select c2, c3 from input('c1 UInt8, c2 String, c3 Int32')")
        .data(myInputStream).execute().thenAccept(response -> {
	        response.close();
        });

    // query with named parameter
    try (ClickHouseResponse response = request.query(ClickHouseParameterizedQuery.of(
        request.getConfig(), "select * from numbers(:limit)")).params(100000).executeAndWait()) {
        for (ClickHouseRecord r : response.records()) {
            // Don't cache ClickHouseValue / ClickHouseRecord as they're reused for
            // corresponding column / row
            ClickHouseValue v = r.getValue(0);
            // converts to DateTime64(6)
            LocalDateTime dateTime = v.asDateTime(6);
            // converts to long/int/byte if you want to
            long l = v.asLong();
            int i = v.asInteger();
            byte b = v.asByte();
        }
    }
}
```
