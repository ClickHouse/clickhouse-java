# ClickHouse Command-line Client

## Warning
`clickhouse-cli-client` package is deprecated from version 0.6.0 and it's going to be removed in `0.7.0`. We recommend using [clickhouse-client](https://clickhouse.com/docs/en/interfaces/cli) instead.

This is a thin wrapper of ClickHouse native command-line client. It provides an alternative way to communicate with ClickHouse, which might be of use when you prefer:

- TCP/native protocol over HTTP or gRPC
- native CLI client instead of pure Java implementation
- an example of implementing SPI defined in `clickhouse-client` module

Either [clickhouse](https://clickhouse.com/docs/en/interfaces/cli/) or [docker](https://docs.docker.com/get-docker/) must be installed prior to use. And it's important to understand that this module uses sub-process(in addition to threads) and file-based streaming, meaning 1) it's not as fast as native CLI client or pure Java implementation, although it's close in the case of dumping and loading data; and 2) it's not suitable for scenarios like dealing with many queries in short period of time.

## Limitations and Known Issues

- Only `max_result_rows`, `result_overflow_mode` and `readonly` 3 settings are currently supported
- ClickHouseResponseSummary is always empty - see ClickHouse/ClickHouse#37241
- Session is not supported - see ClickHouse/ClickHouse#37308

## Maven Dependency

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-cli-client</artifactId>
    <version>0.4.2</version>
</dependency>
```

## Examples

```java
// make sure 'clickhouse' or 'docker' is in PATH before you start the program
// alternatively, configure CLI path in either Java system property or environment variable, for examples:
// CHC_CLICKHOUSE_CLI_PATH=/path/to/clickhouse CHC_DOCKER_CLI_PATH=/path/to/docker java MyProgram
// java -Dchc_clickhouse_cli_path=/path/to/clickhouse -Dchc_docker_cli_path=/path/to/docker MyProgram

// clickhouse-cli-client uses TCP protocol
ClickHouseProtocol preferredProtocol = ClickHouseProtocol.TCP;
// connect to my-server, use default port(9000) of TCP/native protocol
ClickHouseNode server = ClickHouseNode.builder().host("my-server").port(preferredProtocol).build();

// declares a file
ClickHouseFile file = ClickHouseFile.of("data.csv");

// dump query results into the file - format is CSV, according to file extension
ClickHouseClient.dump(server, "select * from some_table", file).get();

// now load it into my_table, using CSV format
ClickHouseClient.load(server, "my_table", file).get();

// it can be used in the same way as any other client
try (ClickHouseClient client = ClickHouseClient.newInstance(preferredProtocol);
    ClickHouseResponse response = client.connect(server)
        .query("select * from numbers(:limit)")
        .params(1000).executeAndWait()) {
    for (ClickHouseRecord r : response.records()) {
        int num = r.getValue(0).asInteger();
        String str = r.getValue(0).asString();
    }
}

// and of course it's part of JDBC driver
try (Connection conn = DriverManager.getConnect("jdbc:ch:tcp://my-server", "default", "");
    PreparedStatement stmt = conn.preparedStatement("select * from numbers(?)")) {
    stmt.setInt(1, 1000);
    ResultSet rs = stmt.executeQuery();
    while (rs.next()) {
        int num = rs.getInt(1);
        String str = rs.getString(1);
    }
}
```
