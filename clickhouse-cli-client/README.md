# ClickHouse Command-line Client

This is a wrapper of ClickHouse native command-line client. In order to use it, please make sure 1) either the native command-line client or docker is installed; and 2) `clickhouse_cli_path` or `docker_cli_path` is configured properly.

Unlike `clickhouse-http-client`, this module is not designed for dealing with many queries in short period of time, because it uses sub-process(NOT thread) and file-based streaming. Having said that, it provides an alternative, usually faster,way to dump and load large data sets. Besides, due to its simplicity, it can be used as an example to demonstrate how to implement SPI defined in `clickhouse-client`.

## Limitations and Known Issues

- Only `max_result_rows` and `result_overflow_mode` two settings are currently supported
- ClickHouseResponseSummary is always empty - see ClickHouse/ClickHouse#37241
- Session is not supported and query cannot be cancelled - see ClickHouse/ClickHouse#37308

## Maven Dependency

```xml
<dependency>
    <!-- will stop using ru.yandex.clickhouse starting from 0.4.0 -->
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-cli-client</artifactId>
    <version>0.3.2-patch9</version>
</dependency>
```

## Examples

```java
// make sure 'clickhouse-client' or 'docker' is in PATH before you start the program
// alternatively, configure CLI path in either Java system property or environment variable, for examples:
// CHC_CLICKHOUSE_CLI_PATH=/path/to/clickhouse-client CHC_DOCKER_CLI_PATH=/path/to/docker java MyProgram
// java -Dclickhouse_cli_path=/path/to/clickhouse-client -Ddocker_cli_path=/path/to/docker MyProgram

// clickhouse-cli-client uses TCP protocol
ClickHouseProtocol preferredProtocol = ClickHouseProtocol.TCP;
// connect to my-server, use default port(9000) of TCP/native protocol
ClickHouseNode server = ClickHouseNode.builder().host("my-server").port(preferredProtocol).build();

// declares a file
ClickHouseFile file = ClickHouseFile.of("data.csv");

// dump query results into the file - format is TSV, according to file extension
ClickHouseClient.dump(server, "select * from some_table", file).get();

// now load it into my_table, using TSV format
ClickHouseClient.load(server, "my_table", file).get();

// it can be used in the same as any other client
try (ClickHouseClient client = ClickHouseClient.newInstance(preferredProtocol);
    ClickHouseResponse response = client.connect(server)
        .query("select * from numbers(:limit)")
        .params(1000).executeAndWait()) {
    for (ClickHouseRecord r : response.records()) {
        int num = r.getValue(0).asInteger();
        String str = r.getValue(0).asString();
    }
}
```
