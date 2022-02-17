# ClickHouse Java Client & JDBC Driver

[![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/ClickHouse/clickhouse-jdbc?include_prereleases)](https://github.com/ClickHouse/clickhouse-jdbc/releases/) ![Build Status(https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg)](https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=ClickHouse_clickhouse-jdbc&metric=coverage)](https://sonarcloud.io/dashboard?id=ClickHouse_clickhouse-jdbc)

Java client and JDBC driver for ClickHouse. Java client is async, lightweight, and low-overhead library for ClickHouse; while JDBC driver is built on top of the Java client with more dependencies and extensions for JDBC-compliance.

Java 8 or higher is required in order to use Java client([clickhouse-client](https://github.com/ClickHouse/clickhouse-jdbc/clickhouse-client)) and/or JDBC driver([clickhouse-jdbc](https://github.com/ClickHouse/clickhouse-jdbc/clickhouse-jdbc)). In addition, starting from 0.3.2, JDBC driver only works on ClickHouse 20.7 or above, so please consider to either downgrade the driver to 0.3.1-patch or upgrade server to one of [active releases](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease).

## Feature Matrix

| Category      | Feature                                                      | Supported          | Remark                                                                                                                                       |
| ------------- | ------------------------------------------------------------ | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Protocol      | [HTTP](https://clickhouse.com/docs/en/interfaces/http/)      | :white_check_mark: | recommended, defaults to `java.net.HttpURLConnection` and can be changed to `java.net.http.HttpClient`(faster but less stable)               |
|               | [gRPC](https://clickhouse.com/docs/en/interfaces/grpc/)      | :white_check_mark: | experimental, still missing some features like LZ4 compression                                                                               |
|               | [TCP/Native](https://clickhouse.com/docs/en/interfaces/tcp/) | :x:                | will be available in 0.3.3                                                                                                                   |
| Compatibility | Server < 20.7                                                | :x:                | use 0.3.1-patch                                                                                                                              |
|               | Server >= 20.7                                               | :white_check_mark: | use 0.3.2 or above. All [active releases](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) are supported. |
| Data Type     | AggregatedFunction                                           | :x:                | limited to `groupBitmap`                                                                                                                     |
|               | Array(\*)                                                    | :white_check_mark: |                                                                                                                                              |
|               | Date\*                                                       | :white_check_mark: |                                                                                                                                              |
|               | DateTime\*                                                   | :white_check_mark: |                                                                                                                                              |
|               | Decimal\*                                                    | :white_check_mark: | `SET output_format_decimal_trailing_zeros=1` in 21.9+ for consistency                                                                        |
|               | Enum\*                                                       | :white_check_mark: | can be treated as both string and integer                                                                                                    |
|               | Geo Types                                                    | :white_check_mark: |                                                                                                                                              |
|               | Int\*, UInt\*                                                | :white_check_mark: | UInt64 is mapped to `long`                                                                                                                   |
|               | IPv\*                                                        | :white_check_mark: |                                                                                                                                              |
|               | \*String                                                     | :white_check_mark: |                                                                                                                                              |
|               | Map(\*)                                                      | :white_check_mark: |                                                                                                                                              |
|               | Nested(\*)                                                   | :white_check_mark: |                                                                                                                                              |
|               | Tuple(\*)                                                    | :white_check_mark: |                                                                                                                                              |
|               | UUID                                                         | :white_check_mark: |                                                                                                                                              |
| Format        | RowBinary                                                    | :white_check_mark: | `RowBinaryWithNamesAndTypes` for query and `RowBinary` for insertion                                                                         |
|               | TabSeparated                                                 | :white_check_mark: | Does not support as many data types as RowBinary                                                                                             |

## Configuration

- Client option, server setting, and default value

  You can pass any client option([common](https://github.com/ClickHouse/clickhouse-jdbc/blob/master/clickhouse-client/src/main/java/com/clickhouse/client/config/ClickHouseClientOption.java), [http](https://github.com/ClickHouse/clickhouse-jdbc/blob/master/clickhouse-http-client/src/main/java/com/clickhouse/client/http/config/ClickHouseHttpOption.java) and [grpc](https://github.com/ClickHouse/clickhouse-jdbc/blob/master/clickhouse-grpc-client/src/main/java/com/clickhouse/client/grpc/config/ClickHouseGrpcOption.java)) to `ClickHouseRequest.option()` and [server setting](https://clickhouse.com/docs/en/operations/settings/) to `ClickHouseRequest.set()` before execution, for instance:

  ```java
  ClickHouseRequest<?> request = client.connect(myServer);
  request
      .query("select 1")
      // short version of option(ClickHouseClientOption.FORMAT, ClickHouseFormat.RowBinaryWithNamesAndTypes)
      .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
      .option(ClickHouseClientOption.SOCKET_TIMEOUT, 30000 * 2)
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

  [Default value](https://github.com/ClickHouse/clickhouse-jdbc/blob/master/clickhouse-client/src/main/java/com/clickhouse/client/config/ClickHouseDefaults.java) can be either configured via system property or environment variable.

- JDBC configuration

  **Driver Class**: `com.clickhouse.jdbc.ClickHouseDriver`

  Note: `ru.yandex.clickhouse.ClickHouseDriver` and everything under `ru.yandex.clickhouse` will be removed starting from 0.4.0.

  **URL Syntax**: `jdbc:<prefix>[:<protocol>]://<host>:[<port>][/<database>[?param1=value1&param2=value2]]`, for examples:

  - `jdbc:ch://localhost` is same as `jdbc:clickhouse:http://localhost:8123`
  - `jdbc:ch:grpc://localhost` is same as `jdbc:clickhouse:grpc://localhost:9100`
  - `jdbc:ch://localhost/test?socket_timeout=120000`

  **Connection Properties**:

  | Property             | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                |
  | -------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | continueBatchOnError | `false`  | Whether to continue batch processing when error occurred                                                                                                                                                                                                                                                                                                                                                                   |
  | custom_http_headers  |         | comma separated custom http headers, for example: `User-Agent=client1,X-Gateway-Id=123`                                                                                                                                                                                                                                                                                                                                    |
  | custom_http_params   |         | comma separated custom http query parameters, for example: `extremes=0,max_result_rows=100`                                                                                                                                                                                                                                                                                                                                |
  | jdbcCompliance       | `true`  | Whether to support standard synchronous UPDATE/DELETE and fake transaction                                                                                                                                                                                                                                                                                                                                                 |
  | typeMappings         |         | Customize mapping between ClickHouse data type and Java class, which will affect result of both [getColumnType()](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSetMetaData.html#getColumnType-int-) and [getObject(Class<?>)](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html#getObject-java.lang.String-java.lang.Class-). For example: `UInt128=java.lang.String,UInt256=java.lang.String` |
  | wrapperObject        | `false` | Whether [getObject()](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html#getObject-int-) should return java.sql.Array / java.sql.Struct for Array / Tuple.                                                                                                                                                                                                                                                  |

  Note: please refer to [JDBC specific configuration](https://github.com/ClickHouse/clickhouse-jdbc/blob/master/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcConfig.java) and client options([common](https://github.com/ClickHouse/clickhouse-jdbc/blob/master/clickhouse-client/src/main/java/com/clickhouse/client/config/ClickHouseClientOption.java), [http](https://github.com/ClickHouse/clickhouse-jdbc/blob/master/clickhouse-http-client/src/main/java/com/clickhouse/client/http/config/ClickHouseHttpOption.java) and [grpc](https://github.com/ClickHouse/clickhouse-jdbc/blob/master/clickhouse-grpc-client/src/main/java/com/clickhouse/client/grpc/config/ClickHouseGrpcOption.java)) for more.

## Examples

### Java Client

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <!-- or clickhouse-grpc-client if you prefer gRPC -->
    <artifactId>clickhouse-http-client</artifactId>
    <version>0.3.2-patch5</version>
</dependency>
```

```java
// only HTTP and gRPC are supported at this point
ClickHouseProtocol preferredProtocol = ClickHouseProtocol.HTTP;
// you'll have to parse response manually if use different format
ClickHouseFormat preferredFormat = ClickHouseFormat.RowBinaryWithNamesAndTypes;

// connect to localhost, use default port of the preferred protocol
ClickHouseNode server = ClickHouseNode.builder().port(preferredProtocol).build();

try (ClickHouseClient client = ClickHouseClient.newInstance(preferredProtocol);
    ClickHouseResponse response = client.connect(server)
        .format(preferredFormat)
        .query("select * from numbers(:limit)")
        .params(1000).execute().get()) {
    // or resp.stream() if you prefer stream API
    for (ClickHouseRecord record : resp.records()) {
        int num = record.getValue(0).asInteger();
        String str = record.getValue(0).asString();
    }

    ClickHouseResponseSummary summary = resp.getSummary();
    long totalRows = summary.getRows();
}
```

### JDBC Driver

```xml
<dependency>
    <!-- will stop using ru.yandex.clickhouse starting from 0.4.0  -->
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>0.3.2-patch5</version>
    <!-- below is only needed when all you want is a shaded jar -->
    <classifier>http</classifier>
    <exclusions>
        <exclusion>
            <groupId>*</groupId>
            <artifactId>*</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

```java
String url = "jdbc:ch://localhost/test";
Properties properties = new Properties();
// optionally set connection properties
properties.setProperty("client_name", "Agent #1");
...

ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
try (Connection conn = dataSource.getConnection();
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from mytable")) {
    ...
}
```

## Build with Maven

Use `mvn clean verify` to compile, test and generate shaded packages if you're using JDK 8. To create a multi-release jar file(see [JEP-238](https://openjdk.java.net/jeps/238)), please use JDK 11 or above and follow instructions below:

- make sure you have `~/.m2/toolchains.xml`, for example:

  ```xml
  <?xml version="1.0" encoding="UTF8"?>
  <toolchains>
      <toolchain>
          <type>jdk</type>
          <provides>
              <version>11</version>
          </provides>
          <configuration>
              <jdkHome>${{ env.JDK11_HOME }}</jdkHome>
          </configuration>
      </toolchain>
  </toolchains>
  ```

- run `mvn -Drelease clean install` to build and install the artificat to local repository

  Note: if you need to build modules separately, please start with `clickhouse-client`, followed by `clickhouse-http-client` and `clickhouse-grpc-client`, and then `clickhouse-jdbc` and `clickhouse-benchmark`.

## Benchmark

To benchmark JDBC drivers for comparison:

```bash
cd clickhouse-benchmark
mvn -Drelease clean package
# single thread mode
java -DdbHost=localhost -jar target/benchmarks.jar -t 1 -p client=clickhouse-http-jdbc1 -p connection=reuse -p statement=prepared Query.selectInt8
```

It's time consuming to run all benchmarks against all drivers using different parameters. If you just need some numbers to understand performance, please refer to table below and some more details like CPU and memory usage mentioned at [here](https://github.com/ClickHouse/clickhouse-jdbc/issues/768)(still have plenty of room to improve according to ranking at [here](https://github.com/go-faster/ch-bench)).

![image](https://user-images.githubusercontent.com/4270380/154429324-631f718d-9277-4522-b60d-13f87b2e6c31.png)


## Testing

By default, docker container will be created automatically during integration test. You can pass system property like `-DclickhouseVersion=21.8` to specify version of ClickHouse.

In the case you prefer to test against an existing server, please follow instructions below:

- make sure the server can be accessed using default account(user `default` and no password), which has both DDL and DML privileges
- add below two configuration files to the existing server and expose all ports for external access
  - [ports.xml](./tree/master/clickhouse-client/src/test/resources/containers/clickhouse-server/config.d/ports.xml) - enable all ports
  - and [users.xml](./tree/master/clickhouse-client/src/test/resources/containers/clickhouse-server/users.d/users.xml) - accounts used for integration test
- put `test.properties` under either `~/.clickhouse` or `src/test/resources` of your project, with content like below:
  ```properties
  clickhouseServer=x.x.x.x
  # below properties are only useful for test containers
  #clickhouseVersion=latest
  #clickhouseTimezone=UTC
  #clickhouseImage=clickhouse/clickhouse-server
  #additionalPackages=
  ```
