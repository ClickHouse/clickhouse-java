# ClickHouse Java Client & JDBC Driver

[![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/ClickHouse/clickhouse-jdbc?include_prereleases)](https://github.com/ClickHouse/clickhouse-jdbc/releases/) ![Build Status(https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg)](https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=ClickHouse_clickhouse-jdbc&metric=coverage)](https://sonarcloud.io/dashboard?id=ClickHouse_clickhouse-jdbc)

Java client and JDBC driver for ClickHouse. Java client is async, lightweight, and low-overhead library for ClickHouse; while JDBC driver is built on top of the Java client with more dependencies and extensions for JDBC-compliance.

Java 8 or higher is required in order to use Java client([clickhouse-client](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client)) and/or JDBC driver([clickhouse-jdbc](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-jdbc)). In addition, starting from 0.3.2, JDBC driver only works with ClickHouse 20.7 or above, so please consider to either downgrade the driver to 0.3.1-patch or upgrade server to one of [active releases](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease).

---

:exclamation: **IMPORTANT**

Maven groupId `ru.yandex.clickhouse` and legacy JDBC driver `ru.yandex.clickhouse.ClickHouseDriver` have been deprecated and no longer receive updates.

Please use new groupId `com.clickhouse` and driver `com.clickhouse.jdbc.ClickHouseDriver` instead. It's highly recommended to upgrade to 0.3.2+ now for improved performance and stability.

![image](https://user-images.githubusercontent.com/4270380/154429324-631f718d-9277-4522-b60d-13f87b2e6c31.png)
Note: in general, the new driver(v0.3.2) is a few times faster with less memory usage. More information can be found at [here](https://github.com/ClickHouse/clickhouse-jdbc/issues/768).

---

## Features

| Category          | Feature                                                              | Supported          | Remark                                                                                                                                                               |
| ----------------- | -------------------------------------------------------------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| API               | [JDBC](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/) | :white_check_mark: |                                                                                                                                                                      |
|                   | [R2DBC](https://r2dbc.io/)                                           | :x:                | will be supported in 0.3.3                                                                                                                                           |
| Protocol          | [HTTP](https://clickhouse.com/docs/en/interfaces/http/)              | :white_check_mark: | recommended, defaults to `java.net.HttpURLConnection` and can be changed to `java.net.http.HttpClient`(faster but less stable)                                       |
|                   | [gRPC](https://clickhouse.com/docs/en/interfaces/grpc/)              | :white_check_mark: | still experimental, works with 22.3+, known to has [issue](https://github.com/ClickHouse/ClickHouse/issues/28671#issuecomment-1087049993) when using LZ4 compression |
|                   | [TCP/Native](https://clickhouse.com/docs/en/interfaces/tcp/)         | :white_check_mark: | `clickhouse-cli-client`(wrapper of ClickHouse native command-line client) was added in 0.3.2-patch10, `clickhouse-tcp-client` will be available in 0.3.3             |
| Compatibility     | Server < 20.7                                                        | :x:                | use 0.3.1-patch(or 0.2.6 if you're stuck with JDK 7)                                                                                                                 |
|                   | Server >= 20.7                                                       | :white_check_mark: | use 0.3.2 or above. All [active releases](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) are supported.                         |
| Data Format       | RowBinary                                                            | :white_check_mark: | `RowBinaryWithNamesAndTypes` for query and `RowBinary` for insertion                                                                                                 |
|                   | TabSeparated                                                         | :white_check_mark: | Does not support as many data types as RowBinary                                                                                                                     |
| Data Type         | AggregatedFunction                                                   | :x:                | limited to `groupBitmap`                                                                                                                                             |
|                   | Array(\*)                                                            | :white_check_mark: |                                                                                                                                                                      |
|                   | Bool                                                                 | :white_check_mark: |                                                                                                                                                                      |
|                   | Date\*                                                               | :white_check_mark: |                                                                                                                                                                      |
|                   | DateTime\*                                                           | :white_check_mark: |                                                                                                                                                                      |
|                   | Decimal\*                                                            | :white_check_mark: | `SET output_format_decimal_trailing_zeros=1` in 21.9+ for consistency                                                                                                |
|                   | Enum\*                                                               | :white_check_mark: | can be treated as both string and integer                                                                                                                            |
|                   | Geo Types                                                            | :white_check_mark: | Point, Ring, Polygon, and MultiPolygon                                                                                                                               |
|                   | Int\*, UInt\*                                                        | :white_check_mark: | UInt64 is mapped to `long`                                                                                                                                           |
|                   | IPv\*                                                                | :white_check_mark: |                                                                                                                                                                      |
|                   | Map(\*)                                                              | :white_check_mark: |                                                                                                                                                                      |
|                   | Nested(\*)                                                           | :white_check_mark: |                                                                                                                                                                      |
|                   | Object('JSON')                                                       | :white_check_mark: | supported since 0.3.2-patch8                                                                                                                                         |
|                   | SimpleAggregateFunction                                              | :white_check_mark: |                                                                                                                                                                      |
|                   | \*String                                                             | :white_check_mark: |                                                                                                                                                                      |
|                   | Tuple(\*)                                                            | :white_check_mark: |                                                                                                                                                                      |
|                   | UUID                                                                 | :white_check_mark: |                                                                                                                                                                      |
| High Availability | Load Balancing                                                       | :white_check_mark: | supported since 0.3.2-patch10                                                                                                                                        |
|                   | Failover                                                             | :white_check_mark: | supported since 0.3.2-patch10                                                                                                                                        |
| Transaction       | Transaction                                                          | :white_check_mark: | supported since 0.3.2-patch11, use ClickHouse 22.7+ for native implicit transaction support                                                                          |
|                   | Savepoint                                                            | :x:                |                                                                                                                                                                      |
|                   | XAConnection                                                         | :x:                |                                                                                                                                                                      |

## Examples

### Java Client

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <!-- or clickhouse-grpc-client if you prefer gRPC -->
    <artifactId>clickhouse-http-client</artifactId>
    <version>0.3.2-patch10</version>
</dependency>
```

```java
// endpoint: protocol://host[:port][/database][?param1=value1&param2=value2...][#tag1,tag2,...]
ClickHouseNode endpoint = ClickHouseNode.of("https://localhost"); // http://localhost:8443?ssl=true&sslmode=NONE
// endpoints: [defaultProtocol://]endpoint1[,endpoint2,endpoint3,...][/defaultDatabase][?defaultParameters][#efaultTags]
ClickHouseNodes endpoints = ClickHouseNodes.of("http://(https://explorer@play.clickhouse.com:443),localhost,(tcp://localhost?!auto_discovery#experimental),(grpc://localhost#experimental)?failover=3#test")

try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
    ClickHouseResponse response = client.connect(endpoint) // or client.connect(endpoints)
        // you'll have to parse response manually if using a different format
        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
        .query("select * from numbers(:limit)")
        .params(1000).executeAndWait()) {
    // or response.stream() if you prefer stream API
    for (ClickHouseRecord r : response.records()) {
        int num = r.getValue(0).asInteger();
        // type conversion
        String str = r.getValue(0).asString();
        LocalDate date = r.getValue(0).asDate();
    }

    ClickHouseResponseSummary summary = response.getSummary();
    long totalRows = summary.getTotalRowsToRead();
}
```

### JDBC Driver

```xml
<dependency>
    <!-- please stop using ru.yandex.clickhouse as it's been deprecated -->
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>0.3.2-patch10</version>
    <!-- use uber jar with all dependencies included, change classifier to http for smaller jar -->
    <classifier>all</classifier>
    <exclusions>
        <exclusion>
            <groupId>*</groupId>
            <artifactId>*</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

```java
// jdbc:(ch|clickhouse):[defaultProtocol://]endpoint1[,endpoint2,endpoint3,...][/defaultDatabase][?defaultParameters][#efaultTags]
String url = "jdbc:ch:https://play.clickhouse.com:443";
Properties properties = new Properties();
properties.setProperty("user", "explorer");
properties.setProperty("password", "");
// optional properties
properties.setProperty("client_name", "Agent #1");
...

ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
try (Connection conn = dataSource.getConnection();
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("show databases")) {
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
              <jdkHome>/usr/lib/jvm/java-11-openjdk</jdkHome>
          </configuration>
      </toolchain>
      <toolchain>
          <type>jdk</type>
          <provides>
              <version>17</version>
          </provides>
          <configuration>
              <jdkHome>/usr/lib/jvm/java-17-openjdk</jdkHome>
          </configuration>
      </toolchain>
  </toolchains>
  ```

- run `mvn -Drelease clean install` to build and install the artificat to local repository

  Note: if you need to build modules separately, please start with `clickhouse-client`, followed by `clickhouse-http-client` and `clickhouse-grpc-client`, and then `clickhouse-jdbc` and `clickhouse-benchmark`.

## Benchmark

To benchmark JDBC drivers:

```bash
cd clickhouse-benchmark
mvn -Drelease clean package
# single thread mode
java -DdbHost=localhost -jar target/benchmarks.jar -t 1 \
    -p client=clickhouse-jdbc -p connection=reuse \
    -p statement=prepared Query.selectInt8
```

It's time consuming to run all benchmarks against all drivers using different parameters for comparison. If you just need some numbers to understand performance, please refer to table below and some more details like CPU and memory usage mentioned at [here](https://github.com/ClickHouse/clickhouse-jdbc/issues/768)(still have plenty of room to improve according to ranking at [here](https://github.com/go-faster/ch-bench)).

## Testing

By default, docker container will be created automatically during integration test. You can pass system property like `-DclickhouseVersion=21.8` to specify version of ClickHouse.

In the case you prefer to test against an existing server, please follow instructions below:

- make sure the server can be accessed using default account(user `default` and no password), which has both DDL and DML privileges
- add below two configuration files to the existing server and expose all ports for external access
  - [ports.xml](../../blob/master/clickhouse-client/src/test/resources/containers/clickhouse-server/config.d/ports.xml) - enable all ports
  - and [users.xml](../../blob/master/clickhouse-client/src/test/resources/containers/clickhouse-server/users.d/users.xml) - accounts used for integration test
    Note: you may need to change root element from `clickhouse` to `yandex` when testing old version of ClickHouse.
- put `test.properties` under either `~/.clickhouse` or `src/test/resources` of your project, with content like below:
  ```properties
  clickhouseServer=x.x.x.x
  # below properties are only useful for test containers
  #clickhouseVersion=latest
  #clickhouseTimezone=UTC
  #clickhouseImage=clickhouse/clickhouse-server
  #additionalPackages=
  ```
