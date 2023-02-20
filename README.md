# ClickHouse Java Libraries

[![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/ClickHouse/clickhouse-java?style=plastic&include_prereleases&label=Latest%20Release)](https://github.com/ClickHouse/clickhouse-java/releases/) [![GitHub release (by tag)](https://img.shields.io/github/downloads/ClickHouse/clickhouse-java/latest/total?style=plastic)](https://github.com/ClickHouse/clickhouse-java/releases/) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=ClickHouse_clickhouse-jdbc&metric=coverage)](https://sonarcloud.io/summary/new_code?id=ClickHouse_clickhouse-jdbc) [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/com.clickhouse/clickhouse-java?style=plastic&label=Nightly%20Build&server=https%3A%2F%2Fs01.oss.sonatype.org)](https://s01.oss.sonatype.org/content/repositories/snapshots/com/clickhouse/) [![GitHub milestone](https://img.shields.io/github/milestones/progress-percent/ClickHouse/clickhouse-java/12?style=social)](https://github.com/ClickHouse/clickhouse-java/milestone/12)

Java libraries for connecting to ClickHouse and processing data in various formats. Java client is async, lightweight, and low-overhead library for ClickHouse; while JDBC and R2DBC drivers are built on top of the Java client with more dependencies and features. Java 8 or higher is required to use the libraries. In addition, please use ClickHouse 20.7+ or any of [active releases](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease).

![image](https://user-images.githubusercontent.com/4270380/212460181-2b806482-bc1c-492c-bd69-cdeb2c8845b5.png)

## Features

| Category          | Feature                                                                             | Supported          | Remark                                                                                                                                                                                                                   |
| ----------------- | ----------------------------------------------------------------------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| API               | [JDBC](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/)                | :white_check_mark: |                                                                                                                                                                                                                          |
|                   | [R2DBC](https://r2dbc.io/)                                                          | :white_check_mark: | supported since 0.4.0                                                                                                                                                                                                    |
| Query Language    | SQL                                                                                 | :white_check_mark: |                                                                                                                                                                                                                          |
|                   | [PRQL](https://prql-lang.org/)                                                      | :x:                | will be available in 0.4.2                                                                                                                                                                                               |
|                   | [GraphQL](https://graphql.org/)                                                     | :x:                | will be available in 0.4.2                                                                                                                                                                                               |
| Protocol          | [HTTP](https://clickhouse.com/docs/en/interfaces/http/)                             | :white_check_mark: | recommended, defaults to `java.net.HttpURLConnection` and it can be changed to `java.net.http.HttpClient`(unstable) or `Apache HTTP Client 5`. Note that the latter was added in 0.4.0 to support custom socket options. |
|                   | [gRPC](https://clickhouse.com/docs/en/interfaces/grpc/)                             | :white_check_mark: | :warning: experimental, works with 22.3+, known to has issue with lz4 compression and may cause high memory usage on server                                                                                              |
|                   | [TCP/Native](https://clickhouse.com/docs/en/interfaces/tcp/)                        | :white_check_mark: | `clickhouse-cli-client`(wrapper of ClickHouse native command-line client) was added in 0.3.2-patch10, `clickhouse-tcp-client` will be available in 0.5                                                                   |
|                   | [Local/File](https://clickhouse.com/docs/en/operations/utilities/clickhouse-local/) | :x:                | `clickhouse-cli-client` will be enhanced to support `clickhouse-local`                                                                                                                                                   |
| Compatibility     | Server < 20.7                                                                       | :x:                | use 0.3.1-patch(or 0.2.6 if you're stuck with JDK 7)                                                                                                                                                                     |
|                   | Server >= 20.7                                                                      | :white_check_mark: | use 0.3.2 or above. All [active releases](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) are supported.                                                                             |
| Compression       | [lz4](https://lz4.github.io/lz4/)                                                   | :white_check_mark: | default                                                                                                                                                                                                                  |
|                   | [zstd](https://facebook.github.io/zstd/)                                            | :white_check_mark: | supported since 0.4.0, works with ClickHouse 22.10+                                                                                                                                                                      |
| Data Format       | RowBinary                                                                           | :white_check_mark: | `RowBinaryWithNamesAndTypes` for query and `RowBinary` for insertion                                                                                                                                                     |
|                   | TabSeparated                                                                        | :white_check_mark: | :warning: does not support as many data types as RowBinary                                                                                                                                                               |
| Data Type         | AggregateFunction                                                                   | :x:                | :warning: limited to `groupBitmap`; 64bit bitmap was NOT working properly before 0.4.1                                                                                                                                   |
|                   | Array(\*)                                                                           | :white_check_mark: |                                                                                                                                                                                                                          |
|                   | Bool                                                                                | :white_check_mark: |                                                                                                                                                                                                                          |
|                   | Date\*                                                                              | :white_check_mark: |                                                                                                                                                                                                                          |
|                   | DateTime\*                                                                          | :white_check_mark: |                                                                                                                                                                                                                          |
|                   | Decimal\*                                                                           | :white_check_mark: | `SET output_format_decimal_trailing_zeros=1` in 21.9+ for consistency                                                                                                                                                    |
|                   | Enum\*                                                                              | :white_check_mark: | can be treated as both string and integer                                                                                                                                                                                |
|                   | Geo Types                                                                           | :white_check_mark: | Point, Ring, Polygon, and MultiPolygon                                                                                                                                                                                   |
|                   | Int\*, UInt\*                                                                       | :white_check_mark: | UInt64 is mapped to `long`                                                                                                                                                                                               |
|                   | IPv\*                                                                               | :white_check_mark: |                                                                                                                                                                                                                          |
|                   | Map(\*)                                                                             | :white_check_mark: |                                                                                                                                                                                                                          |
|                   | Nested(\*)                                                                          | :white_check_mark: | :warning: broken before 0.4.1                                                                                                                                                                                            |
|                   | Object('JSON')                                                                      | :white_check_mark: | supported since 0.3.2-patch8                                                                                                                                                                                             |
|                   | SimpleAggregateFunction                                                             | :white_check_mark: |                                                                                                                                                                                                                          |
|                   | \*String                                                                            | :white_check_mark: | :warning: requires `use_binary_string=true` for binary string support since v0.4.0                                                                                                                                       |
|                   | Tuple(\*)                                                                           | :white_check_mark: |                                                                                                                                                                                                                          |
|                   | UUID                                                                                | :white_check_mark: |                                                                                                                                                                                                                          |
| High Availability | Load Balancing                                                                      | :white_check_mark: | supported since 0.3.2-patch10                                                                                                                                                                                            |
|                   | Failover                                                                            | :white_check_mark: | supported since 0.3.2-patch10                                                                                                                                                                                            |
| Transaction       | Transaction                                                                         | :white_check_mark: | supported since 0.3.2-patch11, use ClickHouse 22.7+ for native implicit transaction support                                                                                                                              |
|                   | Savepoint                                                                           | :x:                |                                                                                                                                                                                                                          |
|                   | XAConnection                                                                        | :x:                |                                                                                                                                                                                                                          |

## Usage

The library can be downloaded from both [Github Releases](../../releases) and [Maven Central](https://repo1.maven.org/maven2/com/clickhouse/). Development snapshots(aka. nightly build) are available on [Sonatype OSSRH](https://s01.oss.sonatype.org/content/repositories/snapshots/com/clickhouse/).

```xml
<repositories>
    <repository>
        <id>ossrh</id>
        <name>Sonatype OSSRH</name>
        <url>https://s01.oss.sonatype.org/content/repositories/snapshots/</url>
    </repository>
</repositories>
```

### Java Client

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <!-- or clickhouse-grpc-client if you prefer gRPC -->
    <artifactId>clickhouse-http-client</artifactId>
    <version>0.4.1</version>
</dependency>
```

```java
//  endpoint: protocol://host[:port][/database][?param[=value][&param[=value]][#tag[,tag]]
ClickHouseNode endpoint = ClickHouseNode.of("https://localhost"); // http://localhost:8443?ssl=true&sslmode=NONE
// endpoints: [defaultProtocol://]endpoint[,endpoint][/defaultDatabase][?defaultParameters][#defaultTags]
ClickHouseNodes endpoints = ClickHouseNodes.of("http://(https://explorer@play.clickhouse.com:443),localhost,"
    + "(tcp://localhost?!auto_discovery#experimental),(grpc://localhost#experimental)?failover=3#test")

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
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>0.4.1</version>
    <!-- use uber jar with all dependencies included, change classifier to http for smaller jar -->
    <classifier>all</classifier>
</dependency>
```

```java
// jdbc:(ch|clickhouse):[defaultProtocol://]endpoint[,endpoint][/defaultDatabase][?defaultParameters][#defaultTags]
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

More examples can be found at [here](../../tree/main/examples/jdbc).

## Build with Maven

Use `mvn -Dj8 -DskipITs clean verify` to compile and generate packages if you're using JDK 8. To create a multi-release jar (see [JEP-238](https://openjdk.java.net/jeps/238)), please use JDK 17+ with `~/.m2/toolchains.xml` like below, and run `mvn -DskipITs clean verify` instead.

```xml
<?xml version="1.0" encoding="UTF8"?>
<toolchains>
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

To create a native binary of JDBC driver for evaluation and testing:

- [install GraalVM](https://www.graalvm.org/latest/docs/getting-started/) and optionally [upx](https://upx.github.io/)

- make sure you have [native-image](https://www.graalvm.org/latest/docs/getting-started/#native-image) installed, and then build the native binary

  ```bash
  cd clickhouse-java
  mvn -DskipTests clean install
  cd clickhouse-jdbc
  mvn -DskipTests -Pnative clean package
  # only if you have upx installed
  upx -7 -k target/clickhouse-jdbc-bin
  ```

- run native binary

  ```bash
  # print usage
  ./target/clickhouse-jdbc-bin
  Usage: clickhouse-jdbc-bin [PROPERTIES] <URL> [QUERY] [FILE]
  ...

  # test database connection using JDBC driver
  ./target/clickhouse-jdbc-bin -Dverbose=true 'jdbc:ch:http://localhost'
  Arguments:
    -   url=jdbc:ch:http://localhost
    - query=select 500000000
    -  file=jdbc.out

  Options: action=read, batch=1000, samples=500000000, serde=true, type=, verbose=true
  Processed 1 rows in 85.56 ms (11.69 rows/s)

  # test query performance using Java client
  ./target/clickhouse-jdbc-bin -Dverbose=true -Dtype=uint64 'http://localhost'
  ...
  Processed 500,000,000 rows in 52,491.38 ms (9,525,373.89 rows/s)

  # test same query again using JVM for comparison - don't have GraalVM EE so JIT wins in my case
  java -Dverbose=true -Dtype=uint64 -jar target/clickhouse-jdbc-*-http.jar 'http://localhost'
  ...
  Processed 500,000,000 rows in 25,267.89 ms (19,787,963.94 rows/s)
  ```

## Testing

By default, [docker](https://docs.docker.com/engine/install/) is required to run integration test. Docker image(defaults to `clickhouse/clickhouse-server`) will be pulled from Internet, and containers will be created automatically by [testcontainers](https://www.testcontainers.org/) before testing. To test against specific version of ClickHouse, you can pass parameter like `-DclickhouseVersion=22.8` to Maven.

In the case you don't want to use docker and/or prefer to test against an existing server, please follow instructions below:

- make sure the server can be accessed using default account(user `default` and no password), which has both DDL and DML privileges
- add below two configuration files to the existing server and expose all defaults ports for external access
  - [ports.xml](../../blob/main/clickhouse-client/src/test/resources/containers/clickhouse-server/config.d/ports.xml) - enable all ports
  - and [users.xml](../../blob/main/clickhouse-client/src/test/resources/containers/clickhouse-server/users.d/users.xml) - accounts used for integration test
    Note: you may need to change root element from `clickhouse` to `yandex` when testing old version of ClickHouse.
- make sure ClickHouse binary(usually `/usr/bin/clickhouse`) is available in PATH, as it's required to test `clickhouse-cli-client`
- put `test.properties` under either `~/.clickhouse` or `src/test/resources` of your project, with content like below:
  ```properties
  clickhouseServer=x.x.x.x
  # below properties are only useful for test containers
  #clickhouseVersion=latest
  #clickhouseTimezone=UTC
  #clickhouseImage=clickhouse/clickhouse-server
  #additionalPackages=
  ```

## Benchmark

To benchmark JDBC drivers:

```bash
cd clickhouse-benchmark
mvn clean package
# single thread mode
java -DdbHost=localhost -jar target/benchmarks.jar -t 1 \
    -p client=clickhouse-jdbc -p connection=reuse \
    -p statement=prepared -p type=default Query.selectInt8
```

It's time consuming to run all benchmarks against all drivers using different parameters for comparison. If you just need some numbers to understand performance, please refer to [this](https://github.com/ClickHouse/clickhouse-java/issues/768) and watch [this](https://github.com/ClickHouse/clickhouse-java/issues/928) for more information and updates.
