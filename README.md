# ClickHouse Java Client & JDBC Driver

[![clickhouse-jdbc](https://maven-badges.herokuapp.com/maven-central/com.clickhouse/clickhouse-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.clickhouse/clickhouse-jdbc) ![Build Status(https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg)](https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=ClickHouse_clickhouse-jdbc&metric=coverage)](https://sonarcloud.io/dashboard?id=ClickHouse_clickhouse-jdbc)

Java client and JDBC driver for ClickHouse. Java client is async and light weight library for accessing ClickHouse in Java; while JDBC driver is built on top of the Java client with more dependencies and extensions for JDBC-compliance.

## Usage

### Java Client

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <!-- or clickhouse-grpc-client if you prefer gRPC -->
    <artifactId>clickhouse-http-client</artifactId>
    <version>0.3.2</version>
</dependency>
```

<details>
    <summary>Expand to see example...</summary>

    ```Java
    // declare a server to connect to
    ClickHouseNode server = ClickHouseNode.of("server.domain", ClickHouseProtocol.HTTP, 8123, "my_db");

    // run multiple queries in one go and wait until they're completed
    ClickHouseClient.send(server, "create database if not exists test",
        "use test", // change current database from my_db to test
        "create table if not exists test_table(s String) engine=Memory",
        "insert into test_table values('1')('2')('3')",
        "select * from test_table limit 1",
        "truncate table test_table",
        "drop table if exists test_table").get();

    // query with named parameters
    try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.GRPC);
        ClickHouseResponse resp = client.connect(server)
            .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).set("send_logs_level", "trace")
            .query("select id, name from some_table where id in :ids and name like :name").params(Arrays.asList(1,2,3), "%key%").execute().get()) {
        // you can also use resp.stream() as well
        for (ClickHouseRecord record : resp.records()) {
            int id = record.getValue(0).asInteger();
            String name = record.getValue(1).asString();
        }

        ClickHouseResponseSummary summary = resp.getSummary();
        long totalRows = summary.getRows();
    }

    // load data with custom writer
    ClickHouseClient.load(server, "target_table", ClickHouseFormat.TabSeparated,
        ClickHouseCompression.NONE, new ClickHouseWriter() {
            @Override
            public void write(OutputStream output) throws IOException {
                output.write("1\t\\N\n".getBytes());
                output.write("2\t123".getBytes());
            }
        }).get();
    ```
</details>


### JDBC Driver

```xml
<dependency>
    <!-- ru.yandex.clickhouse will be retired starting from 0.4.0 -->
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>0.3.2</version>
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

URL Syntax: `jdbc:(clickhouse|ch)[:(grpc|http)]://<host>:[<port>][/<database>[?param1=value1&param2=value2]]`
  - `jdbc:ch:grpc://localhost` is same as `jdbc:clickhouse:grpc://localhost:9100`
  - `jdbc:ch://localhost/test?socket_timeout=120000` is same as `jdbc:clickhouse:http://localhost:8123/test?socket_timeout=120000`

JDBC Driver Class: `com.clickhouse.jdbc.ClickHouseDriver` (will remove `ru.yandex.clickhouse.ClickHouseDriver` starting from 0.4.0)

For example:

```java
String url = "jdbc:ch://localhost/test";
Properties properties = new Properties();
// set connection options - see more defined in ClickHouseConnectionSettings
properties.setClientName("Agent #1");
...
// set default request options - more in ClickHouseQueryParam
properties.setSessionId("default-session-id");
...

ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties)
String sql = "select * from mytable";
Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<>();
// set request options, which will override the default ones in ClickHouseProperties
additionalDBParams.put(ClickHouseQueryParam.SESSION_ID, "new-session-id");
...
try (ClickHouseConnection conn = dataSource.getConnection();
    ClickHouseStatement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(sql)) {
    ...
}
```

### Extended API

In order to provide non-JDBC complaint data manipulation functionality, proprietary API exists.
Entry point for API is `ClickHouseStatement#write()` method.

#### Importing file into table

```java
import ru.yandex.clickhouse.ClickHouseStatement;
ClickHouseStatement sth = connection.createStatement();
sth
    .write() // Write API entrypoint
    .table("default.my_table") // where to write data
    .option("format_csv_delimiter", ";") // specific param
    .data(new File("/path/to/file.csv.gz"), ClickHouseFormat.CSV, ClickHouseCompression.gzip) // specify input
    .send();
```

#### Configurable send

```java
import ru.yandex.clickhouse.ClickHouseStatement;
ClickHouseStatement sth = connection.createStatement();
sth
    .write()
    .sql("INSERT INTO default.my_table (a,b,c)")
    .data(new MyCustomInputStream(), ClickHouseFormat.JSONEachRow)
    .dataCompression(ClickHouseCompression.brotli)
    .addDbParam(ClickHouseQueryParam.MAX_PARALLEL_REPLICAS, 2)
    .send();
```

#### Send data in binary formatted with custom user callback

```java
import ru.yandex.clickhouse.ClickHouseStatement;
ClickHouseStatement sth = connection.createStatement();
sth.write().send("INSERT INTO test.writer", new ClickHouseStreamCallback() {
    @Override
    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
        for (int i = 0; i < 10; i++) {
            stream.writeInt32(i);
            stream.writeString("Name " + i);
        }
    }
},
ClickHouseFormat.RowBinary); // RowBinary or Native are supported
```

## Compatibility

Java 8 or higher is required in order to use Java client and/or JDBC driver.

### Data Format

`RowBinary` is preferred format in Java client, while JDBC driver uses `TabSeparated`.

### Data Type

| Data Type(s)       | Java Client | JDBC Driver | Remark                                                                |
| ------------------ | ----------- | ----------- | --------------------------------------------------------------------- |
| Date\*             | Y           | Y           |                                                                       |
| DateTime\*         | Y           | Y           |                                                                       |
| Decimal\*          | Y           | Y           | `SET output_format_decimal_trailing_zeros=1` in 21.9+ for consistency |
| Enum\*             | Y           | Y           | Treated as integer                                                    |
| Int*, UInt*        | Y           | Y           | UInt64 is mapped to `long`                                            |
| IPv\*              | Y           | Y           |                                                                       |
| Geo Types          | Y           | N           |                                                                       |
| \*String           | Y           | Y           |                                                                       |
| UUID               | Y           | Y           |                                                                       |
| AggregatedFunction | N           | N           | Partially supported                                                   |
| Array              | Y           | Y           |                                                                       |
| Map                | Y           | Y           |                                                                       |
| Nested             | Y           | N           |                                                                       |
| Tuple              | Y           | N           |                                                                       |

### Server Version

All [active releases](../ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) are supported. You can still use the JDBC driver for older versions like 18.14 or 19.16, but please keep in mind that they're no longer supported.

## Build with Maven

Use `mvn clean verify` to compile, test and generate packages if you're using JDK 8.

If you want to make a multi-release jar file(see [JEP-238](https://openjdk.java.net/jeps/238)), you'd better use JDK 11 or higher version like 17 with below command line:

```bash
mvn --global-toolchains .github/toolchains.xml -Drelease clean verify
```

## Testing

By default, docker container will be created automatically during integration test. You can pass system property like `-DclickhouseVersion=21.8` to test against specific version of ClickHouse.

In the case you prefer to test against an existing server, please follow instructions below:

- make sure the server can be accessed using default account(`default` user without password), which has both DDL and DML privileges
- add below two configuration files to the existing server and expose all ports for external access
  - [ports.xml](./tree/master/clickhouse-client/src/test/resources/containers/clickhouse-server/config.d/ports.xml) - enable all ports
  - and [users.xml](./tree/master/clickhouse-client/src/test/resources/containers/clickhouse-server/users.d/users.xml) - accounts used for integration test
- put `test.properties` under either `~/.m2/clickhouse` or `src/test/resources` of your project, with content like below:
  ```properties
  clickhouseServer=127.0.0.1
  # below properties are only useful for test containers
  #clickhouseVersion=latest
  #clickhouseTimezone=UTC
  #clickhouseImage=clickhouse/clickhouse-server
  #additionalPackages=
  ```
