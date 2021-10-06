# ClickHouse Java Client & JDBC Driver

[![clickhouse-jdbc](https://maven-badges.herokuapp.com/maven-central/ru.yandex.clickhouse/clickhouse-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ru.yandex.clickhouse/clickhouse-jdbc) ![Build Status(https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg)](https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=ClickHouse_clickhouse-jdbc&metric=coverage)](https://sonarcloud.io/dashboard?id=ClickHouse_clickhouse-jdbc)

Java client and JDBC driver for ClickHouse.

## Usage

### Java Client

Use Java client when you prefer async and more "direct" way to communicate with ClickHouse. JDBC driver is actually a thin wrapper of the Java client.

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <!-- you'll be able to use clickhouse-http-client and clickhouse-tcp-client as well in the near future -->
    <artifactId>clickhouse-grpc-client</artifactId>
    <version>0.3.2</version>
</dependency>
```

Example:

```Java
// declare a server to connect to
ClickHouseNode server = ClickHouseNode.of("server.domain", ClickHouseProtocol.GRPC, 9100, "my_db");

// run multiple queries in one go and wait until it's finished
ClickHouseClient.send(server,
    "create database if not exists another_database",
    "use another_database", // change current database from my_db to test
    "create table if not exists my_table(s String) engine=Memory",
    "insert into my_table values('1')('2')('3')",
    "select * from my_table limit 1",
    "truncate table my_table",
    "drop table if exists my_table").get();

// query with named parameters
try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.GRPC);
    ClickHouseResponse resp = client.connect(server)
        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).set("send_logs_level", "trace")
        .query("select id, name from some_table where id in :ids and name like :name").params(Arrays.asList(1,2,3), "%key%").execute().get()) {
    // you can also use resp.recordStream() as well
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

### JDBC Driver

```xml
<dependency>
    <!-- groupId and package name will be changed to com.clickhouse starting from 0.4.0 -->
    <groupId>ru.yandex.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>0.3.2</version>
</dependency>
```

URL syntax: `jdbc:clickhouse://<host>:<port>[/<database>[?param1=value1&param2=value2]]`, e.g. `jdbc:clickhouse://localhost:8123/test?socket_timeout=120000`

JDBC Driver Class: `ru.yandex.clickhouse.ClickHouseDriver` (will be changed to `com.clickhouse.jdbc.ClickHouseDriver` starting from 0.4.0)

For example:

```java
String url = "jdbc:clickhouse://localhost:8123/test";
ClickHouseProperties properties = new ClickHouseProperties();
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
    ResultSet rs = stmt.executeQuery(sql, additionalDBParams)) {
    ...
}
```

Additionally, if you have a few instances, you can use `BalancedClickhouseDataSource`.

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

TODO

### Data Type

TODO: a table

### Server Version

All [active releases](../ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) are supported. You can still use the JDBC driver for older versions like 18.14 or 19.16 but please keep in mind that they're no longer supported.

## Build with Maven

### JDK 8

Use below command line to compile, test and generate packages.
`mvn clean verify`

### JDK 11+

You need a [toolchains.xml](https://maven.apache.org/guides/mini/guide-using-toolchains.html) under `<home directory>/.m2/`, as multi-release jar will be generated.

`mvn -Drelease clean verify`

## Testing

By default, docker container will be created automatically for integration test. In case you need to test against an existing server, you may put `test.properties` under either test/resources or <home directory>/.m2/clickhouse with content like below:

```properties
clickhouseServer=127.0.0.1
clickhouseVersion=21.9
```
