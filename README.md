ClickHouse JDBC driver
===============
[![clickhouse-jdbc](https://maven-badges.herokuapp.com/maven-central/ru.yandex.clickhouse/clickhouse-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ru.yandex.clickhouse/clickhouse-jdbc) ![Build Status(https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg)](https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=ClickHouse_clickhouse-jdbc&metric=coverage)](https://sonarcloud.io/dashboard?id=ClickHouse_clickhouse-jdbc)

This is a basic and restricted implementation of jdbc driver for ClickHouse.
It has support of a minimal subset of features to be usable.


### Usage
```xml
<dependency>
    <groupId>ru.yandex.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>0.3.2</version>
</dependency>
```

use shaded
```xml
<!-- https://mvnrepository.com/artifact/ru.yandex.clickhouse/clickhouse-jdbc -->
<dependency>
    <groupId>ru.yandex.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <classifier>shaded</classifier>
    <version>0.3.2</version>
</dependency>
```

URL syntax: 
`jdbc:clickhouse://<host>:<port>[/<database>]`, e.g. `jdbc:clickhouse://localhost:8123/test`

JDBC Driver Class:
`ru.yandex.clickhouse.ClickHouseDriver`

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


### Supported Server Versions
All [active releases](../ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) are supported. You can still use the driver for older versions like 18.14 or 19.16 but please keep in mind that they're no longer supported.


### Compiling with maven
The driver is built with maven.
`mvn package -DskipTests=true`

To build a jar with dependencies use

`mvn package assembly:single -DskipTests=true`


### Build requirements
In order to build the jdbc client one need to have jdk 1.8 or higher.
