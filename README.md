ClickHouse JDBC driver
===============
[![clickhouse-jdbc](https://maven-badges.herokuapp.com/maven-central/ru.yandex.clickhouse/clickhouse-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ru.yandex.clickhouse/clickhouse-jdbc) ![Build Status(https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg)](https://github.com/ClickHouse/clickhouse-jdbc/workflows/Build/badge.svg)

This is a basic and restricted implementation of jdbc driver for ClickHouse.
It has support of a minimal subset of features to be usable.

### Usage
```xml
<dependency>
    <groupId>ru.yandex.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>0.2.5</version>
</dependency>
```

URL syntax: 
`jdbc:clickhouse://<host>:<port>[/<database>]`, e.g. `jdbc:clickhouse://localhost:8123/test`

JDBC Driver Class:
`ru.yandex.clickhouse.ClickHouseDriver`

additionally, if you have a few instances, you can use `BalancedClickhouseDataSource`.


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
    .data(new File("/path/to/file.csv"), ClickHouseFormat.CSV) // specify input
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
### Compiling with maven
The driver is built with maven.
`mvn package -DskipTests=true`

To build a jar with dependencies use

`mvn package assembly:single -DskipTests=true`

### Build requirements
In order to build the jdbc client one need to have jdk 1.7 or higher.
