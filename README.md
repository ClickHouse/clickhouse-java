ClickHouse JDBC driver
===============

This is a basic and restricted implementation of jdbc driver for ClickHouse.
It has support of a minimal subset of features to be usable.

### URL syntax

`jdbc:clickhouse:host:port`

For example:
`jdbc:clickhouse:localhost:8123`

### Compiling with maven

`mvn package assembly:single`

### Build requirements
In order to build the jdbc client one need to have jdk 1.6.

### Maven:
```
<dependency>
    <groupId>ru.yandex.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>1.1.0</version>
</dependency>
```
