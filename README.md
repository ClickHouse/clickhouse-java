[![clickhouse-jdbc](https://maven-badges.herokuapp.com/maven-central/ru.yandex.clickhouse/clickhouse-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ru.yandex.clickhouse/clickhouse-jdbc)
ClickHouse JDBC driver
===============

This is a basic and restricted implementation of jdbc driver for ClickHouse.
It has support of a minimal subset of features to be usable.

### URL syntax

`jdbc:clickhouse://host:port`

For example:
`jdbc:clickhouse://localhost:8123`

### Compiling with maven
The driver is built with maven.
`mvn package`

To build a jar with dependencies use

`mvn package assembly:single`

### Build requirements
In order to build the jdbc client one need to have jdk 1.6 or higher.
