# ClickHouse R2DBC Driver

[R2DBC](https://r2dbc.io/) wrapper of async [Java client](/ClickHouse/clickhouse-java/clickhouse-client) for ClickHouse.

## Usage

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <!-- change to clickhouse-r2dbc_0.9.1 for SPI 0.9.1.RELEASE -->
    <artifactId>clickhouse-r2dbc</artifactId>
    <version>0.4.1</version>
    <!-- use uber jar with all dependencies included, change classifier to http or grpc for smaller jar -->
    <classifier>all</classifier>
    <exclusions>
        <exclusion>
            <groupId>*</groupId>
            <artifactId>*</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

Sample code:

```java
ConnectionFactory connectionFactory = ConnectionFactories
        .get("r2dbc:clickhouse:http://{username}:{password}@{host}:{port}/{database}");

        Mono.from(connectionFactory.create())
        .flatMapMany(connection -> connection
        .createStatement("select domain, path,  toDate(cdate) as d, count(1) as count from clickdb.clicks where domain = :domain group by domain, path, d")
        .bind("domain", domain)
        .execute())
        .flatMap(result -> result
        .map((row, rowMetadata) -> String.format("%s%s[%s]:%d", row.get("domain", String.class),
                                                                row.get("path", String.class),
                                                                row.get("d", LocalDate.class),
                                                                row.get("count", Long.class)) ))
        .doOnNext(System.out::println)
        .subscribe();
```

For full example please check [here](/ClickHouse/clickhouse-java/examples/r2dbc).
