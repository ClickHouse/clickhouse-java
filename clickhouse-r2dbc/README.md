# clickhouse-r2dbc

This module provides r2dbc support to clickhouse-jdbc driver.

r2dbc link : https://r2dbc.io/

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

for full example please check clickhouse-jdbc/examples/clickhouse-r2dbc-samples/clickhouse-r2dbc-spring-webflux-sample .