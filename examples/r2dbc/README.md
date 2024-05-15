
# Example of ClickHouse Reactive Client Usage 

This type of client still uses HTTP protocol to communicate with ClickHouse server, 
but it has another mechanism of working with IO - when CPU time utilizes more efficiently 
by avoiding blocking operations.
DB interface is exposed via [r2dbc](https://r2dbc.io/) specification.

## Usage 

Apache Maven or IDE with Maven support is required to run this example.  

To compile:
```shell
mvn clean compile
```

Init DB (from resource/init.sql):
```sql
create database clickdb;

create table if not exists clickdb.clicks
(
    domain String,
    path String,
    cdate DateTime,
    count UInt64
)
engine = SummingMergeTree(count)
order by (domain, path, cdate);
```

To run application :
```shell
cd clickhouse-r2dbc-spring-webflux-sample
mvn exec:java -Dclickhouse.database=default -Dexec.mainClass="com.clickhouse.r2dbc.spring.webflux.sample.Application"

```

To test the application (run in another terminal): 
```shell
# add clicks
curl -v -X POST -H "application/json" -d '{"domain": "example.org", "path": "/test"}' http://localhost:8080/clicks

# get counters
curl -v http://localhost:8080/clicks/example.org/
```

Addition options can be passed to the application:
- `-Dclickhouse.host` - ClickHouse server host
- `-Dclickhouse.port` - ClickHouse server port
- `-Dclickhouse.user` - ClickHouse user name
- `-Dclickhouse.password` - ClickHouse user password
- `-Dclickhouse.database` - ClickHouse database name
