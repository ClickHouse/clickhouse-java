
# Example of ClickHouse Client Usage 

This example application uses low level ClickHouse client to execute a simple query.


## Usage 

Apache Maven or IDE with Maven support is required to run this example.  

To compile:
```shell
mvn clean compile
```

To run:
```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.jdbc.Main"
```

Addition options can be passed to the application:
- `-DchPort` - ClickHouse server port (default: 8123)
- `-DchUser` - ClickHouse user name (default: default)
- `-DchPassword` - ClickHouse user password (default: empty)
- `-DchDatabase` - ClickHouse database name (default: default)


## Protobuf Example 

Class `com.clickhouse.examples.formats.ProtobufMain` demonstrates how to use Protobuf with ClickHouse.

### Setup 

First of all new table should be created in a DB:
```sql
CREATE TABLE ui_events
(
    `url` String,
    `user_id` String,
    `session_id` String,
    `timestamp` Int64,
    `duration` Int64 DEFAULT 0,
    `event` String,
)
ENGINE = MergeTree
ORDER BY timestamp;
```

To regenerate Java classes from Protobuf file:
```shell
 protoc --java_out=./src/main/java/ ./src/proto/ui_stats_event.proto
```

To compile:
```shell
mvn clean compile
```

To run:
```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.formats.ProtobufMain"
```

To run with a snapshot version of ClickHouse Java client (replace `0.6.0` with the actual version and use repository `https://oss.sonatype.org/content/repositories/snapshots`)
```shell
mvn exec:java -Dclickhouse-java.version=0.6.0-SNAPSHOT -Dexec.mainClass="com.clickhouse.examples.formats.ProtobufMain"
```


Addition options can be passed to the application:
- `-DchPort` - ClickHouse server port (default: 8123)
- `-DchSsl` - Set to `true` to use https (default: false)
- `-DchUser` - ClickHouse user name (default: default)
- `-DchPassword` - ClickHouse user password (default: empty)
- `-DchDatabase` - ClickHouse database name (default: default)
