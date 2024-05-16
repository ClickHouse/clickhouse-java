
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