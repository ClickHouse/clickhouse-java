
# Example of ClickHouse JDBC Client Usage 

This example application uses JDBC to interact with ClickHouse Server.


## Usage 

Apache Maven or IDE with Maven support is required to run this example.  

To compile:
```shell
mvn clean compile
```

To run simplified example:
```shell
 mvn exec:java -Dexec.mainClass="com.clickhouse.examples.jdbc.Basic"
```

To run example with jdbc-all packaging (fat-jar):
```shell
 mvn -Pfatjar exec:java -Dexec.mainClass="com.clickhouse.examples.jdbc.Basic"
```

To run example with jdbc-shaded packaging:
```shell
 mvn -Pshaded exec:java -Dexec.mainClass="com.clickhouse.examples.jdbc.Basic"
```

Addition options can be passed to the application:
- `-DchUrl` - ClickHouse JDBC URL. Default is `jdbc:clickhouse://localhost:8123/default`