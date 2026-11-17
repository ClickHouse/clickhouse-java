# Connect a Java application to ClickHouse

## Java Client

### Installation

The client artifact is [`com.clickhouse:client-v2`](https://central.sonatype.com/artifact/com.clickhouse/client-v2).

### Usage Example

```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;

public Client createClient() {
    return new Client.Builder()
            .addEndpoint("https://your-instance.clickhouse.cloud:8443")
            .setUsername("default")
            .setPassword("secret")
            .setDefaultDatabase("app_db")
            .setClientName("my-app/1.0.0")
            .build();
}

public String readServerVersion(Client client) {
    List<GenericRecord> rows = client.queryAll("SELECT version() AS version");
    if (rows.isEmpty()) {
        return "";
    }
    return rows.get(0).getString("version");
}

public void printServerVersion() {
    try (Client client = createClient()) {
        System.out.println(readServerVersion(client));
    }
}
```

### Documentation

- [Java Client](https://clickhouse.com/docs/integrations/language-clients/java/client)
- [Setup](https://clickhouse.com/docs/integrations/language-clients/java/client#setup)
- [Initialization](https://clickhouse.com/docs/integrations/language-clients/java/client#initialization)

## JDBC

### Installation

The published driver artifact is [`com.clickhouse:clickhouse-jdbc`](https://central.sonatype.com/artifact/com.clickhouse/clickhouse-jdbc).

The standard artifact declares its libraries as normal Maven dependencies. Use the `all` classifier when the application cannot manage those libraries. The `all` jar relocates most third-party classes. It leaves `slf4j` and `micrometer` unshaded.

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>${clickhouse.driver.version}</version>
    <classifier>all</classifier>
</dependency>
```

### Usage Example

Set the driver class to `com.clickhouse.jdbc.Driver` when a pool or a framework asks for a class name. `DriverManager` loads `com.clickhouse.jdbc.ClickHouseDriver` from the jar.

```java
public Connection createConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", "default");
    props.setProperty("password", "secret");
    props.setProperty("client_name", "my-app/1.0.0");
    return DriverManager.getConnection(
            "jdbc:clickhouse:https://your-instance.clickhouse.cloud:8443/app_db",
            props);
}

public String readServerVersion(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT version()")) {
        if (!rs.next()) {
            return "";
        }
        return rs.getString(1);
    }
}

public void printServerVersion() throws SQLException {
    try (Connection conn = createConnection()) {
        System.out.println(readServerVersion(conn));
    }
}
```

### Documentation

- [JDBC driver](https://clickhouse.com/docs/integrations/language-clients/java/jdbc)
- [Setup](https://clickhouse.com/docs/integrations/language-clients/java/jdbc#setup)
- [Creating a connection](https://clickhouse.com/docs/integrations/language-clients/java/jdbc#creating-connection)
