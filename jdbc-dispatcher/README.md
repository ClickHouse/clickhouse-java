# JDBC Dispatcher

A JDBC driver proxy library that loads multiple versions of the same JDBC driver and provides automatic failover with configurable retry strategies.

## Why Is This Needed?

When working with databases in production environments, you may encounter situations where:

1. **Driver Version Incompatibilities**: A new driver version introduces a bug or regression that affects your specific use case, but you've already deployed it. You need a quick fallback mechanism.

2. **Gradual Migration**: You want to test a new driver version in production while having the ability to automatically fall back to the proven stable version if issues occur.

3. **High Availability Requirements**: Critical applications need resilience against driver-level failures. If one driver version has connectivity issues (e.g., due to a specific server version incompatibility), another version might work.

4. **A/B Testing Drivers**: You want to compare behavior or performance between driver versions in a controlled manner.

5. **Zero-Downtime Upgrades**: During driver upgrades, you want to seamlessly switch between versions without application restarts.

The JDBC Dispatcher solves these problems by:
- Loading multiple driver versions in isolated classloaders (no class conflicts)
- Wrapping JDBC connections, statements, and result sets in proxies
- Automatically retrying failed operations with different driver versions
- Supporting pluggable retry strategies

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application                               │
│                             │                                    │
│                      DispatcherDriver                            │
│                             │                                    │
│              ┌──────────────┼──────────────┐                     │
│              ▼              ▼              ▼                     │
│     ┌────────────┐  ┌────────────┐  ┌────────────┐              │
│     │ Driver v3  │  │ Driver v2  │  │ Driver v1  │              │
│     │ (newest)   │  │            │  │ (oldest)   │              │
│     └─────┬──────┘  └─────┬──────┘  └─────┬──────┘              │
│           │               │               │                      │
│     IsolatedCL #1   IsolatedCL #2   IsolatedCL #3               │
└───────────┼───────────────┼───────────────┼─────────────────────┘
            │               │               │
            ▼               ▼               ▼
        ┌───────────────────────────────────────┐
        │              Database                  │
        └───────────────────────────────────────┘
```

### Component Overview

| Component | Description |
|-----------|-------------|
| `DispatcherDriver` | Main entry point implementing `java.sql.Driver`. Manages driver versions and creates proxy connections. |
| `DriverVersionManager` | Loads and manages multiple driver versions. Extracts version info from JAR filenames. |
| `IsolatedClassLoader` | Child-first classloader that loads each driver version in isolation, preventing class conflicts. |
| `ConnectionProxy` | Wraps `java.sql.Connection`. Maintains connections to multiple driver versions for failover. |
| `StatementProxy` | Wraps `java.sql.Statement`. Implements retry logic for `executeQuery`, `executeUpdate`, and `execute`. |
| `PreparedStatementProxy` | Wraps `java.sql.PreparedStatement` with the same retry capabilities. |
| `ResultSetProxy` | Wraps `java.sql.ResultSet`. Tied to the specific execution that created it. |
| `RetryStrategy` | Interface for implementing different failover strategies. |

### Proxy Chain

When you call `dispatcher.connect()`, you receive a `ConnectionProxy`:

```
DispatcherDriver.connect()
        │
        ▼
   ConnectionProxy  ─────────────────┐
        │                            │
        │ createStatement()          │ Maintains connections to
        ▼                            │ multiple driver versions
   StatementProxy                    │
        │                            │
        │ executeQuery()             │
        │    ┌──────────────────────┐│
        │    │ Try version 3.0.0   ││
        │    │ If fails, try 2.0.0 ││
        │    │ If fails, try 1.0.0 ││
        │    └──────────────────────┘│
        ▼                            │
   ResultSetProxy  ◄─────────────────┘
```

### Retry Strategies

Three built-in strategies are provided:

#### NewestFirstRetryStrategy (Default)
Tries the newest driver version first, then falls back to older versions in descending order.

```java
// Version order: 3.0.0 → 2.0.0 → 1.0.0
new NewestFirstRetryStrategy(maxRetries, skipUnhealthy)
```

#### RoundRobinRetryStrategy
Rotates the starting version for each new operation, distributing load across all versions.

```java
// First call:  3.0.0 → 2.0.0 → 1.0.0
// Second call: 2.0.0 → 1.0.0 → 3.0.0
// Third call:  1.0.0 → 3.0.0 → 2.0.0
new RoundRobinRetryStrategy(maxRetries)
```

#### FailoverOnlyRetryStrategy
Sticks to one preferred version until it fails, then switches to the next available version.

```java
// Uses preferred version until failure, then picks a new preferred
new FailoverOnlyRetryStrategy(maxRetries)
```

### Health Tracking

Each driver version tracks its health status:
- When an operation fails, the version is marked **unhealthy**
- Unhealthy versions are deprioritized in retry ordering
- After a configurable cooldown period (default: 60 seconds), versions are automatically reconsidered

## Usage Examples

### Basic Setup

```java
import com.clickhouse.jdbc.dispatcher.DispatcherDriver;
import com.clickhouse.jdbc.dispatcher.DriverVersion;

// Create dispatcher for a specific driver class
DispatcherDriver dispatcher = new DispatcherDriver("com.clickhouse.jdbc.ClickHouseDriver");

// Load driver versions from JAR files
dispatcher.loadDriver(new File("libs/clickhouse-jdbc-0.4.6.jar"), "0.4.6");
dispatcher.loadDriver(new File("libs/clickhouse-jdbc-0.5.0.jar"), "0.5.0");
dispatcher.loadDriver(new File("libs/clickhouse-jdbc-0.6.0.jar"), "0.6.0");

// Connect - uses newest version first (0.6.0), fails over if needed
Properties props = new Properties();
props.setProperty("user", "default");
props.setProperty("password", "");

Connection conn = dispatcher.connect("jdbc:clickhouse://localhost:8123/default", props);

// Use connection normally - retry logic is transparent
try (Statement stmt = conn.createStatement()) {
    ResultSet rs = stmt.executeQuery("SELECT version()");
    while (rs.next()) {
        System.out.println("ClickHouse version: " + rs.getString(1));
    }
}
```

### Load All Versions from Directory

```java
DispatcherDriver dispatcher = new DispatcherDriver("com.clickhouse.jdbc.ClickHouseDriver");

// Automatically loads all JARs and extracts versions from filenames
// e.g., "clickhouse-jdbc-0.5.0.jar" → version "0.5.0"
int loaded = dispatcher.loadFromDirectory(new File("/opt/drivers/clickhouse"));
System.out.println("Loaded " + loaded + " driver versions");

Connection conn = dispatcher.connect("jdbc:clickhouse://localhost:8123", new Properties());
```

### Custom Retry Strategy

```java
import com.clickhouse.jdbc.dispatcher.strategy.RoundRobinRetryStrategy;

// Use round-robin with max 2 retries
DispatcherDriver dispatcher = new DispatcherDriver(
    "com.clickhouse.jdbc.ClickHouseDriver",
    new RoundRobinRetryStrategy(2)
);

dispatcher.loadFromDirectory(new File("/opt/drivers"));
```

### Using with DriverManager

```java
DispatcherDriver dispatcher = new DispatcherDriver("com.clickhouse.jdbc.ClickHouseDriver");
dispatcher.loadFromDirectory(new File("/opt/drivers"));

// Register with DriverManager
dispatcher.register();

// Now you can use DriverManager with the dispatcher URL prefix
Connection conn = DriverManager.getConnection(
    "jdbc:dispatcher:jdbc:clickhouse://localhost:8123",
    "default",
    ""
);

// Don't forget to deregister when done
dispatcher.deregister();
```

### Accessing Version Information

```java
DispatcherDriver dispatcher = new DispatcherDriver("com.clickhouse.jdbc.ClickHouseDriver");
dispatcher.loadFromDirectory(new File("/opt/drivers"));

// Get all loaded versions
for (DriverVersion version : dispatcher.getVersionManager().getVersions()) {
    System.out.printf("Version %s: healthy=%s, major=%d, minor=%d%n",
        version.getVersion(),
        version.isHealthy(),
        version.getMajorVersion(),
        version.getMinorVersion());
}

// Get newest version
DriverVersion newest = dispatcher.getVersionManager().getNewestVersion();
System.out.println("Newest version: " + newest.getVersion());

// Manually mark a version as unhealthy
dispatcher.getVersionManager().markUnhealthy("0.5.0");
```

### Handling Dispatcher Exceptions

```java
try {
    Connection conn = dispatcher.connect("jdbc:clickhouse://localhost:8123", props);
} catch (DispatcherException e) {
    System.err.println("All versions failed: " + e.getMessage());
    
    // Inspect individual failures
    for (DispatcherException.VersionFailure failure : e.getFailures()) {
        System.err.printf("  Version %s failed: %s%n",
            failure.getVersion(),
            failure.getException().getMessage());
    }
}
```

## Limitations

### 1. Transaction Isolation
Transactions are tied to a specific connection. If a failover occurs mid-transaction, the new connection will not have the same transaction state. **Do not rely on failover within an active transaction.**

```java
conn.setAutoCommit(false);
stmt.executeUpdate("INSERT INTO t VALUES (1)");
// If failover happens here, the INSERT is lost!
stmt.executeUpdate("INSERT INTO t VALUES (2)");
conn.commit();
```

**Recommendation**: Keep transactions short and handle `DispatcherException` by retrying the entire transaction.

### 2. PreparedStatement Parameter Binding
When using `PreparedStatement`, if a failover occurs, parameter bindings are **not** automatically replayed on the new statement. The current implementation requires you to re-bind parameters.

### 3. ResultSet Failover
ResultSets are tied to the specific Statement execution that created them. If you iterate through a ResultSet and the connection fails, you cannot failover mid-iteration—the entire query must be re-executed.

### 4. Connection State Synchronization
Connection properties (catalog, schema, transaction isolation, etc.) are synchronized across all version connections when set. However, if a new version connection is created during failover, it starts with default properties and may not match the original connection's state.

### 5. Stored Procedures and Callable Statements
`CallableStatement` is not wrapped with retry logic. It delegates directly to the underlying connection.

### 6. Version Extraction from Filenames
The `loadFromDirectory` method extracts versions from JAR filenames using a regex pattern. It expects formats like:
- `driver-1.2.3.jar`
- `driver-1.2.3-SNAPSHOT.jar`
- `driver_1.2.jar`

Non-standard naming may require manual version specification using `loadDriver(file, version)`.

### 7. Memory Usage
Each loaded driver version maintains its own classloader and potentially its own connection. Loading many versions or maintaining many failover connections increases memory usage.

### 8. Driver Compatibility
All loaded driver versions must be compatible with the target database server. The dispatcher cannot help if all versions are incompatible with the server.

### 9. Thread Safety
The proxy classes are designed for concurrent use, but individual JDBC objects (Connection, Statement, ResultSet) should follow standard JDBC thread-safety guidelines—typically, don't share them across threads.

### 10. No Automatic Retry for Reads After Writes
The dispatcher does not track read-after-write consistency. If you write with version A and it fails over to version B for reads, there's no guarantee of consistency (this depends on your database, not the dispatcher).

## Configuration

### DriverVersionManager Options

| Option | Default | Description |
|--------|---------|-------------|
| `driverClassName` | (required) | Fully qualified class name of the JDBC driver |
| `healthCheckCooldownMs` | 60000 | Time in ms before unhealthy versions are reconsidered |

### Retry Strategy Options

| Strategy | Option | Default | Description |
|----------|--------|---------|-------------|
| NewestFirst | `maxRetries` | 3 | Maximum number of versions to try |
| NewestFirst | `skipUnhealthy` | true | Whether to skip unhealthy versions on first pass |
| RoundRobin | `maxRetries` | 3 | Maximum number of versions to try |
| FailoverOnly | `maxRetries` | 3 | Maximum number of versions to try |

## Building

```bash
cd jdbc-dispatcher
mvn clean install
```

## Dependencies

- Java 17+
- SLF4J API (for logging)

## License

Apache License 2.0
