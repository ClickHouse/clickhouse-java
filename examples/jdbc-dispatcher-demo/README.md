# JDBC Dispatcher Demo

This example demonstrates how to use the **JDBC Dispatcher** module to load and manage multiple versions of the ClickHouse JDBC driver with automatic failover capabilities.

## Overview

The JDBC Dispatcher allows you to:
- Load multiple JDBC driver versions in isolated classloaders
- Automatically failover between driver versions when operations fail
- Use different retry strategies (NewestFirst, RoundRobin, FailoverOnly)
- Integrate with standard JDBC DriverManager

## Prerequisites

1. **Java 17+** installed
2. **ClickHouse server** running on `localhost:8123`

   Start a ClickHouse server using Docker:
   ```bash
   docker run -d -p 8123:8123 --name clickhouse clickhouse/clickhouse-server
   ```

3. **Build the parent project** (to install jdbc-dispatcher to local Maven repository):
   ```bash
   cd ../..
   mvn clean install -DskipTests
   ```

## Project Structure

```
jdbc-dispatcher-demo/
├── build.gradle.kts          # Gradle build configuration
├── settings.gradle.kts       # Gradle settings
├── gradle.properties         # Project properties
├── drivers/                  # Downloaded driver JARs (created by downloadDrivers task)
│   ├── clickhouse-jdbc-0.9.6-all.jar
│   └── clickhouse-jdbc-0.7.2-all.jar
└── src/main/java/
    └── com/clickhouse/examples/dispatcher/
        └── DispatcherDemo.java
```

## Running the Demo

### Step 1: Download Driver JARs

The build will automatically download the required driver versions from GitHub releases:

```bash
./gradlew downloadDrivers
```

This downloads:
- `clickhouse-jdbc-0.9.6-all.jar` (latest version)
- `clickhouse-jdbc-0.7.2-all.jar` (older version)

### Step 2: Run the Demo

```bash
./gradlew run
```

## Running the HTTP Service

There's also a simple HTTP backend service that demonstrates jdbc-dispatcher in a web context.
It uses only JDK built-in tools (`com.sun.net.httpserver.HttpServer`) - no external frameworks.

### Start the Service

```bash
./gradlew runService
```

The service starts on **http://localhost:8080**.

### Available Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check - returns service status |
| `GET /version` | Returns ClickHouse server version |
| `GET /drivers` | Lists all loaded driver versions with health status |
| `GET /query?sql=SELECT...` | Executes a SELECT query and returns JSON results |

### Example Requests

```bash
# Health check
curl http://localhost:8080/health

# Get ClickHouse version
curl http://localhost:8080/version

# List loaded drivers
curl http://localhost:8080/drivers

# Execute a query
curl "http://localhost:8080/query?sql=SELECT%20number%20FROM%20system.numbers%20LIMIT%205"

# Query with more complex SQL
curl "http://localhost:8080/query?sql=SELECT%20name,%20value%20FROM%20system.settings%20LIMIT%2010"
```

### Example Responses

**GET /health**
```json
{"status":"ok","service":"jdbc-dispatcher-demo"}
```

**GET /version**
```json
{"clickhouse_version":"24.8.1.1","server_time":"2026-02-02 21:30:00","status":"connected"}
```

**GET /drivers**
```json
{
  "drivers": [
    {"version":"0.9.6","healthy":"true","major":"0","minor":"9"},
    {"version":"0.7.2","healthy":"true","major":"0","minor":"7"}
  ],
  "newest": "0.9.6",
  "count": 2
}
```

**GET /query?sql=SELECT...**
```json
{
  "columns": ["number"],
  "rows": [[0],[1],[2],[3],[4]],
  "row_count": 5
}
```

## What the Demo Shows

The demo runs four scenarios:

### Demo 1: Basic Usage
- Loads all driver JARs from the `drivers/` directory
- Connects to ClickHouse and executes simple queries
- Shows transparent failover handling

### Demo 2: Retry Strategies
- **NewestFirstRetryStrategy**: Tries newest version first, then older versions
- **RoundRobinRetryStrategy**: Rotates starting version for load distribution

### Demo 3: DriverManager Integration
- Registers the dispatcher with `java.sql.DriverManager`
- Uses standard JDBC URL with `jdbc:dispatcher:` prefix
- Properly deregisters when done

### Demo 4: Version Inspection
- Lists all loaded driver versions
- Shows health status of each version
- Demonstrates marking versions as unhealthy

## Configuration

### Changing ClickHouse Connection

Edit `DispatcherDemo.java`:

```java
private static final String CLICKHOUSE_URL = "jdbc:clickhouse://your-host:8123/your-database";
```

### Using Different Driver Versions

Edit `build.gradle.kts` to modify the `downloadDrivers` task:

```kotlin
val drivers = mapOf(
    "clickhouse-jdbc-0.9.6-all.jar" to "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.9.6/clickhouse-jdbc-0.9.6-all.jar",
    "clickhouse-jdbc-0.8.0-all.jar" to "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.8.0/clickhouse-jdbc-0.8.0-all.jar"
)
```

### Retry Strategy Configuration

```java
// Maximum 3 retries, skip unhealthy versions on first pass
new NewestFirstRetryStrategy(3, true)

// Maximum 2 retries with round-robin
new RoundRobinRetryStrategy(2)

// Failover-only strategy
new FailoverOnlyRetryStrategy(3)
```

## Expected Output

```
=== JDBC Dispatcher Demo ===

--- Demo 1: Basic Usage ---
Loaded 2 driver versions from /path/to/drivers
Connected to ClickHouse version: 24.x.x.x
Query result: 2 at 2026-02-02 ...
Demo 1 completed successfully

--- Demo 2: Retry Strategies ---
Using NewestFirstRetryStrategy...
  [NewestFirst] Query executed at: ...
Using RoundRobinRetryStrategy...
  [RoundRobin-1] Query executed at: ...
  [RoundRobin-2] Query executed at: ...
  [RoundRobin-3] Query executed at: ...
Demo 2 completed successfully

--- Demo 3: DriverManager Integration ---
Dispatcher registered with DriverManager
DriverManager query result: Hello from DriverManager!
Demo 3 completed successfully
Dispatcher deregistered from DriverManager

--- Demo 4: Version Inspection ---
Loaded driver versions:
  - Version 0.9.6: healthy=true, major=0, minor=9
  - Version 0.7.2: healthy=true, major=0, minor=7
Newest version: 0.9.6
Marking version 0.7.2 as unhealthy for demonstration...
Updated version status:
  - Version 0.9.6: healthy=true
  - Version 0.7.2: healthy=false
Query with unhealthy version marked: Still working!
Demo 4 completed successfully

=== All demos completed successfully! ===
```

## Troubleshooting

### "Drivers directory not found"
Run `./gradlew downloadDrivers` to download the driver JARs.

### Connection refused
Ensure ClickHouse is running on `localhost:8123`:
```bash
docker ps | grep clickhouse
# If not running:
docker start clickhouse
```

### "jdbc-dispatcher not found"
Build the parent project first:
```bash
cd ../..
mvn clean install -DskipTests
```

## Learn More

- [JDBC Dispatcher README](../../jdbc-dispatcher/README.md) - Full documentation
- [ClickHouse JDBC Releases](https://github.com/ClickHouse/clickhouse-java/releases) - Available driver versions
