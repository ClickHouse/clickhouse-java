# ClickHouse JDBC Integration Guide

This guide is a **step-by-step integration path** for the **JDBC Driver V2** (`jdbc-v2`, published as `com.clickhouse:clickhouse-jdbc`). It is written to be used as context for building an application or a downstream integration spec: each step states the decisions you must make, how to configure them, and the common pitfalls to avoid.

**Prerequisites:** Read [integration-common.md](integration-common.md) to understand the JDBC trade-offs before committing to this path.


> **Architecture in one line.** Every JDBC `Connection` wraps a `client-v2` [`Client`](../client-v2/src/main/java/com/clickhouse/client/api/Client.java) internally. JDBC is not a separate protocol stack — it is a `java.sql.*` façade over the Java Client.
>
> ```
> Application → ConnectionImpl → Client → HTTP pool → ClickHouse
> ```

> **Configuration philosophy.** This guide names only the properties relevant to each step. The exhaustive lists live in [`DriverProperties`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/DriverProperties.java), [`ClientConfigProperties`](../client-v2/src/main/java/com/clickhouse/client/api/ClientConfigProperties.java), and the official docs. Configuration splits into two groups:
> - **Init configuration** — set once via the JDBC URL or `Properties`: endpoint, connection pool size, authentication, TLS. Covered in Steps 1–3.
> - **Operation configuration** — set per statement or as connection defaults: fetch size, timeouts, batch behavior, dedup tokens. Covered in Steps 4–6.
>
> Property routing (see [`DriverProperties`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/DriverProperties.java)): if a property is a JDBC-specific driver property it is handled by the driver; **all other properties are forwarded to `ClientConfigProperties`**.

## Artifacts

The driver is published to Maven Central as **`com.clickhouse:clickhouse-jdbc`**. 

Two distributions are published under the same artifact:

- **Standard artifact** (default, no classifier) — the driver together with its dependencies declared as ordinary transitive Maven dependencies. Recommended for managed builds in which the application controls the dependency tree.
- **Shaded artifact** (`all` classifier) — a single self-contained archive that bundles and **relocates** most third-party dependencies. Recommended when transitive dependencies cannot be managed.

---

## Development Environment 

### Java Version

The ClickHouse JDBC driver requires **Java 8 or newer**. All mainline development and testing are validated on recent LTS versions (Java 8, 11, 17, and 21 as available). We recommend using up-to-date LTS releases, as feature coverage, TLS support, and performance are best with newer runtimes.

### Local ClickHouse for Development

For local development and rapid prototyping, you can run ClickHouse in a container with minimal setup:

```sh
docker run --rm -d -p 8123:8123 --name clickhouse-server clickhouse/clickhouse-server:latest
```

This launches a fresh ClickHouse server, with the HTTP API available at `localhost:8123`. The default database is `default` and no password is required for the default user (`default`). This is ideal for smoke tests, schema exploration, and validating basic integration.

You can also use any recent [official ClickHouse binary](https://clickhouse.com/docs/en/getting-started/install/) or use Docker Compose for more complex setups.

> **Note:** Local environments are best for initial integration, experiments, and CI pipelines — not for realistic scale, performance, or operational requirements.

### ClickHouse Cloud (Recommended for Production-like Integration)

While local ClickHouse is excellent for early development, **we strongly recommend using [ClickHouse Cloud](https://clickhouse.com/cloud/)** for end-to-end integration and before considering your application production ready.

ClickHouse Cloud environments mirror real-world production settings:

- Managed authentication, scaling, secure TLS endpoints
- Production-like network performance and stability
- Long-running, stateful databases without the churn or "works on my machine" surprises

Using ClickHouse Cloud during integration helps uncover configuration or authentication nuances, avoid brittle local-only assumptions, and validates your app against true cloud operations. All features described in this guide are supported equally, and connection examples are identical — just provide your cloud hostname, port, and TLS-enabled URL in the JDBC connection string.

> **Best practice:** Validate your integration against ClickHouse Cloud regularly, especially before feature launches and release cutoffs.

### Summary

- **Local ClickHouse**: Fast, simple; best for initial trials and interactive development.
- **ClickHouse Cloud**: Closest to real-world production, surfaces integration and operational issues early, and ensures all JDBC (and client) features work as expected.

For more, see the [ClickHouse Cloud docs](https://clickhouse.com/docs/en/cloud/) and the [official integrations guide](https://clickhouse.com/docs/en/integrations/java/).



## Integration path at a glance

Work through these steps in order. The "Common Pitfalls" notes tell you what breaks if you skip one.

| # | Milestone | Core decision |
|---|-----------|---------------|
| 1 | [Instantiation strategy](#step-1--instantiation-strategy) | Connection lifecycle, pooling, and workload identification |
| 2 | [Authentication](#step-2--authentication) | Which auth mechanism and how to configure it via URL/Properties |
| 3 | [Transport & connectivity](#step-3--transport--connectivity-tls-proxies-timeouts) | TLS/mTLS, proxies, timeouts, health checks |
| 4 | [Formats under the hood](#step-4--formats-under-the-hood) | What the driver does internally; when JDBC is not enough |
| 5 | [Read operations & tuning](#step-5--read-operations--tuning) | `ResultSet` streaming; type mapping; heavy-read tuning |
| 6 | [Write operations & tuning](#step-6--write-operations--tuning) | Batch vs RowBinary beta; heavy-ingest tuning; idempotency |
| 7 | [Metadata & schema discovery](#step-7--metadata--schema-discovery) | `DatabaseMetaData`, `ResultSetMetaData`, type mapping |

---

## Step 1 — Instantiation strategy

**Goal:** decide the lifecycle of a JDBC `Connection` and how you pool connections.

This is important to mention about user provided configuration when system is transparent about connection properties. Build an adoption layer between connector and JDBC driver properties to 
decouple your system from changes in JDBC driver. Having such adoption layer helps also with migration to new version and resolving compatibility issues. Always separate your application or 
connector configuration from JDBC driver. It may be very problematic in future to resolve name collisions and handle upgrades. It is very important to document how JDBC driver 
can be configured by user and where to find driver configuration references. 

**Security Note** Always validate input for configuration. JDBC verifies it own configuration but some values can be semantically invalid. For example, do not let to reconfigure client name.
 

### What the JDBC objects are

| Object | Class | Role |
|--------|-------|------|
| `Connection` | [`ConnectionImpl`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/ConnectionImpl.java) | Wraps one `Client`; manages config and delegates to HTTP |
| `Statement` | [`StatementImpl`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/StatementImpl.java) | Execute raw SQL strings |
| `PreparedStatement` | [`PreparedStatementImpl`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/PreparedStatementImpl.java) | Parameterized SQL with `?` placeholders |
| Writer statement | [`WriterStatementImpl`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/WriterStatementImpl.java) | Streaming RowBinary insert |
| `ResultSet` | [`ResultSetImpl`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/ResultSetImpl.java) | Row-by-row streaming of query results |
| `Driver` | [`Driver`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/Driver.java) | Registers `jdbc:clickhouse:` and `jdbc:ch:` URLs |

### Decisions

| Question | Recommended answer |
|----------|--------------------|
| How many connections? | **One per concurrent thread of work**, obtained from a pool. |
| Short- or long-lived? | Let a **connection pool** (HikariCP, DBCP, container-managed) manage lifetime; borrow and return. |
| Thread-safe? | **No** — `Connection` is *not* thread-safe. **CONSTRAINT:** Never share one across threads. |
| Own pool needed? | **Yes** — **CONSTRAINT:** Use a standard JDBC connection pool. Each `Connection` still owns an HTTP pool via its internal `Client`. |

### JDBC URL format

```
jdbc:clickhouse://[host][:port][/[path/]database][?param=value&...]
jdbc:clickhouse:https://host:8443/mydb?ssl=true
jdbc:ch://localhost:8123/default
```

```java
public Connection createConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", "default");
    props.setProperty("password", "secret");
    return DriverManager.getConnection(
        "jdbc:clickhouse://localhost:8123/default", props);
}
```

Prefer `Properties` over embedding credentials in the URL. URL/Properties parsing is handled by [`JdbcConfiguration`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/internal/JdbcConfiguration.java); programmatic setup is available via [`DataSourceImpl`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/DataSourceImpl.java).

### Init configuration — pool sizing and threading

Because each `Connection` wraps a `Client` with its own HTTP pool, the client-level `max_open_connections` (default 10) forwards through. Two layers of pooling exist:

- **JDBC connection pool** (your responsibility) — controls how many `Connection` objects (and thus `Client` instances) exist.
- **HTTP pool per connection** — `max_open_connections` forwarded to `ClientConfigProperties`.

### Workload identification & client name

In production environments, a single ClickHouse cluster is often shared across diverse workloads: user-facing web services, streaming ingestion pipelines, ETL batch jobs, BI reporting dashboards (e.g., Superset, Tableau, Grafana), and ad-hoc analytics. When queries fail, time out, or consume excessive memory (`MEMORY_LIMIT_EXCEEDED`), identifying the originating application or workload is essential for fast troubleshooting, root-cause analysis, and resource attribution.

#### Setting client name

Use a structured format such as `<application-name>/<version>` or `<application-name>:<workload-type>/<version>` (for example, `order-service/1.2.0` or `etl-worker:cdc/2.0.1`).

There are three ways to configure client identification in JDBC:

**1. Connection Properties or JDBC URL** (static setup for the connection or pool):

```java
import com.clickhouse.client.api.ClientConfigProperties;

public Connection createIdentifiedConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", "default");
    props.setProperty("password", "secret");
    // Identify application workload in system.query_log (http_user_agent)
    props.setProperty(ClientConfigProperties.CLIENT_NAME.getKey(), "order-service/1.2.0");
    // Alternatively pass as string key:
    // props.setProperty("client_name", "order-service/1.2.0");

    return DriverManager.getConnection(
        "jdbc:clickhouse://localhost:8123/default", props);
}
```

Or directly via JDBC URL query parameters:

```
jdbc:clickhouse://localhost:8123/default?client_name=order-service/1.2.0
```

**2. Standard JDBC `setClientInfo`** (dynamic per-connection or per-task setup):

When sharing pooled connections across different worker threads or tasks, set the application name dynamically on the borrowed connection before executing work:

```java
import com.clickhouse.jdbc.ClientInfoProperties;

public void executeWorkloadTask(Connection conn, String taskName) throws SQLException {
    // Dynamically tag the connection before executing queries
    conn.setClientInfo(ClientInfoProperties.APPLICATION_NAME.getKey(), "order-service:" + taskName + "/1.2.0");
    // Standard JDBC property name "ApplicationName" is also supported:
    // conn.setClientInfo("ApplicationName", "order-service:" + taskName + "/1.2.0");

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT count() FROM orders")) {
        // process query results
    }
}
```

#### How it is observed on the server (`User-Agent` header)

The JDBC driver communicates over HTTP and passes the client name as the leading segment of the HTTP `User-Agent` header. The driver automatically appends the JDBC driver version, detected frameworks (e.g. HikariCP), client version, operating system, and JVM version:

```text
order-service/1.2.0 ClickHouse-JDBC/0.9.8 clickhouse-java-v2/0.9.8 (Linux; jvm:17.0.2) Apache-HttpClient/5.4.4
```

> **CRITICAL SERVER OBSERVATION NOTE:**
> In ClickHouse's `system.query_log` and `system.processes`, HTTP requests record this information in the **`http_user_agent`** column. The `client_name` column in `system.query_log` is populated **only** for native TCP protocol connections. Always query `http_user_agent` when troubleshooting JDBC applications.

#### Finding workloads in `system.query_log`

Use the following queries on ClickHouse to troubleshoot and monitor application workloads:

**Find recent queries and execution metrics for a specific application:**
```sql
SELECT
    event_time,
    query_id,
    query_duration_ms,
    memory_usage,
    read_rows,
    read_bytes,
    result_rows,
    http_user_agent,
    query
FROM system.query_log
WHERE type = 'QueryFinish'
  AND http_user_agent LIKE '%order-service%'
  AND event_time >= now() - INTERVAL 1 HOUR
ORDER BY event_time DESC
LIMIT 100;
```

**Find failed queries and exceptions for a workload:**
```sql
SELECT
    event_time,
    query_id,
    exception_code,
    exception,
    http_user_agent,
    query
FROM system.query_log
WHERE type = 'ExceptionWhileProcessing'
  AND http_user_agent LIKE '%order-service%'
  AND event_time >= now() - INTERVAL 24 HOUR
ORDER BY event_time DESC
LIMIT 50;
```

**Aggregate workload resource consumption across all applications:**
```sql
SELECT
    extract(http_user_agent, '^([^ ]+)') AS workload,
    count() AS query_count,
    round(avg(query_duration_ms), 2) AS avg_duration_ms,
    round(quantile(0.95)(query_duration_ms), 2) AS p95_duration_ms,
    round(max(query_duration_ms), 2) AS max_duration_ms,
    formatReadableSize(sum(memory_usage)) AS total_memory,
    formatReadableQuantity(sum(read_rows)) AS total_read_rows,
    countIf(type = 'ExceptionWhileProcessing') AS error_count
FROM system.query_log
WHERE event_time >= now() - INTERVAL 24 HOUR
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
GROUP BY workload
ORDER BY query_count DESC;
```

**Inspect active running queries (`system.processes`):**
```sql
SELECT
    query_id,
    elapsed,
    memory_usage,
    http_user_agent,
    query
FROM system.processes
WHERE http_user_agent LIKE '%order-service%';
```

**Query across a cluster:**
```sql
SELECT
    hostName() AS host,
    event_time,
    query_id,
    query_duration_ms,
    memory_usage,
    http_user_agent,
    query
FROM clusterAllReplicas('default', system.query_log)
WHERE type = 'QueryFinish'
  AND http_user_agent LIKE '%order-service%'
  AND event_time >= now() - INTERVAL 1 HOUR
ORDER BY event_time DESC
LIMIT 100;
```

### Common Pitfalls

<common-pitfalls>
- **CONSTRAINT:** Never share a `Connection` across threads. It causes data races — it is not thread-safe.
- **No connection pool** means a new `Client` + HTTP pool warm-up on every `getConnection()` — high latency.
- **Over-sized JDBC pool × per-connection HTTP pool** can multiply into far more server connections than expected. Size both deliberately.
- **`Connection.close()`** closes the underlying `Client` and its HTTP pool — expected when returning to a pool.

</common-pitfalls>
---

## Step 2 — Authentication

**Goal:** choose exactly one primary authentication mechanism and pass it through the JDBC URL or `Properties`. The driver forwards these to the underlying client, whose `CredentialsManager` rejects mixed mechanisms.

> This section is intentionally self-contained (it mirrors the [Java Client guide](integration-client.md#step-2--authentication) but with **JDBC URL / `Properties` configuration**). For the full reference, see [authentication.md](authentication.md).

### Option A — Basic (username + password)

```java
public Connection createBasicAuthConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", "default");
    props.setProperty("password", "secret");
    return DriverManager.getConnection(
        "jdbc:clickhouse://localhost:8123/default", props);
}
```

### Option B — Token / bearer

Pass the token as a client property; it forwards to the underlying client's token auth:

```java
public Connection createTokenAuthConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("access_token", "my_access_token");
    // or: props.setProperty("bearer_token", "my_access_token");
    return DriverManager.getConnection(
        "jdbc:clickhouse://localhost:8123/default", props);
}
```

### Option C — Mutual TLS (client certificate)

```java
public Connection createMtlsConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("ssl", "true");
    props.setProperty("ssl_authentication", "true");
    props.setProperty("sslcert", "/path/to/client.crt");
    props.setProperty("ssl_key", "/path/to/client.key");
    props.setProperty("sslrootcert", "/path/to/ca.crt"); // if server cert is self-signed
    return DriverManager.getConnection(
        "jdbc:clickhouse://localhost:8443/default", props);
}
```

A trust store may be used instead: `trust_store`, `key_store_password`, `key_store_type`.

### Option D — Custom headers (proxies / gateways)

For OAuth gateways, custom authentication proxies, or API gateway configurations, inject arbitrary HTTP headers via connection properties using `DriverProperties.httpHeader(...)` (or the `http_header_<NAME>` property prefix):

```java
import com.clickhouse.jdbc.DriverProperties;

public Connection createCustomHeadersConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", "default");
    props.setProperty("password", "secret");
    props.setProperty(DriverProperties.httpHeader("X-API-Key"), "my_custom_api_key");
    // or directly: props.setProperty("http_header_X-API-Key", "my_custom_api_key");
    return DriverManager.getConnection(
        "jdbc:clickhouse://localhost:8123/default", props);
}
```

### Decisions

| Question | Guidance |
|----------|----------|
| Which mechanism? | Password for most deployments; `access_token`/`bearer_token` for gateway-fronted or cloud setups; mTLS for certificate-based zero-trust; custom headers for API gateways. |
| Credentials in URL or Properties? | **Properties** — keeps secrets out of URLs and logs. |
| Behind an auth proxy? | Prefer token auth (Option B) or custom headers via `http_header_<NAME>` (Option D). |

### Common Pitfalls

<common-pitfalls>
- **CONSTRAINT:** Do not mix auth mechanisms (password *and* token). It throws a misconfiguration error when the connection is created.
- **mTLS requires HTTPS** (`ssl=true` + port 8443) and a valid cert/key pair; a missing root CA for self-signed servers fails the handshake.
- **Credentials embedded in the JDBC URL** leak into logs and connection-pool config dumps — use `Properties`.

</common-pitfalls>
---

## Step 3 — Transport & connectivity (TLS, proxies, timeouts)

**Goal:** make the driver reach the server reliably. The driver delegates TLS and proxy handling to the underlying client; configure via URL/Properties.

### TLS / mTLS / proxies

| Scenario | Property / URL parameter |
|----------|--------------------------|
| Enable HTTPS | `ssl=true` + port 8443 |
| Self-signed server cert | `sslrootcert=/path/to/ca.crt` |
| Client certificate (mTLS) | `sslcert`, `ssl_key`, `ssl_authentication=true` |
| Trust store | `trust_store`, `key_store_password`, `key_store_type` |
| HTTP proxy | `proxy_type=http`, `proxy_host`, `proxy_port`, `proxy_user`, `proxy_password` |

See [examples/jdbc SSLExamples](../examples/jdbc/src/main/java/com/clickhouse/examples/jdbc/SSLExamples.java) and [authentication.md](authentication.md).

### Init configuration — server vs client settings

Both forward through URL/Properties:

```java
public Connection createConfiguredConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", "default");
    props.setProperty("max_execution_time", "60");   // server setting
    props.setProperty("max_open_connections", "20");  // client setting
    return DriverManager.getConnection(
        "jdbc:clickhouse://localhost:8123/default", props);
}
```

Per-statement server settings: use `Statement.setQueryTimeout(...)` or an SQL `SETTINGS` clause.

### Health check

```java
public boolean checkConnectionHealth(Connection conn, int timeoutSeconds) throws SQLException {
    if (!conn.isValid(timeoutSeconds)) {
        // trigger recovery logic, mark service unhealthy, or fail fast
        return false;
    }
    return true;
}
```



### Common Pitfalls

<common-pitfalls>
- **Timeouts too aggressive** for heavy analytical queries cause spurious failures — align `max_execution_time` / `setQueryTimeout` with expected duration.
- **Proxy credentials omitted** on authenticated proxies produce opaque connection failures.

</common-pitfalls>
---

## Step 4 — Formats under the hood

**Goal:** understand how JDBC handles format selection internally and how to configure custom formats like `JSONEachRow`.

JDBC uses the client's format selection mechanism under the hood. By default, the driver sends `X-ClickHouse-Format: RowBinaryWithNamesAndTypes` for query execution:

| Operation | Internal format | Notes |
|-----------|-----------------|-------|
| Query (`executeQuery`) | `RowBinaryWithNamesAndTypes` | Converted to JDBC `ResultSet` rows |
| Simple INSERT via `Statement` | SQL text | `INSERT INTO t VALUES (...)` |
| `PreparedStatement` INSERT | SQL text or RowBinary | RowBinary when `beta.row_binary_for_simple_insert=true` |
| Writer statement INSERT | RowBinary | Streaming binary writer |
| Batch INSERT | Multi-row SQL rewrite or RowBinary | Depends on statement shape |

### Format Selection and SQL `FORMAT` Clauses

The response format can be configured using the `format` connection property (`ClientConfigProperties.INPUT_OUTPUT_FORMAT` or `"format"`).

**Important for ClickHouse 26.8+:**
- On ClickHouse 26.8+, the request format header sent by the driver (`X-ClickHouse-Format`) takes priority over a `FORMAT` clause written in the SQL query string.
- By default, the driver sends `format=RowBinaryWithNamesAndTypes`.
- To read JSON in JDBC, the recommended approach is setting `format=JSONEachRow` in connection properties along with `jdbc_json_parser_factory`.
- Setting `format=` (empty string) or `null` is an **expert-only setting**:
  - Setting `format=` omits the `X-ClickHouse-Format` request header, allowing explicit SQL `FORMAT` clauses written in query strings to take effect.
  - **Caveat:** For any statement without an explicit SQL `FORMAT` clause, the server falls back to its `default_format` (`TabSeparated`). Because JDBC `ResultSet` only consumes `RowBinaryWithNamesAndTypes` and `JSONEachRow`, such queries fail with a `SQLException`.
  - `DatabaseMetaData` operations (e.g. `getTables()`, `getColumns()`) are not affected by the `format` property: they pin `RowBinaryWithNamesAndTypes` on the statements they run internally.

### Usage of `JSONEachRow` in JDBC

JDBC V2 supports streaming `JSONEachRow` responses as standard `ResultSet` instances. This feature is opt-in and requires configuring a `JsonParserFactory`.

1. **Configure Driver Properties:**
   Set `jdbc_json_parser_factory` (`DriverProperties.JSON_PARSER_FACTORY`) to the fully-qualified class name of a `JsonParserFactory` implementation (such as `JacksonJsonParserFactory` or `GsonJsonParserFactory`).
   Set `format` (`ClientConfigProperties.INPUT_OUTPUT_FORMAT`) to `"JSONEachRow"`.

```java
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.data_formats.JacksonJsonParserFactory;
import com.clickhouse.jdbc.DriverProperties;

public Properties createJsonEachRowProperties() {
    Properties props = new Properties();
    props.setProperty(DriverProperties.JSON_PARSER_FACTORY.getKey(), JacksonJsonParserFactory.class.getName());
    props.setProperty(ClientConfigProperties.INPUT_OUTPUT_FORMAT.getKey(), "JSONEachRow");
    return props;
}
```

2. **Execute Query and Process ResultSet:**

```java
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public void readJsonEachRowResultSet(Connection conn) throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT id, name, payload FROM events ORDER BY id")) {
        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            Object payload = rs.getObject("payload"); // returns parser-native List/Map
        }
    }
}
```

### When JDBC's format contract is not enough

| Goal | JDBC approach | Better alternative |
|------|---------------|--------------------|
| Simple CRUD / reporting | Standard JDBC — sufficient | — |
| Bulk ingest (millions of rows) | Batch `PreparedStatement` + RowBinary beta | Java Client stream insert |
| Complex type handling | `getObject()` with type map | Java Client POJO/binary readers |
| Export to a file format | Not supported via ResultSet (ResultSet requires `RowBinaryWithNamesAndTypes` or `JSONEachRow`; text formats like CSV fail) | Java Client with format selection (`conn.unwrap(ConnectionImpl.class).getClient()`) |
| BI tool integration | JDBC is the right choice | — |

### Hybrid usage: dropping down to the Java Client

If you need maximum ingest throughput, specific binary formats, or POJO serialization, but your application is fundamentally built on JDBC, you can extract the underlying `Client` from the `Connection`.

```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.jdbc.ConnectionImpl;

public void insertStreamViaHybridClient(Connection conn, InputStream dataStream) throws Exception {
    // Unwrap the JDBC connection to get the native Java Client
    Client client = conn.unwrap(ConnectionImpl.class).getClient();

    // Use native Java Client streaming capabilities while sharing the underlying HTTP connection pool
    InsertSettings settings = new InsertSettings().compressClientRequest(true);
    try (InsertResponse response = client.insert("events", dataStream, ClickHouseFormat.JSONEachRow, settings).get()) {
        // handle response metrics or confirmation
    }
}
```

This hybrid approach allows you to use standard JDBC for simple CRUD and metadata, while using the native Java Client for bulk ingest or custom data processing.

### Common Pitfalls

<common-pitfalls>
- **Format selection scope** — format selection can be configured via connection properties (`format=JSONEachRow` or setting `jdbc_json_parser_factory`), but standard JDBC `ResultSet` requires compatible row formats (`RowBinaryWithNamesAndTypes` or `JSONEachRow`). Other wire formats like `Native` or `Parquet` require dropping down to the Java Client.
- **Row-oriented output only** — no column-oriented or parallel block consumption.
- **Type mapping layer** may lose precision or structure for complex types.
- **Text INSERT overhead** — default SQL-based inserts are slower than binary streaming. Use the [Java Client](integration-client.md) for maximum throughput.

</common-pitfalls>
---

## Step 5 — Read operations & tuning

**Goal:** read `ResultSet` rows correctly (especially ClickHouse-specific types) and tune heavy reads.

### General interface

```java
public void readEvents(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT id, name, created_at FROM events LIMIT 1000")) {

        while (rs.next()) {
            long id = rs.getLong("id");
            String name = rs.getString("name");
            Timestamp created = rs.getTimestamp("created_at");
            // process row data
        }
    }
}
```

**Key classes:**

| Class | Role |
|-------|------|
| [`StatementImpl`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/StatementImpl.java) | Execute queries, manage timeouts and cancellation |
| [`ResultSetImpl`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/ResultSetImpl.java) | Row-by-row result streaming |
| [`PreparedStatementImpl`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/PreparedStatementImpl.java) | Parameterized queries |

### Reading ClickHouse-specific types

```java
public void readSpecialTypes(ResultSet rs) throws SQLException {
    String uuid = rs.getString("uuid_col");
    BigDecimal decimal = rs.getBigDecimal("decimal_col");
    LocalDateTime timestamp = rs.getObject("ts_col", LocalDateTime.class); // java.time

    Map<String, Class<?>> typeMap = Collections.singletonMap("UInt64", BigInteger.class);
    BigInteger bigNum = (BigInteger) rs.getObject("big_num", typeMap);     // custom mapping

    Array array = rs.getArray("tags");                                     // Array
    Struct tuple = (Struct) rs.getObject("point");                         // Tuple
}
```

### Operation configuration — tuning heavy reads

| Setting | Property / method | Notes |
|---------|-------------------|-------|
| Response compression | `compress=true` (default) | Server-side LZ4 |
| Max execution time | `max_execution_time` | Server-side query timeout |
| Max result rows | `jdbc_use_max_result_rows=true` | Enforce server `max_result_rows` |
| Query timeout | `Statement.setQueryTimeout(seconds)` | JDBC-level timeout |
| Result-set auto-close | `jdbc_resultset_auto_close=true` (default) | Close previous result on new query |
| Fetch size | `Statement.setFetchSize(n)` | Streaming batch-size hint |

**Tip:** enforce row limits in SQL (`LIMIT n`). With `jdbc_use_max_result_rows` disabled, the driver stops reading at the limit but the server may still send remaining data.

### Best practices

<best-practices>
- **Always use `LIMIT`** for exploratory queries.
- **Set a query timeout** to prevent hung queries.
- **Use try-with-resources** for `Connection`, `Statement`, `ResultSet`.
- **Prefer `PreparedStatement`** for repeated / parameterized queries.
- **Map large integers**: `jdbc_type_mappings=UInt64=java.math.BigInteger`.
- **Use `getObject(column, Class)`** for `java.time` types instead of legacy getters.

</best-practices>
### Common Pitfalls

<common-pitfalls>
- **`getInt()` on `UInt64`/`Int128`** overflows — use `getBigDecimal`/`BigInteger` or a custom mapping.
- **Complex types** (`Array`, `Tuple`, `Map`, `Nested`, `Variant`, `Dynamic`, geometry) require `getObject()`/`getArray()`, not primitive getters.
- **Scrollable/updatable result sets are unsupported** — forward-only, read-only.
- **No server-side cursors** — the server streams the full result set.
- **Some frameworks materialize all rows** even though the driver streams — watch memory.

</common-pitfalls>
---

## Step 6 — Write operations & tuning

**Goal:** choose an insert path, tune batching, and make retries idempotent. ClickHouse has **no transactions** — every statement auto-commits immediately.

### Insert paths

**Simple INSERT via `Statement`:**

```java
public void insertDirect(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("INSERT INTO events (id, name) VALUES (1, 'click'), (2, 'house')");
    }
}
```

**Batched INSERT via `PreparedStatement`:**

```java
public void insertEventsBatch(Connection conn, List<Event> events) throws SQLException {
    if (events.isEmpty()) {
        return;
    }

    try (PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO events (id, name, created_at) VALUES (?, ?, ?)")) {
        for (Event event : events) {
            ps.setLong(1, event.getId());
            ps.setString(2, event.getName());
            ps.setObject(3, event.getCreatedAt());
            ps.addBatch();
        }
        ps.executeBatch();
    }
}
```

**RowBinary streaming insert (beta):** enable `beta.row_binary_for_simple_insert=true` so simple `INSERT INTO t VALUES (?, ?, ?)` statements serialize as RowBinary via [`WriterStatementImpl`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/WriterStatementImpl.java) instead of SQL text.

```java
public Connection createRowBinaryInsertConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", "default");
    props.setProperty("password", "secret");
    // switch using row binary writer for inserts
    props.setProperty("beta.row_binary_for_simple_insert", "true");
    return DriverManager.getConnection(
        "jdbc:clickhouse://localhost:8123/default", props);
}
```

### Operation configuration — tuning heavy writes

| Setting | Property / method | Notes |
|---------|-------------------|-------|
| Batch inserts | `PreparedStatement.addBatch()` / `executeBatch()` | Multi-row rewrite for eligible INSERTs |
| RowBinary writer | `beta.row_binary_for_simple_insert=true` | Binary path for simple `VALUES (?, ?, ?)` |
| Client compression | `decompress=true` | LZ4-compress insert payload |
| HTTP compression | `client.use_http_compression=true` | Content-Encoding on the HTTP layer |
| Async insert | `async_insert=1` (server setting) | Server-side insert buffering |

When using the RowBinary writer for inserts, you can combine it with async inserts (`async_insert=1` and optionally `wait_async_insert=1`) to achieve very high-throughput, low-latency ingestion. The RowBinary format is efficient for bulk data transfer, while async insert lets the server buffer and process inserts in the background, enabling the client to proceed without blocking on disk writes.

**How this helps:**
- **Lower client-side latency:** The JDBC driver streams RowBinary-encoded data directly to the server; with async insert enabled, the server acknowledges receipt quickly, decoupling the client from storage latency.
- **Higher throughput:** The combination is ideal for ingest-heavy workloads—batches or streaming ETL—because the client isn't forced to wait for each insert to be durably stored before proceeding.
- **Optimized network and server utilization:** RowBinary reduces payload size and overhead, async insert handles bursty/high-rate loads by buffering, and both together reduce insert round-trip costs.

**Example: Batched, streaming async insert using RowBinary**

```java
public void asyncRowBinaryInsert(Connection conn, List<Event> events) throws SQLException {
    if (events.isEmpty()) {
        return;
    }
    try (PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO events (id, name, created_at) " +
            "SETTINGS async_insert=1, wait_async_insert=1 VALUES (?, ?, ?)")) {
        for (Event event : events) {
            ps.setLong(1, event.getId());
            ps.setString(2, event.getName());
            ps.setObject(3, event.getCreatedAt());
            ps.addBatch();
        }
        ps.executeBatch();
    }
}
```

This leverages the JDBC driver's RowBinary streaming (when `beta.row_binary_for_simple_insert=true` is set) *plus* async server-side processing, so your application can ingest large volumes with minimal response-time overhead.

> **Tip:** For the best results on very high-volume pipelines, adjust both server async insert settings (see [ClickHouse docs](https://clickhouse.com/docs/en/operations/settings/settings#async_insert)) and the client's connection/HTTP pool size and streaming/chunking properties.
> 
> **Caution:** Remember that with async insert, success response means "data accepted for processing" — not yet "written." Use `wait_async_insert=1` to wait for commit, and deduplication tokens if retrying inserts to avoid duplicates.


### Idempotency — deduplication token

JDBC does not expose `insert_deduplication_token` as a first-class API. Three ways to use it:


**SQL `SETTINGS` clause** (per statement):

```java
public void insertWithPerStatementDedup(Connection conn, String token) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(
            "INSERT INTO events SETTINGS insert_deduplication_token = '" + token + "' VALUES (1, 'a')");
    }
}
```

**2. Switch to the Java Client** for per-insert token control via `InsertSettings.setDeduplicationToken(...)`.

See [integration-client.md — deduplication token](integration-client.md#idempotency--deduplication-token) for semantics and requirements.


**Async inserts** 
ClickHouse supports "async inserts", which allow inserts to buffer on the server side (`async_insert=1`), returning control to the client before the data is fully written to disk. **Caveat:** The insert operation may report success even though the data is not yet persisted, and you *cannot* reliably check row count in statistics to confirm completion.

**To ensure the insert is actually complete and data is committed, the only supported option is to use `wait_async_insert=1` in the SQL SETTINGS clause.** This makes the server wait for the buffered insert to finish before acknowledging the operation:

```java
public void safeAsyncInsert(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(
            "INSERT INTO events SETTINGS async_insert=1, wait_async_insert=1 VALUES (1, 'a')");
    }
}
```

> **Note:** Inserts using async mode may interact with deduplication. If your workflow is retry-prone or idempotency is required, always set an explicit deduplication token (see prior section) in `insert_deduplication_token`. This avoids duplicate inserts if the first async INSERT is still being processed when a retry occurs.

**Summary:**  
- Never rely on server statistics/row count to determine async insert completion.
- Use `wait_async_insert=1` to guarantee data is committed before continuing.
- For deduplication, combine `insert_deduplication_token` with async inserts as needed (`SETTINGS async_insert=1, wait_async_insert=1, insert_deduplication_token = 'your_token'`).

### Failure Handling 

#### Retriable Operations

Most operations retried by the JDBC driver are **network-level failures** — for example, timeouts, connection drops, and intermediate transport errors.  
The underlying Java Client (`client-v2`) already implements robust automatic retry logic for such network and transient issues, so in almost all cases **explicit retry logic in application code is unnecessary**.

Some server-side errors, however, may also be transient and suitable for retry, depending on the use case:

- **Execution timeout errors**: If a query fails due to a timeout (e.g., server-side `max_execution_time` or too aggressive timeout settings), the operation may be retried with adjusted settings or after a backoff delay.
- **Schema mismatch errors** (see below): When the table schema changes between requests (causing errors such as "UNABLE TO READ ALL DATA"), retrying the operation after refreshing the schema can resolve the problem.
- **Server overloads or resource spikes**: Very occasionally, a server might return a retryable error indicating temporary overload; review the exception chain for details.

> **Best Practice:**  
> - Trust the client's network retry strategy for network or I/O failures.
> - For retrying queries after **server-side errors**, examine the exception code and context to determine whether a retry makes sense — especially for timeouts, schema mismatches, or temporary overloads.
> - Avoid blanket retries of all `SQLException`s, since some are non-retriable (invalid SQL, authentication failure, etc.), and indiscriminate retrying can produce duplicate writes if idempotency is not ensured.

See below for how to detect and handle schema mismatch errors specifically.

#### Schema mismatch

When working with a ClickHouse cluster, you may encounter errors if the table schema changes unexpectedly between queries or ingestions. While most JDBC applications escape this because generated SQL typically matches the current schema, dynamic ingestion workflows or custom binary writers (like RowBinary insert streams) are particularly sensitive.

**Typical error:**  
A schema change — such as adding, removing, or altering a column — can result in ingestion failures with errors like:

- **ClickHouse Error Code 33 — "UNABLE TO READ ALL DATA"**:  
  This error appears when the data sent from the client does not match the expected table schema on the server (column count, order, or types are incompatible).  
  - For example, if you use a RowBinary writer and the schema is out of sync, ClickHouse cannot parse the incoming data block, resulting in this error.
  - Code 33 may also be reported as a generic SQL exception by the JDBC layer, and the actual error code is only accessible if the exception's cause is a `ServerException` from the ClickHouse client.

**Detection & Handling:**
- **Detection:**  
  Error Code 33 signals that ClickHouse could not read all required data — most often due to a schema drift or column mismatch between the client and ClickHouse table.  
  However, in the JDBC pathway, the underlying ClickHouse error code is accessible *only if* the root cause of the `SQLException` is a `com.clickhouse.client.ServerException`. You should check if `SQLException.getCause()` is a `ServerException` and inspect the code/message.
- **Best practices for handling:**  
  1. **Catch and examine SQL exceptions.**  
     - If `ex.getCause()` is a `ServerException` with code 33 (or message indicating unable to read all data), assume a schema mismatch.
  2. **Proactively refresh schema.**  
     - Fetch the latest table schema using JDBC's `DatabaseMetaData` or a `DESCRIBE TABLE` query.
  3. **Regenerate SQL or update your ingestion/mapping logic.**  
     - Make sure your SQL, `PreparedStatement`, or RowBinary column mapping exactly matches the current table schema.
  4. **Retry the operation.**

**Example: Handling Error Code 33 (ServerException only)**

```java
import com.clickhouse.client.ServerException;

try {
    // ... ingestion code ...
} catch (SQLException ex) {
    Throwable cause = ex.getCause();
    if (cause instanceof ServerException) {
        ServerException serverEx = (ServerException) cause;
        // ClickHouse code 33: UNABLE TO READ ALL DATA (schema mismatch)
        if (serverEx.getErrorCode() == 33 ) {
            // Possible table schema mismatch
            // 1. Reload schema via DatabaseMetaData or DESCRIBE TABLE
            // 2. Update SQL or RowBinary mapping as needed
            // 3. Retry with updated logic
        } else {
            throw ex; // not a schema mismatch
        }
    } else {
        throw ex; // Cannot determine server error code
    }
}
```

> **Note:** Always ensure your application's data shape matches the target table exactly when using RowBinary or similar binary ingestion APIs. If your code holds a schema cache, invalidate it and fetch the up-to-date schema on a Code 33 error before retrying.

See [ClickHouse error codes documentation](https://clickhouse.com/docs/en/operations/error-codes/) for more on Code 33 and related scenarios.

### Best practices

<best-practices>
- **Batch inserts** — hundreds to thousands of rows per batch.
- **Enable the RowBinary beta** for simple inserts — significantly faster than SQL text rendering.
- **Use `PreparedStatement`** so the driver escapes values correctly.
- **Set deduplication tokens** on retry-prone pipelines.
- **Tune batch size** by row width; watch `system.query_log` for insert performance.

</best-practices>
### Common Pitfalls

<common-pitfalls>
- **One row per `executeUpdate()`** — HTTP overhead dominates.
- **No transactional rollback** — a failed batch may leave partial data depending on the engine.
- **Batching complex INSERT shapes** (`INSERT SELECT`, multi-table) is unsupported — use `Statement`.
- **Retrying without a dedup token** on MergeTree can create duplicates.
- **Maximum ingest throughput** is not JDBC's strength — the [Java Client](integration-client.md) stream insert is faster.

</common-pitfalls>
---

## Step 7 — Metadata & schema discovery

**Goal:** use standard JDBC metadata interfaces, backed by ClickHouse system tables, and understand type mapping.

### DatabaseMetaData

```java
public TableMetadata buildMetadataModel(Connection conn, String database, String table) throws SQLException {
    DatabaseMetaData meta = conn.getMetaData();
    TableMetadata.Builder tableMetadataBuilder = new TableMetadata.Builder()
        .setDatabase(database)
        .setTableName(table);

    try (ResultSet tables = meta.getTables(null, database, table, new String[]{"TABLE"})) {
        if (tables.next()) {
            tableMetadataBuilder.setTableType(tables.getString("TABLE_TYPE"));
        }
    }

    try (ResultSet columns = meta.getColumns(null, database, table, "%")) {
        while (columns.next()) {
            String name = columns.getString("COLUMN_NAME");
            int jdbcType = columns.getInt("DATA_TYPE");
            String chType = columns.getString("TYPE_NAME");
            tableMetadataBuilder.addColumn(name, jdbcType, chType);
        }
    }

    return tableMetadataBuilder.build();
}
```

Implemented by [`DatabaseMetaDataImpl`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/metadata/DatabaseMetaDataImpl.java). Supports catalogs, schemas, tables/views/materialized views, column metadata (JDBC codes + native type names), primary keys, and index info (sorting keys), with table types mapped from ClickHouse engines.

### ResultSetMetaData & ParameterMetaData

```java
public void inspectResultSetMetadata(ResultSet rs) throws SQLException {
    ResultSetMetaData rsMeta = rs.getMetaData();
    for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
        String colName = rsMeta.getColumnName(i);
        String chType = rsMeta.getColumnTypeName(i); // exact ClickHouse type, e.g. Nullable(UInt64)
        int jdbcType = rsMeta.getColumnType(i);     // mapped JDBC type code
        // process metadata
    }
}

public int getParameterCount(PreparedStatement ps) throws SQLException {
    return ps.getParameterMetaData().getParameterCount();
}
```

### Type mapping

The driver maps ClickHouse types to JDBC types (e.g. `UInt64` → `NUMERIC`, `Tuple` → `STRUCT`); see [type_mapping.md](../type_mapping.md). Override defaults:


```java
public Connection createConnection() throws SQLException {
    Properties props = new Properties();
    // .. base configuration 
    props.setProperty("jdbc_type_mappings", "UInt64=java.math.BigInteger,Int128=java.math.BigInteger");

    return DriverManager.getConnection(
        "jdbc:clickhouse://localhost:8123/default", props);
}
```

### Tools summary

| Tool | Interface | Use case |
|------|-----------|----------|
| Table discovery | `DatabaseMetaData.getTables()` | List tables and views |
| Column discovery | `DatabaseMetaData.getColumns()` | Schema inspection, ORM tooling |
| Query result schema | `ResultSetMetaData` | Dynamic query handling |
| Native type name | `getColumnTypeName()` | Exact ClickHouse type |
| JDBC type code | `getColumnType()` | Standard JDBC interop |
| Custom type map | `jdbc_type_mappings` / `setTypeMap()` | Override default mappings |
| Client-side schema | `ConnectionImpl.getClient().getTableSchema()` | Advanced: reach the underlying client API |

For schema-driven POJO binding and binary format writers, use the [Java Client integration guide](integration-client.md).

### Common Pitfalls

<common-pitfalls>
- **JDBC metadata may not reflect every ClickHouse type nuance** — use `getColumnTypeName()` for the native string.
- **Default type mappings** may not match your expectations for large integers or complex types — override with `jdbc_type_mappings`.

</common-pitfalls>
---

## JDBC-specific features

| Feature | How to use |
|---------|------------|
| Application name | Set via `client_name` property, URL parameter, or `Connection.setClientInfo("ApplicationName", "my-app")` — see [Workload identification & client name](#workload-identification--client-name) |
| Schema / database | `Connection.setSchema("analytics")` or the URL path |
| Query cancellation | `Statement.cancel()` → `KILL QUERY` (optionally `ON CLUSTER` via `jdbc_cluster_name`) |
| JDBC escape syntax | `{ts '...'}`, `{d '...'}`, `{fn ...}` — translated before execution |
| Default query settings | `default_query_settings` property |
| Role management | `SET ROLE` statements (roles remembered by default) |

Key JDBC-specific properties (see [`DriverProperties`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/DriverProperties.java)):

| Property | Default | Purpose |
|----------|---------|---------|
| `ssl` | `false` | Enable HTTPS |
| `jdbc_ignore_unsupported_values` | `false` | Silently ignore unsupported JDBC calls |
| `jdbc_resultset_auto_close` | `true` | Auto-close result set on new query |
| `jdbc_use_max_result_rows` | `false` | Enforce server `max_result_rows` |
| `beta.row_binary_for_simple_insert` | `false` | RowBinary writer for simple INSERT |
| `jdbc_sql_parser` | `JAVACC` | SQL parser backend |
| `jdbc_cluster_name` | — | Cluster for `KILL QUERY ON CLUSTER` |
| `jdbc_type_mappings` | — | Custom ClickHouse → Java type overrides |
| `default_query_settings` | — | Default settings for all queries |


## References

**External resources:**

| Resource | Link |
|----------|------|
| Official docs | [clickhouse.com/docs/integrations/language-clients/java/jdbc](https://clickhouse.com/docs/integrations/language-clients/java/jdbc) |
| Javadoc | [javadoc.io/doc/com.clickhouse/clickhouse-jdbc](https://javadoc.io/doc/com.clickhouse/clickhouse-jdbc) |
| Maven artifact | `com.clickhouse:clickhouse-jdbc` (use the `all` classifier for bundled dependencies) |
| Examples | [examples/jdbc](../examples/jdbc) |
| Full property reference | [`DriverProperties`](../jdbc-v2/src/main/java/com/clickhouse/jdbc/DriverProperties.java), [`ClientConfigProperties`](../client-v2/src/main/java/com/clickhouse/client/api/ClientConfigProperties.java), and [ClickHouse server settings](https://clickhouse.com/docs/operations/settings/settings) |

**Related documents in this repository:**

- [integration-common.md](integration-common.md) — choosing JDBC vs Client
- [integration-client.md](integration-client.md) — Java Client integration path
- [authentication.md](authentication.md) — full authentication and TLS reference
- [features.md](features.md) — compatibility contract
- [type_mapping.md](../type_mapping.md) — JDBC type mapping recommendations