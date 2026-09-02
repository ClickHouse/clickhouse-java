# ClickHouse Java Operations & Observability Guide

This guide covers operational monitoring, connection pool management, telemetry instrumentation, and troubleshooting for Java applications using ClickHouse (`com.clickhouse:client-v2` and `com.clickhouse.clickhouse-jdbc`).

---

## Overview of Workloads & Integration Layers

The `clickhouse-java` ecosystem provides two primary integration layers built on the same underlying HTTP transport engine (`client-v2` HTTP client helper):

1. **Java Client (`client-v2`)**: Native asynchronous and streaming API. Ideal for high-throughput microservices, event streaming consumers, and bulk ingestion/analytical workloads.
2. **JDBC Driver (`clickhouse-jdbc`)**: JDBC 4.2 compliant driver wrapping the Java Client internally. Ideal for Spring Data JPA, Hibernate, BI tools, and ORM-based applications.

Because both layers share the same Apache HttpClient HTTP transport stack, connection pooling, metrics collection, and distributed tracing work consistently across both direct Java Client usage and JDBC driver connections.

---

## Connection Pools & Resource Management

### HTTP Connection Pooling (`client-v2`)

The `Client` instance owns an internal Apache HttpClient 5 connection pool. It manages persistent HTTP connections to ClickHouse endpoints.

#### Key Pool Settings

| Setting | Builder Method | Default | Description |
|---------|----------------|---------|-------------|
| Max Connections | `setMaxConnections(int)` | `10` | Maximum open HTTP connections per server endpoint. |
| Connection TTL | `setConnectionTTL(long, TimeUnit)` | `-1` (disabled) | Time-to-live after which an active connection is closed and recreated. |
| Keep-Alive Timeout | `setKeepAliveTimeout(long, TimeUnit)` | Server default | HTTP Keep-Alive duration for idle pooled connections. |
| Connection Request Timeout | `setConnectionRequestTimeout(long, TimeUnit)` | `10000ms` | Maximum time a thread blocks waiting for an available connection from the pool. |
| Reuse Strategy | `setConnectionReuseStrategy(ConnectionReuseStrategy)` | `FIFO` | Connection pool allocation strategy (`FIFO` or `LIFO`). |

#### Recommended Pool Configuration

```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.ConnectionReuseStrategy;

import java.util.concurrent.TimeUnit;

public Client createOptimizedClient() {
    return new Client.Builder()
        .addEndpoint("http://localhost:8123")
        .setUsername("default")
        .setPassword("secret")
        .setMaxConnections(50)
        .setConnectionRequestTimeout(5, TimeUnit.SECONDS)
        .setKeepAliveTimeout(60, TimeUnit.SECONDS)
        .setConnectionReuseStrategy(ConnectionReuseStrategy.FIFO)
        .build();
}
```

### JDBC Pooling Considerations (HikariCP & Frameworks)

When using the JDBC driver (`clickhouse-jdbc`) with an external connection pooler like **HikariCP**:

- **Layering:** HikariCP manages JDBC `Connection` instances, while each JDBC connection wraps a `Client` instance with its own internal HTTP socket pool.
- **Sizing Alignment:** Avoid over-allocating HikariCP connections. Because ClickHouse processes HTTP requests concurrently over pooled sockets, a smaller HikariCP pool (e.g., 10–20 connections) paired with a properly sized `Client` HTTP pool is typically optimal.
- **Connection Lifecycle:** Configure HikariCP's `maxLifetime` slightly shorter than any network or load-balancer idle timeout to prevent stale socket exceptions.

---

## Metrics & Monitoring

Observability in `clickhouse-java` is split into operational metrics (reported via `MetricsRecorder`), HTTP connection pool metrics (bound to Micrometer), and in-band response statistics (`OperationMetrics`).

### Operational Metrics (`MetricsRecorder`)

Client V2 defines standard database client metrics following OpenTelemetry semantic conventions (`MetricName` and `MetricAttribute`):

| Metric Name | Type / Unit | Description |
|-------------|-------------|-------------|
| `db.client.operation.duration` | Timer (`s`) | Total duration of a ClickHouse client operation (queries and inserts), recorded for both successful and failed operations. |
| `clickhouse.client.operation.serialization.duration` | Timer (`s`) | Duration of client-side serialization (e.g., POJO encoding during inserts). |
| `clickhouse.client.operation.count` | Counter (`{operation}`) | Total number of completed client operations, grouped by outcome tags. |
| `clickhouse.client.operation.retries` | Counter (`{retry}`) | Number of retried attempts across client operations. |

#### Low-Cardinality Metric Tags / Attributes

Meters exported via `MicrometerMetricsRecorder` carry low-cardinality tags (`MetricAttribute`):

| Tag Key | Description | Example Values |
|---------|-------------|----------------|
| `db.system.name` | Database system identifier | `clickhouse` |
| `db.namespace` | Target ClickHouse database | `default`, `analytics` |
| `db.operation.name` | Operation type | `query`, `insert` |
| `db.collection.name` | Target table name (recorded for inserts) | `events` |
| `db.response.status_code` | Server error code on failure | `60` |
| `error.type` | Exception class name on failure (`none` on success) | `com.clickhouse.client.api.ServerException` |

### Connection Pool Gauges

Binding connection pool metrics exposes Apache HttpClient 5 pool statistics to Micrometer via `.registerClientMetrics(meterRegistry, "groupName")`:

| Meter Name | Tags | Description |
|------------|------|-------------|
| `httpcomponents.httpclient.pool.total.max` | `httpclient=<group>` | Configured maximum allowed persistent connections across all routes. |
| `httpcomponents.httpclient.pool.total.connections` | `httpclient=<group>`, `state=available` / `leased` | Number of persistent available or active leased connections. |
| `httpcomponents.httpclient.pool.total.pending` | `httpclient=<group>` | Number of threads currently blocked awaiting a free pooled connection. |
| `httpcomponents.httpclient.connect.time` | `httpclient=<group>` | Running average connection establishment time. |

### In-Band Response Metrics API

Applications can inspect execution metrics directly from response objects (`QueryResponse`, `InsertResponse`) via `OperationMetrics`:

```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.QueryResponse;

public void inspectResponseMetrics(Client client) throws Exception {
    try (QueryResponse response = client.query("SELECT * FROM my_table").get()) {
        OperationMetrics metrics = response.getMetrics();
        
        long readRows = metrics.getMetric(ServerMetrics.NUM_ROWS_READ).getLong();
        long readBytes = metrics.getMetric(ServerMetrics.NUM_BYTES_READ).getLong();
        long writtenRows = metrics.getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong();
        long clientDurationMs = metrics.getMetric(ClientMetrics.OP_DURATION).getLong();
        
        String queryId = metrics.getQueryId();
    }
}
```

---

## Distributed Tracing & Spans

Client V2 supports OpenTelemetry distributed tracing across a structured 3-tier parent-child span hierarchy.

### Span Hierarchy

```text
Application HTTP / Messaging Span (e.g. Spring Controller / Kafka Consumer)
  └── [JDBC Path Only] JDBC Statement Span (e.g., PreparedStatement.executeBatch)
      └── Client V2 Operation Span (query <database> or insert <database>.<table_name>)
          └── Transport Request Span (POST http://localhost:8123)
```

1. **Operation Span**: High-level operation created under the current active trace context (`Context.current()`).
2. **Transport Request Span**: Individual HTTP transport request attempt (`POST`). Created per attempt, so retries generate separate request spans under the same operation span.

### Span Attributes Reference

Spans are populated with standard OpenTelemetry attributes (`SpanAttribute`):

| Span Attribute Key | Scope / Type | Recorded Moment | Description |
|--------------------|--------------|-----------------|-------------|
| `db.system.name` | All operations | Before request start | Always `clickhouse`. |
| `db.namespace` | All operations | Before request start | Target database name. |
| `db.query.text` | Query | Before request start | SQL statement text. |
| `db.collection.name` | Insert | Before request start | Target table name. |
| `db.operation.name` | Insert | Before request start | Operation type (`insert`, `ping`, `getTableSchema`). |
| `db.operation.batch.size` | Insert | Before request start | Batch row count. |
| `db.query.parameter.<name>` | Query | Before request start | Statement parameter values. |
| `clickhouse.query_id` | All operations | Before request start, updated on completion | ClickHouse query ID assigned by client or server. |
| `db.response.returned_rows` | Query | On success completion | Rows returned to caller. |
| `clickhouse.response.read_rows` | Query | On success completion | Rows read from storage by server. |
| `clickhouse.response.read_bytes` | Query | On success completion | Bytes read from storage by server. |
| `clickhouse.response.written_rows` | Insert | On success completion | Rows written to storage by server. |
| `clickhouse.response.written_bytes` | Insert | On success completion | Bytes written to storage by server. |
| `server.address` | Transport request | Before attempt | Target server hostname or IP address. |
| `server.port` | Transport request | Before attempt | Target server port. |
| `http.request.method` | Transport request | Before attempt | HTTP method (always `POST`). |
| `http.response.status_code` | Transport request | On response / failure | HTTP status code (e.g. `200`, `500`). |
| `db.response.status_code` | All & Transport request | On failure | ClickHouse server error code. |
| `error.type` | All & Transport request | On failure | Exception class name on error. |

### Context Propagation across Asynchronous Boundaries

Because Client V2 inherits `Context.current()`, operations started on threads with an active trace span automatically join the trace. When delegating tasks across thread pools (e.g., Spring `@Async` or custom `ExecutorService`), propagate the OpenTelemetry context explicitly:

```java
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;

public void processBatchAsync(Client client, String table, List<?> data, Context parentContext) throws Exception {
    try (Scope ignored = parentContext.makeCurrent()) {
        try (InsertResponse response = client.insert(table, data).get()) {
            // Background Client V2 span joins parentContext trace
        }
    }
}
```

---

## Troubleshooting & Diagnostics

### 1. Diagnosing Connection Pool Starvation

**Symptom:** Threads block or throw `ConnectionInitiationException` / `ConnectionRequestTimeoutException` with messages indicating connection acquisition timeout.

**Diagnosis:**
- Monitor the `httpcomponents.httpclient.pool.total.pending` gauge in Grafana / Prometheus. A non-zero or spiking pending count indicates thread contention for HTTP sockets.
- Compare `httpcomponents.httpclient.pool.total.connections{state="leased"}` against `httpcomponents.httpclient.pool.total.max`.

**Remediation:**
- Increase `Client.Builder.setMaxConnections(...)` to match concurrent thread demand.
- Ensure all query and insert response objects (`QueryResponse`, `InsertResponse`) are closed promptly using `try-with-resources`.

### 2. Correlating Application Traces with ClickHouse Server Logs (`system.query_log`)

**Symptom:** Need to trace an expensive or failing query from APM traces down to ClickHouse server execution logs.

**Solution:**
- The client records `clickhouse.query_id` on every operation span.
- You can supply a custom query ID generator during client setup:
  ```java
  clientBuilder.setQueryIdGenerator(() -> UUID.randomUUID().toString());
  ```
- Query `system.query_log` in ClickHouse using the recorded query ID:
  ```sql
  SELECT query_id, type, query_duration_ms, read_rows, read_bytes, memory_usage, exception
  FROM system.query_log
  WHERE query_id = 'your-recorded-query-id'
  ORDER BY event_time DESC;
  ```

### 3. Missing Classpath Dependencies

**Symptom:** `NoClassDefFoundError: io/opentelemetry/api/...` or `java.lang.NoClassDefFoundError: io/micrometer/core/...` at runtime when using `OpenTelemetrySpanRecorder` or `MicrometerMetricsRecorder`.

**Cause:** The main `client-v2` artifact includes recorder classes, but does not bundle or transitively pull in OpenTelemetry or Micrometer dependencies.

**Solution:** Explicitly declare the required telemetry libraries in your `pom.xml` or `build.gradle`:

```xml
<!-- OpenTelemetry API -->
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-api</artifactId>
    <version>1.38.0</version>
</dependency>

<!-- Micrometer Core -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-core</artifactId>
    <version>1.13.0</version>
</dependency>
```

### 4. High Serialization Duration

**Symptom:** `clickhouse.client.operation.serialization.duration` takes a significant portion of total operation duration (`db.client.operation.duration`).

**Diagnosis & Remediation:**
- For POJO inserts, high serialization duration points to expensive reflection or large batch encoding.
- Consider tuning batch sizes or utilizing direct binary stream writers (`RowBinaryFormatWriter`) for ultra-high throughput paths.

### 5. Distinguishing Transport Failures vs Server Exceptions

- **Server Exception:** Indicated by non-null `db.response.status_code` tag (e.g. `60` for missing table, `159` for timeout). Represents ClickHouse server rejecting the query.
- **Transport / Connection Failure:** `db.response.status_code` is absent, and `error.type` indicates `ConnectionInitiationException`, `DataTransferException`, or `NoHttpResponseException`. Indicates network, proxy, or server availability issues.

---

## References & Demos

- **Spring Boot Telemetry Demo:** See `examples/demo-spring-service` in this repository for a complete implementation showing Client V2 setup, JDBC usage, connection pool metrics, and direct metrics exporting to ClickHouse.
- **Java Client Integration Guide:** [docs/integration-client.md](integration-client.md)
- **JDBC Integration Guide:** [docs/integration-jdbc.md](integration-jdbc.md)
