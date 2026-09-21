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

The `Client` uses Apache HttpClient `5.x`, which manages its own internal HTTP connection pool.

For parameter specifications and pool configuration guidance, see:
- [Java Client Integration Guide: Connections Configuration](integration-client.md#step-4--connections-configuration)
- [Java Client Documentation: Connection Pooling](clickhouse-docs/client.mdx#connection-pooling)

#### Operational Considerations

When operating `client-v2` in production environments, keep the following operational rules in mind:

- **Operation Timeouts under High Concurrency:** If the application experiences operation timeouts or connection request timeouts (`ConnectionRequestTimeoutException`) while handling many concurrent requests, check the maximum connections setting (`setMaxConnections`). If the pool limit is too small for peak concurrent demand, threads will block waiting for an available connection from the pool and eventually time out.
- **`NoHttpResponseException` (Stale Connections):** If the application encounters `NoHttpResponseException`, it is most probably a stale connection problem where the ClickHouse server or an intermediate load balancer/proxy closed idle HTTP connections without the client knowing. Configure `setKeepAliveTimeout` to be less than the keep-alive timeout configured on the ClickHouse server or load balancer so idle connections are closed client-side before becoming stale.

---

## Metrics & Monitoring

Observability in `clickhouse-java` is split into operational metrics (reported via `MetricsRecorder`), HTTP connection pool metrics (bound to Micrometer), and in-band response statistics (`OperationMetrics`).

For detailed metric definitions, tag specifications, and connection pool gauge details, see the [Client V2 Metrics Documentation](https://clickhouse.com/docs/integrations/language-clients/java/client#v2-o11y-metrics) (or local [clickhouse-docs/client.mdx](clickhouse-docs/client.mdx#v2-o11y-metrics)).

- **Operational Metrics (`MetricsRecorder`):** Tracks operation durations (`db.client.operation.duration`), serialization overhead, operation counts, and retry attempt counters with low-cardinality tags (`db.system.name`, `db.namespace`, `db.operation.name`, `db.collection.name`, `error.type`).
- **Connection Pool Gauges:** Binds Apache HttpClient 5 pool statistics (`pool.total.max`, `pool.total.connections`, `pool.total.pending`, `connect.time`) to Micrometer via `.registerClientMetrics(meterRegistry, groupName)`.
- **In-Band Response Metrics (`OperationMetrics`):** Directly inspect execution stats (rows/bytes read or written, server execution time, query ID) from response objects (`QueryResponse`, `InsertResponse`).

---

## Distributed Tracing & Spans

Client V2 supports OpenTelemetry distributed tracing across a structured 3-tier parent-child span hierarchy:

1. **Outer / Application Span:** Ambient trace context in thread (`Context.current()`).
2. **Client V2 Operation Span:** High-level client operation span (`QUERY <database>` or `INSERT <database>.<table_name>`).
3. **Transport Request Span:** Individual HTTP POST attempt span (including retries).

For complete span hierarchy specifications and span attributes, see the [Client V2 Spans Documentation](https://clickhouse.com/docs/integrations/language-clients/java/client#v2-o11y-spans) (or local [clickhouse-docs/client.mdx](clickhouse-docs/client.mdx#v2-o11y-spans)).

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

### 1. Connection Pool Starvation

**Symptom:** Threads block or throw `ConnectionInitiationException` / `ConnectionRequestTimeoutException` with messages indicating connection acquisition timeout.

**Diagnosis:**
- Monitor the `httpcomponents.httpclient.pool.total.pending` gauge in Grafana / Prometheus. A non-zero or spiking pending count indicates thread contention for HTTP sockets.
- Compare `httpcomponents.httpclient.pool.total.connections{state="leased"}` against `httpcomponents.httpclient.pool.total.connections{state="max"}` (or `pool.total.max`).

**Remediation:**
- Increase `Client.Builder.setMaxConnections(...)` to match concurrent thread demand.
- Ensure all query and insert response objects (`QueryResponse`, `InsertResponse`) are closed promptly using `try-with-resources`.

### 2. Query Correlation with Server Logs (`system.query_log`)

**Task:** Correlate client-side operation failures, slow queries, or execution details with server-side logs in ClickHouse (`system.query_log`).

**Solution:**
- The client assigns or receives a `query_id` for all operations. It can be retrieved from `OperationMetrics.getQueryId()`, logged in application logs, or read from the `clickhouse.query_id` span attribute when tracing is enabled.
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

### 5. Transport Failures vs. Server Exceptions

- **Server Exception:** Indicated by non-null `db.response.status_code` tag (e.g. `60` for missing table, `159` for timeout). Represents ClickHouse server rejecting the query.
- **Transport / Connection Failure:** `db.response.status_code` is absent, and `error.type` indicates `ConnectionInitiationException`, `DataTransferException`, or `NoHttpResponseException`. Indicates network, proxy, or server availability issues.

---

## References & Demos

- **Spring Boot Telemetry Demo:** See `examples/demo-spring-service` in this repository for a complete implementation showing Client V2 setup, JDBC usage, connection pool metrics, and direct metrics exporting to ClickHouse.
- **Java Client Integration Guide:** [docs/integration-client.md](integration-client.md)
- **JDBC Integration Guide:** [docs/integration-jdbc.md](integration-jdbc.md)
