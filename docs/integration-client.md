# ClickHouse Java Client Integration Guide

This guide is a **step-by-step, end-to-end integration path** for the **Java Client V2** (`client-v2`). It is written to be used as context for building an application or a downstream integration spec: each step states the decisions you must make, how to configure them, and the common pitfalls to avoid. It is self-contained — you can work through it from the empty project to a running read/write path without other prerequisites.

> **Configuration philosophy.** This guide names only the properties relevant to each step. It does not repeat the exhaustive property list — that lives in [`ClientConfigProperties`](../client-v2/src/main/java/com/clickhouse/client/api/ClientConfigProperties.java) and the official docs. Configuration splits into two groups:
> - **Init configuration** — set once when the client is built: endpoint, connection pool size, async mode, authentication. Covered in Steps 1–4.
> - **Operation configuration** — set per request or as client defaults: formats, buffer sizes, timeouts, retries, dedup tokens. Covered in Steps 5–7.

## Artifacts

The client is published to Maven Central as **`com.clickhouse:client-v2`**. Browse versions and copy a ready-made dependency snippet for any build system (Maven, Gradle, sbt, Ivy, ...) from the [Maven Central page](https://central.sonatype.com/artifact/com.clickhouse/client-v2).

Two distributions are published under the same artifact:

- **Standard artifact** (default, no classifier) — the client together with its dependencies declared as ordinary transitive Maven dependencies. Recommended for managed builds in which the application controls the dependency tree.
- **Shaded artifact** (`all` classifier) — a single self-contained archive that bundles and **relocates** most third-party dependencies (Apache HttpClient, LZ4, RoaringBitmap, ASM, and others) under `com.clickhouse.shaded.*`. Recommended when transitive dependencies cannot be managed, such as self-contained deployment archives or standalone tooling.

**Dependency conflicts to anticipate.** The standard artifact introduces libraries that the application may already depend on at different versions — notably **Guava**, **Apache HttpClient 5**, and **commons-compress**. If the application declares incompatible versions, `NoSuchMethodError` or `LinkageError` may occur at runtime. Two approaches resolve this:

- Use the shaded artifact so that these dependencies are relocated and cannot collide. Note that `slf4j` and `micrometer` are intentionally left unshaded, so logging and metrics continue to bind to the application's own implementations.
- Alternatively, use the standard artifact and reconcile versions explicitly through dependency management or `<exclusions>`.

---

## Integration path at a glance

Work through these steps in order. Each one is a decision point; the "Common Pitfalls" notes describe the consequences of skipping it.

| # | Step | Core decision |
|---|-----------|---------------|
| 1 | [Instantiation](#step-1--instantiation) | How many `Client` instances, their lifetime |
| 2 | [Authentication](#step-2--authentication) | Which auth mechanism and how to configure it |
| 3 | [Transport & connectivity](#step-3--transport--connectivity-tls-proxies-timeouts) | TLS/mTLS, proxies, timeouts, health checks |
| 4 | [Connections Configuration](#step-4--connections-configuration) | Pool sizing, async vs sync, sessions |
| 5 | [Data formats, readers & writers](#step-5--data-formats-readers--writers) | Which wire format and reader/writer to use |
| 6 | [Read operations & tuning](#step-6--read-operations--tuning) | Streaming vs materializing; heavy-read tuning; read errors |
| 7 | [Write operations & tuning](#step-7--write-operations--tuning) | Insert pattern; heavy-ingest tuning; idempotency; write errors |
| 8 | [Metadata & schema discovery](#step-8--metadata--schema-discovery) | How to obtain schemas without JDBC metadata |

---

## Step 1 — Instantiation

**Goal:** decide how many `Client` instances exist, how long they live, and how the internal connection pool is sized.

### What a `Client` is

The [`Client`](https://javadoc.io/doc/com.clickhouse/client-v2/latest/com/clickhouse/client/api/Client.html) is the single entry point for all operations. It owns:

- An HTTP connection pool (Apache HttpClient)
- Endpoint configuration and retry policy
- A table schema cache
- The POJO serialization/deserialization registry
- Optional client-wide session and settings defaults

```java
import com.clickhouse.client.api.Client;

Client client = new Client.Builder()
    .addEndpoint("http://localhost:8123")
    .setUsername("default")
    .setPassword("secret")
    .setDefaultDatabase("analytics")
    .build();
```

### Instantiation Strategy

- **Share a single instance:** A single, shared `Client` instance suits most use cases. It is thread-safe and designed to be reused across your application.
- **Long-lived lifecycle:** Build the client once at startup and close it once at shutdown. Creating a new client per request or operation is an anti-pattern because initialization takes time to set up internal structures (like the connection pool and schema cache), which adds latency to your requests.
- **Serverless functions:** For serverless environments (like AWS Lambda), initialize the client outside the function handler so it can be reused across invocations.
- **Warm-up (optional):** Calling `client.ping()` at startup can help initialize the connectivity part and verify the endpoint before serving live traffic, though it is not strictly required.
- **Caching:** The application is responsible for holding the reference to the `Client` instance (e.g., via dependency injection or a singleton). The library does not provide a global static cache.


## Step 2 — Authentication

**Goal:** configure the authentication mechanism the ClickHouse deployment requires. The mechanism is dictated by the server and any fronting infrastructure, not chosen freely; the task is to identify it and configure it correctly. <constraints>
**CONSTRAINT:** Configure exactly one mechanism. Different method cannot be mixed to avoid configuration errors. 
</constraints>

### Option A — Basic (username + password)

The standard mechanism. Default installs ship a `default` user with no password.

```java
import com.clickhouse.client.api.Client;

Client client = new Client.Builder()
    .addEndpoint("http://localhost:8123")
    .setUsername("my_user")
    .setPassword("my_password")
    .build();
```

Rotate credentials at runtime (thread-safe, non-blocking, applies to newly started requests):

```java
client.updateUserAndPassword("new_user", "new_password");
```

### Option B — Token / bearer

For the standard `Authorization: Bearer <token>` scheme use `useBearerTokenAuth(...)` (the `Bearer ` prefix is added for you):

```java
import com.clickhouse.client.api.Client;

Client client = new Client.Builder()
    .addEndpoint("http://localhost:8123")
    .useBearerTokenAuth("my_access_token")
    .build();
```

For a non-`Bearer` scheme, use `setAccessToken(...)` — the value is sent verbatim, so include the scheme yourself. Runtime updates: `updateBearerToken(...)` (adds prefix) and `updateAccessToken(...)` (verbatim).

### Option C — Mutual TLS (client certificate)

```java
import com.clickhouse.client.api.Client;

Client client = new Client.Builder()
    .addEndpoint("https://localhost:8443")
    .useSSLAuthentication(true)
    .setClientCertificate("/path/to/client.crt")
    .setClientKey("/path/to/client.key")
    .setRootCertificate("/path/to/ca.crt") // if the server cert is self-signed
    .build();
```

Alternatively configure a trust store with `setSSLTrustStore(...)`, `setSSLTrustStorePassword(...)`, `setSSLTrustStoreType(...)`.

### Option D — Custom HTTP headers (proxies / gateways)

For OAuth gateways or custom handler configurations, inject arbitrary headers:

```java
import com.clickhouse.client.api.Client;

Client client = new Client.Builder()
    .addEndpoint("http://localhost:8123")
    .httpHeader("X-API-Key", "my_custom_api_key")
    .build();
```

### Option E — Proxy credentials

If you connect to ClickHouse through an HTTP proxy that requires authentication, you can provide proxy credentials. This is configured alongside the proxy connection details and operates independently of the database authentication mechanism you chose above:

```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.ProxyType;

Client client = new Client.Builder()
    .addEndpoint("http://localhost:8123")
    // Database credentials (e.g. Option A)
    .setUsername("default")
    .setPassword("secret")
    // Proxy configuration and credentials
    .addProxy(ProxyType.HTTP, "proxy.example.com", 8080)
    .setProxyCredentials("proxy_user", "proxy_password")
    .build();
```

### Identifying the required mechanism

The mechanism follows from how the server and any fronting infrastructure are configured:

| Deployment | Required mechanism |
|------------|--------------------|
| Native ClickHouse users with passwords | Basic — username + password (Option A) |
| Bearer/token gateway or ClickHouse Cloud token | Token / bearer (Option B) |
| Certificate-based (zero-trust) access | Mutual TLS (Option C) |
| API gateway / Identity Aware Proxy in front of ClickHouse | Custom HTTP headers (Option D) |
| HTTP forward proxy requiring authentication | Proxy credentials (Option E) |

Runtime rotation via `updateUserAndPassword` / `updateBearerToken` updates the credentials of the **already-selected** mechanism; it throws `ClientMisconfigurationException` rather than switching to a different mechanism.

---

## Step 3 — Transport & connectivity (TLS, proxies, timeouts)

**Goal:** make the client reach the server reliably under your network topology. This is init configuration set on the builder.

### TLS / mTLS / proxies

| Scenario | Builder methods / properties |
|----------|------------------------------|
| HTTPS with public CA | `addEndpoint("https://host:8443")` |
| Self-signed server cert | `setRootCertificate("/path/to/ca.crt")` |
| mTLS (client certificate) | `useSSLAuthentication(true)`, `setClientCertificate(...)`, `setClientKey(...)` |
| Trust store (JKS/PKCS12) | `setSSLTrustStore(...)`, `setSSLTrustStorePassword(...)` |
| HTTP proxy | `setProxy(ProxyType.HTTP, host, port)`, `setProxyCredentials(user, password)` |

See [SSLExamples](../examples/client-v2/src/main/java/com/clickhouse/examples/client_v2/SSLExamples.java) for a runnable walkthrough and [authentication.md](authentication.md) for full details.

### Init configuration — timeouts

| Property (Builder method) | Purpose |
|----------|---------|
| `connection_timeout` (`.setConnectTimeout()`) | TCP connect timeout |
| `socket_timeout` (`.setSocketTimeout()`) | Socket read/write timeout |

Per-operation network timeouts are set later (Step 6) via `QuerySettings.setNetworkTimeout(long timeout, ChronoUnit unit)`. Note this method returns `void` (it is not chainable), so call it on a `QuerySettings` instance rather than inside a builder chain.

### Health check

```java
if (!client.ping()) { throw new RuntimeException("ClickHouse unreachable"); }
```

### Server vs client settings

- **Server settings** control query execution (`max_execution_time`, `max_threads`). Pass them with the `server_setting` prefix or helpers: `.serverSetting("max_execution_time", "60")` at build time, or `QuerySettings.serverSetting(...)` per operation.
- **Client settings** control HTTP transport, pooling, compression, and timeouts (`compress`, `socket_timeout`, `retry`).

### Common Pitfalls

<common-pitfalls>
- **Timeouts too aggressive** for heavy analytical queries cause spurious failures — align `socket_timeout` with expected query duration or use per-operation network timeouts.
- **Proxy credentials omitted** on authenticated proxies produce opaque connection failures.

</common-pitfalls>
---

## Step 4 — Connections Configuration

In the Java Client a "connection" is an **HTTP connection borrowed from the internal pool**, not a long-lived database session. Each operation borrows a connection, sends a request, streams the response, and returns the connection to the pool.

### The one decision to make: connection limit (`max_open_connections`)

Unlike the answers above, the pool size depends on your workload — specifically on its **concurrency**, not on how much data it moves. What matters is **how many operations run at the same time**, not the number of rows or bytes any single operation transfers. A pool of 20 connections serves at most 20 simultaneous operations regardless of whether each returns one row or a million. This is the single setting you actually tune. The table below is guidance, not a hard rule: measure under your own load.

| Workload | Concurrent requests | Suggested `max_open_connections` | What happens |
|----------|---------------------|----------------------------------|--------------|
| **Short-lived reads, low concurrency** | a few per second | **10–20** | Enough for the traffic; under bursts a few requests briefly **wait for a connection** to be returned to the pool, which is acceptable. |
| **Short-lived reads, higher concurrency** | tens+ per second | **20–100** | More parallel operations need more connections. Connections are **reused many times**, so `connection_reuse_strategy` and `connection_ttl` start to matter. |
| **Long-running operations** (large reads/writes, streaming) | any | **no fixed rule** | A connection is held for the whole operation and rarely reused, so pool sizing matters less. Size to the number of concurrent long operations and focus on **data-transfer tuning** (Steps 6–7) instead. |

The reasoning is only valid for **short-lived read operations**, where connections cycle back to the pool quickly enough to be shared. For long operations the bottleneck is data transfer, not connection availability.

### Init configuration — connection pool properties (set at build time)

These are the only pool-related properties you normally touch; the rest have safe defaults.

| Property (Builder method) | Purpose | Default |
|---------------------------|---------|---------|
| `max_open_connections` (`.setMaxConnections()`) | Pool size — set from the parallelism guidance above | 10 |
| `connection_pool_enabled` (`.enableConnectionPool()`) | Enable/disable pooling (keep enabled) | true |
| `connection_ttl` (`.setConnectionTTL()`) | Max lifetime of a pooled connection | — |
| `connection_reuse_strategy` (`.setConnectionReuseStrategy()`) | FIFO or LIFO reuse | — |

**`connection_ttl` against ClickHouse Cloud.** Keep it **relatively small** when the endpoint is a Cloud (or otherwise load-balanced) deployment. A short TTL forces connections to be retired and re-established frequently, so new connections keep going through the load balancer, which lets it **redistribute traffic across nodes** instead of pinning long-lived connections to whichever node they first landed on.

**`connection_pool_enabled` is not recommended to change.** Leave pooling **enabled** (the default). The only reason to disable it is a workload with extreme concurrency where the internal Apache HttpClient connection pool itself becomes a contention point — under very high parallel request rates the pool's own bookkeeping can serialize threads. Disabling pooling avoids that bottleneck at the cost of a fresh connection per operation (higher latency, more sockets), so treat it as a last resort after measuring, not a default.

### Init configuration — async vs synchronous execution

All operations return `CompletableFuture`. Whether they run on a client-managed executor is controlled once, at build time:

- **`ClientConfigProperties.ASYNC_OPERATIONS`** — when enabled, operations are dispatched on an internal executor and the returned future completes asynchronously. When disabled, the work runs on the calling thread and the returned future is already completed when you receive it.
- `queryAll(...)` returns a `List<GenericRecord>` directly — it blocks and materializes every row into memory. `queryRecords(...)` returns a `CompletableFuture<Records>` whose `Records` is a **lazy** iterator over the still-open response stream, so it neither blocks the whole read nor materializes all rows. Reserve `queryAll(...)` for small results.

This choice should be made early, as async mode affects how back-pressure and timeouts are propagated throughout the application.

### Sessions (optional, decide now if you need them)

A [`Session`](../client-v2/src/main/java/com/clickhouse/client/api/Session.java) carries ClickHouse HTTP session state (`session_id`, `session_check`, `session_timeout`, `session_timezone`):

- **Client-wide** — `Client.Builder.use(session)` applies to every operation.
- **Operation-wide** — `QuerySettings.use(session)` / `InsertSettings.use(session)` overrides per request.

### Common Pitfalls

<common-pitfalls>
- **CONSTRAINT:** Do not create a `Client` per request. It destroys pool warm-up and adds latency on every call — the single most common mistake.
- **Pool too small** (`max_open_connections`) throttles concurrency; size it to peak concurrent operations, not average.
- **Sessions behind a load balancer** require **server affinity** (sticky sessions) — a session-bound request routed to a different node fails. See [Sessions example](../examples/client-v2/src/main/java/com/clickhouse/examples/client_v2/Sessions.java).
- **CONSTRAINT:** Always `close()` the client at shutdown to avoid leaking the pool and its threads.

</common-pitfalls>
---

## Step 5 — Data formats, readers & writers

**Goal:** pick the wire format and the reader/writer that match your throughput and interop needs. Format is chosen **per operation**, unlike JDBC which hides it.

All formats are defined in [`ClickHouseFormat`](../clickhouse-data/src/main/java/com/clickhouse/data/ClickHouseFormat.java); each declares whether it supports input, output, binary encoding, headers, and row layout.

### Format categories

| Category | Examples | Typical use |
|----------|----------|-------------|
| Binary (high throughput) | `RowBinary`, `RowBinaryWithNamesAndTypes`, `Native` | Production ingest and ETL |
| Text (human-readable) | `TabSeparated`, `CSV`, `JSONEachRow` | Debugging, ad-hoc export, interop |
| Columnar file | `Parquet`, `Arrow`, `ORC`, `Avro` | Data-lake integration |
| Schema-aware binary | `RowBinaryWithNamesAndTypes` | Self-describing streams without a prior schema |

### Readers (for reads)

Select with `QuerySettings.setFormat(format)` and consume the response stream:

| Reader class | Formats |
|--------------|---------|
| [`NativeFormatReader`](../client-v2/src/main/java/com/clickhouse/client/api/data_formats/NativeFormatReader.java) | `Native` |
| [`RowBinaryFormatReader`](../client-v2/src/main/java/com/clickhouse/client/api/data_formats/RowBinaryFormatReader.java) | `RowBinary` variants |
| [`ClickHouseBinaryFormatReader`](../client-v2/src/main/java/com/clickhouse/client/api/data_formats/ClickHouseBinaryFormatReader.java) | General binary access |
| Text / JSON | Read `QueryResponse.getInputStream()` directly |

### Writers (for writes)

| Writer class | Use |
|--------------|-----|
| [`RowBinaryFormatWriter`](../client-v2/src/main/java/com/clickhouse/client/api/data_formats/RowBinaryFormatWriter.java) | Row-oriented binary insert |
| [`ClickHouseBinaryFormatWriter`](../client-v2/src/main/java/com/clickhouse/client/api/data_formats/ClickHouseBinaryFormatWriter.java) | General binary insert |

### How to choose

| Goal | Recommended format |
|------|--------------------|
| Maximum read throughput | `Native` or `RowBinaryWithNamesAndTypes` |
| Maximum write throughput | `RowBinary` or `Native` |
| Interop with other systems | `JSONEachRow`, `CSV`, `Parquet` |
| Debugging / inspection | `TabSeparated`, `JSONEachRow` |
| Schema-less exploration | `RowBinaryWithNamesAndTypes` (Wire format carries column names + types, avoiding need for out-of-band schema) |
| POJO-based ingest | Register POJO + default RowBinary writer |

### Common Pitfalls

<common-pitfalls>
- **Not every `ClickHouseFormat` has a dedicated reader/writer.** Specialized formats (e.g. `CapnProto`, `MySQLDump`) may require raw stream handling — verify a reader exists before committing.
- **POJO SerDe** covers most types but has known limitations (e.g. `Geometry` shape inference) — see [features.md](features.md).
- **`Native`** is block-oriented; best when you control both schema and consumption.

</common-pitfalls>
---

## Step 6 — Read operations & tuning

**Goal:** read results efficiently and configure the operation-level settings for heavy analytical reads.

### General interface

```java
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.data.ClickHouseFormat;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;

QuerySettings settings = new QuerySettings()
    .setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);

try (QueryResponse response = client.query("SELECT * FROM events", settings)
        .get(30, TimeUnit.SECONDS)) {

    ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
    while (reader.hasNext()) {
        reader.next();
        long id = reader.getLong("id");
        String name = reader.getString("name");
    }
}

// Convenience: materialize all rows (small results only)
List<GenericRecord> rows = client.queryAll("SELECT count() FROM events");
```

> **Reader schema source.** The one-argument `client.newBinaryFormatReader(response)` only works when the format carries its own schema (e.g. `RowBinaryWithNamesAndTypes`, `Native`). For a plain `RowBinary` stream, which has no embedded column names/types, use the two-argument overload `client.newBinaryFormatReader(response, schema)` and pass a `TableSchema`, or the reader cannot decode the rows.

Parameterized queries use ClickHouse placeholder syntax `{name:Type}`:

```java
Map<String, Object> params = Map.of("min_id", 1000);
client.query("SELECT * FROM events WHERE id > {min_id:UInt64}", params, settings);
```

**Key classes:**

| Class | Role |
|-------|------|
| [`QueryResponse`](../client-v2/src/main/java/com/clickhouse/client/api/query/QueryResponse.java) | Streaming HTTP response; must be closed |
| [`QuerySettings`](../client-v2/src/main/java/com/clickhouse/client/api/query/QuerySettings.java) | Per-query configuration |
| [`Records`](../client-v2/src/main/java/com/clickhouse/client/api/query/Records.java) | Lazy record iterator |
| [`GenericRecord`](../client-v2/src/main/java/com/clickhouse/client/api/query/GenericRecord.java) | Column access by name or index |
| [`OperationMetrics`](../client-v2/src/main/java/com/clickhouse/client/api/metrics/OperationMetrics.java) | Server timing and row counts via `QueryResponse.getMetrics()` |

### Operation configuration — tuning heavy reads

Set these on `QuerySettings` (per query) or as client defaults:

| Setting | Property / method | Notes |
|---------|-------------------|-------|
| Response compression | `compress=true` (default) | Server-side LZ4 |
| Output format | `QuerySettings.setFormat(...)` | Prefer binary for throughput |
| Read buffer size | `QuerySettings.setReadBufferSize(n)` | Min 8192 bytes; raise for large streams |
| Execution limit | `QuerySettings.setMaxExecutionTime(seconds)` | Server-side timeout |
| Max result rows | `serverSetting("max_result_rows", ...)` | Limit rows server-side |
| Network timeout | `QuerySettings.setNetworkTimeout(long, ChronoUnit)` | Per-operation override; returns `void`, not chainable |

Useful correlation helpers: `settings.setQueryId(...)` and `settings.logComment(...)` surface in `system.query_log`.

### Best practices

<best-practices>
- **Prefer binary formats** over text for production reads.
- **Stream rather than materialize** — use `QueryResponse` with a binary reader for large datasets; reserve `queryAll()` for small results.
- **Set server-side limits** (`max_execution_time`, `max_result_rows`) to stop runaway queries.
- **Register POJOs once** at startup, not per query.
- **Always close `QueryResponse`** (try-with-resources) — a leaked response holds an HTTP connection.

</best-practices>
### Errors & how to handle them

<error-handling-rules>
See the [Error model](#error-model) for the exception hierarchy and how to unwrap `ExecutionException`. Reads have **no side effects**, so a failed read is always safe to re-run from the start — the decisions are about *whether* it is worth retrying:

| Failure | Surfaces as | Handling |
|---------|-------------|----------|
| Bad SQL, unknown column, placeholder type mismatch, missing table | `ServerException` (e.g. code `60` table-not-found) | **Not retryable** — a retry produces the same error. Inspect `getCode()` and fix the query. |
| Server aborted an excessively heavy query | `ServerException` code `159` (`TIMEOUT_EXCEEDED`) | The query exceeded `max_execution_time`. Raise the limit or optimize the query; do not retry unconditionally. |
| Transport connect/read timeout | `DataTransferException` / timeout | Often transient. A read is idempotent, so re-running the whole query is safe. |
| Connection dropped **mid-stream** (after you began iterating) | `DataTransferException` while reading | You cannot resume from the middle — some rows were already consumed. Close the `QueryResponse` and re-run the entire query. Make consumers tolerant of re-reading from the start. |

</error-handling-rules>
---

## Step 7 — Write operations & tuning

**Goal:** choose an insert pattern, tune it for bulk ingest, and make retries idempotent.

### Insert patterns

**1. POJO list insert** (simplest for typed data):

```java
client.register(Event.class, client.getTableSchema("events"));
client.insert("events", List.of(new Event(...), new Event(...))).get();
```

**2. Stream insert** (best for bulk / pre-serialized data):

```java
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import java.io.InputStream;

InsertSettings settings = new InsertSettings().compressClientRequest(true);
try (InputStream data = openJsonLinesStream()) {
    client.insert("events", data, ClickHouseFormat.JSONEachRow, settings).get();
}
```

**3. Callback writer** (best for generated binary data). The writer-based `insert` requires an explicit `InsertSettings`, a `TableSchema`, and the `format` argument; the client opens the stream, invokes your `DataStreamWriter`, and closes the stream for you:

```java
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;

TableSchema schema = client.getTableSchema("events");
ClickHouseFormat format = ClickHouseFormat.RowBinary;

client.insert("events", out -> {
    RowBinaryFormatWriter writer = new RowBinaryFormatWriter(out, schema, format);
    for (Event event : events) {
        writer.setValue("id", event.getId());
        writer.setValue("name", event.getName());
        writer.commitRow();
    }
}, format, new InsertSettings()).get();
```

Populate each row with `setValue(column, value)` (by name or 1-based index) and finish it with `commitRow()`. Do not close the stream yourself — the client does it.

**Key classes:**

| Class | Role |
|-------|------|
| [`InsertResponse`](../client-v2/src/main/java/com/clickhouse/client/api/insert/InsertResponse.java) | Insert result; must be closed |
| [`InsertSettings`](../client-v2/src/main/java/com/clickhouse/client/api/insert/InsertSettings.java) | Per-insert configuration |
| [`DataStreamWriter`](../client-v2/src/main/java/com/clickhouse/client/api/DataStreamWriter.java) | Callback interface for writer-based inserts |

### Operation configuration — tuning heavy writes

| Setting | Property / method | Notes |
|---------|-------------------|-------|
| Client request compression | `InsertSettings.compressClientRequest(true)` | LZ4-compress the insert body |
| HTTP compression | `InsertSettings.useHttpCompression(true)` | Content-Encoding header |
| Pre-compressed data | `InsertSettings.appCompressedData(true, "gzip")` | When you compress the data yourself |
| Copy buffer size | `InsertSettings.setInputStreamCopyBufferSize(n)` | Stream-to-stream copy buffer |
| Async insert | `serverSetting("async_insert", "1")` | Server-side buffering |
| Retry policy | `retry=3` (default), `client_retry_on_failures` | Configurable fault causes (init config) |

### Idempotency — deduplication token

On a `MergeTree`-family table with deduplication enabled, the **`insert_deduplication_token`** lets the server skip duplicate blocks so retries do not create duplicate rows.

```java
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;

InsertSettings settings = new InsertSettings()
    .setDeduplicationToken("batch-2024-06-18-part-001");
client.insert("events", dataStream, ClickHouseFormat.JSONEachRow, settings).get();
```

- Assign a **stable** token per logical batch (file name, Kafka offset, job ID).
- Use it for retry-safe pipelines and at-least-once sources (Kafka, SQS, file reprocessing).
- Requires a `MergeTree` engine with deduplication configured. See [`InsertTests.testInsertSettingsDeduplicationToken`](../client-v2/src/test/java/com/clickhouse/client/insert/InsertTests.java).

### Best practices

<best-practices>
- **Batch large** — thousands to millions of rows per request, never one row per request.
- **Use binary formats** (`RowBinary`, `Native`, or registered POJOs) for production ingest.
- **Enable compression** for large payloads.
- **Set deduplication tokens** on retry-prone pipelines.
- **Pre-fetch and reuse the table schema** (`getTableSchema` is cached internally).

</best-practices>
### Errors & how to handle them

<error-handling-rules>
See the [Error model](#error-model) for the exception hierarchy and how to unwrap `ExecutionException`. Writes have **side effects**, so error handling is fundamentally harder than for reads:

| Failure | Surfaces as | Handling |
|---------|-------------|----------|
| Type mismatch, missing/extra column, quota exceeded, read-only table | `ServerException` | Mostly **not retryable** (schema errors). Fix the payload or schema. Check `isRetryable()` before retrying. |
| Transient server condition (too-many-parts `252`, memory limit `241`, network `210`, ...) | `ServerException` with `isRetryable() == true` | The built-in `retry` policy already re-sends. See the caveat below before relying on it. |
| Timeout/drop **after** the server received the body | `ServerException` code `319` (`UNKNOWN_STATUS_OF_INSERT`) or transport error | **Ambiguous** — you cannot tell whether the insert committed. Treat as "maybe written" and rely on a deduplication token so a safe re-insert cannot double-write. |

**Retrying an insert re-sends the whole payload — this is the key difference from reads.** On a retry the client calls `DataStreamWriter.onRetry()` and then re-invokes `onOutput(...)`, so your data source must be **replayable**:

- A one-shot `InputStream`, a drained queue, or a consumed iterator **cannot be re-read** — the retry either fails or sends truncated data. Buffer the batch in memory, hand the client a rewindable source, or implement `onRetry()` to reset your stream.
- Even a successful retry can **duplicate rows** unless you set a stable `insert_deduplication_token` (see [Idempotency](#idempotency--deduplication-token) above) on a MergeTree-family table.
- If retries are not safe for your source, disable them (`retry=0`) and handle re-submission at the application level with a dedup token.

</error-handling-rules>
---

## Step 8 — Metadata & schema discovery

**Goal:** obtain table and query schemas without JDBC `DatabaseMetaData`.

### Table schema from a table name

```java
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;

TableSchema schema = client.getTableSchema("events");
TableSchema schema = client.getTableSchema("events", "analytics"); // explicit database

for (ClickHouseColumn column : schema.getColumns()) {
    System.out.println(column.getColumnName() + " : " + column.getDataType());
}
```

Internally runs `DESCRIBE TABLE` and parses via [`TableSchemaParser`](../client-v2/src/main/java/com/clickhouse/client/api/internal/TableSchemaParser.java).

### Schema from a query

```java
TableSchema schema = client.getTableSchemaFromQuery(
    "SELECT id, name, created_at FROM events WHERE id > 0");
```

### POJO registration

```java
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;

TableSchema schema = client.getTableSchema("events");
client.register(Event.class, schema);

// The typed queryAll takes the target class AND the schema to bind against.
List<Event> events = client.queryAll("SELECT * FROM events", Event.class, schema);
client.insert("events", events).get();
```

Field-to-column matching is controlled by [`ColumnToMethodMatchingStrategy`](../client-v2/src/main/java/com/clickhouse/client/api/metadata/ColumnToMethodMatchingStrategy.java) (default: camelCase field → snake_case column). You can also register a known schema directly with `client.registerTableSchema("events", schema)`.

### Tools summary

| Tool | Method | Use case |
|------|--------|----------|
| Table schema | `getTableSchema(table)` | Inserts, POJO binding, writers |
| Query schema | `getTableSchemaFromQuery(sql)` | Dynamic queries, unknown result shape |
| POJO registry | `register(Class, schema)` | Typed query/insert |
| Column metadata | `TableSchema.getColumnByName(name)` | Type-aware read/write |
| Server info | `client.loadServerInfo()` (returns `void`) | Refreshes cached server info; then read `getServerVersion()`, `getServerTimeZone()`, `getUser()` |

For JDBC-style catalog metadata (`DatabaseMetaData`), use the [JDBC integration guide](integration-jdbc.md).

---

<error-handling-rules>
## Error model

This is the shared exception reference used by the read ([Step 6](#step-6--read-operations--tuning)) and write ([Step 7](#step-7--write-operations--tuning)) error sections. All exceptions extend [`ClickHouseException`](../client-v2/src/main/java/com/clickhouse/client/api/ClickHouseException.java) (an unchecked `RuntimeException`). Because operations return `CompletableFuture`, a failed operation surfaces its cause wrapped in `java.util.concurrent.ExecutionException` when you call `.get()`; unwrap it with `getCause()`.

| Exception | Meaning | Typical cause |
|-----------|---------|---------------|
| [`ServerException`](../client-v2/src/main/java/com/clickhouse/client/api/ServerException.java) | ClickHouse rejected the request | Bad SQL, type mismatch, missing table; inspect `getCode()` for the CH error code, `isRetryable()`, and `getQueryId()` |
| [`ClientException`](../client-v2/src/main/java/com/clickhouse/client/api/ClientException.java) | Client-side failure | Serialization, reader/writer, or usage error |
| [`ClientMisconfigurationException`](../client-v2/src/main/java/com/clickhouse/client/api/ClientMisconfigurationException.java) | Invalid configuration | Mixed auth mechanisms; runtime mechanism switch |
| [`ConnectionInitiationException`](../client-v2/src/main/java/com/clickhouse/client/api/ConnectionInitiationException.java) | Could not establish a connection | Wrong endpoint, TLS handshake, proxy failure |
| [`DataTransferException`](../client-v2/src/main/java/com/clickhouse/client/api/DataTransferException.java) | Failure while streaming the request/response body | Dropped connection mid-transfer |

```java
try {
    client.query("SELECT * FROM missing_table").get();
} catch (ExecutionException e) {
    Throwable cause = e.getCause();
    if (cause instanceof ServerException) {
        ServerException se = (ServerException) cause;
        // ServerException.TABLE_NOT_FOUND == 60
        logger.error("ClickHouse error {} (query {}, retryable={}): {}",
                se.getCode(), se.getQueryId(), se.isRetryable(), se.getMessage());
    }
    throw e;
}
```

**On built-in retries.** `ServerException.isRetryable()` marks transient server codes (timeouts, network, memory-limit, too-many-parts, ...) and the client's own `retry` policy already re-sends those. Do not add a second retry loop on top without accounting for it. Retries behave very differently for reads vs writes — see the per-operation "Errors & how to handle them" sections for the details (in particular, an insert retry must re-send the whole payload).

</error-handling-rules>

---

## Quick reference

A minimal end-to-end program. `Client` implements `AutoCloseable`, so try-with-resources handles shutdown.

```java
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseFormat;

import java.io.InputStream;
import java.util.List;

public class QuickStart {
    public static void main(String[] args) throws Exception {
        // Steps 1–4: build one long-lived client
        try (Client client = new Client.Builder()
                .addEndpoint("http://localhost:8123")
                .setUsername("default")
                .setPassword("secret")
                .setDefaultDatabase("default")
                .serverSetting("max_execution_time", "120")
                .build()) {

            // Step 3: health check
            if (!client.ping()) {
                throw new RuntimeException("ClickHouse unreachable");
            }

            // Step 7: write (blocks on the returned future)
            try (InputStream data = QuickStart.class.getResourceAsStream("/events.jsonl")) {
                client.insert("my_table", data, ClickHouseFormat.JSONEachRow).get();
            }

            // Step 6: read a small result
            List<GenericRecord> rows = client.queryAll("SELECT count() FROM my_table");
            System.out.println("row count = " + rows.get(0).getLong(1));
        } // client.close() runs here
    }
}
```

## References

**External resources:**

| Resource | Link |
|----------|------|
| Official docs | [clickhouse.com/docs/integrations/java](https://clickhouse.com/docs/integrations/java) |
| Javadoc | [javadoc.io/doc/com.clickhouse/client-v2](https://javadoc.io/doc/com.clickhouse/client-v2) |
| Artifact (versions + build snippets) | [central.sonatype.com/artifact/com.clickhouse/client-v2](https://central.sonatype.com/artifact/com.clickhouse/client-v2) |
| Runnable examples | [examples/client-v2](../examples/client-v2) |
| Full property reference | [`ClientConfigProperties`](../client-v2/src/main/java/com/clickhouse/client/api/ClientConfigProperties.java) and [ClickHouse server settings](https://clickhouse.com/docs/operations/settings/settings) |

**Related documents in this repository:**

- [integration-common.md](integration-common.md) — choosing JDBC vs Client
- [integration-jdbc.md](integration-jdbc.md) — JDBC integration path
- [authentication.md](authentication.md) — full authentication and TLS reference (referenced from Steps 2–3)
- [features.md](features.md) — compatibility contract (referenced from Step 5)
