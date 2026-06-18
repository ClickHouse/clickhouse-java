# ClickHouse Java Integration — Choosing the Right Tool

This document is the starting point for integrating ClickHouse into a Java application. It explains when to use the **Java Client** (`client-v2`) versus the **JDBC driver** (`jdbc-v2`), and points you to the detailed integration guides for each path.

| Document | Audience | Link |
|----------|----------|------|
| This guide | Anyone evaluating options | [integration-common.md](integration-common.md) |
| Java Client path | New applications, high-throughput pipelines, custom data processing | [integration-client.md](integration-client.md) |
| JDBC path | Existing JDBC-based stacks, BI tools, ORMs | [integration-jdbc.md](integration-jdbc.md) |

---

## The Java Ecosystem for ClickHouse

The `clickhouse-java` repository ships two modern integration layers built on the same HTTP transport:

| Component | Maven artifact | Role |
|-----------|----------------|------|
| **Java Client V2** | `com.clickhouse:client-v2` | Native API for queries, inserts, commands, and streaming data |
| **JDBC Driver V2** | `com.clickhouse:clickhouse-jdbc` | JDBC 4.2 driver that wraps `client-v2` internally |

Both components communicate with ClickHouse over HTTP(S). The JDBC driver is not a separate protocol stack — every JDBC connection owns an underlying `Client` instance.

**Official documentation:**

- Java Client: [clickhouse.com/docs/integrations/java](https://clickhouse.com/docs/integrations/java)
- JDBC Driver: [clickhouse.com/docs/integrations/language-clients/java/jdbc](https://clickhouse.com/docs/integrations/language-clients/java/jdbc)
- Repository README: [README.md](../README.md)
- Feature contract (for reviewers and advanced users): [features.md](features.md)

**Examples in this repository:**

- Client: [examples/client-v2](../examples/client-v2)
- JDBC: [examples/jdbc](../examples/jdbc)
- Spring demo: [examples/demo-service](../examples/demo-service)

---

## Selecting the Correct Tool: JDBC vs Java Client

When deciding between the JDBC Driver and the Java Client, consider the following decision path:

1. **Must you use the JDBC API?** If your application or framework mandates standard JDBC interfaces, choose the **JDBC Driver**. Be aware that this introduces row-oriented `ResultSet` extraction and type mapping overhead.
2. **Do you need maximum throughput, native formats, or column-oriented processing?** If performance and modern data processing are priorities, choose the **Java Client**. It provides direct access to streaming formats, POJO serialization/deserialization, and parallel reads.
3. **Is your existing stack already built on JDBC?** If you are migrating a legacy stack, the **JDBC Driver** may be the easiest path. However, if you are building something new or do not have strict JDBC requirements, default to the **Java Client**.

### When to use the Java Client (recommended for new work)

Choose the Java Client when you:

- Build a new ingestion or analytics pipeline and control the application code
- Need maximum read/write throughput
- Want to work with ClickHouse **native or binary formats** (`Native`, `RowBinary`, `Parquet`, `JSONEachRow`, …)
- Need typed POJO serialization/deserialization
- Process data in **column-oriented** or **block-oriented** batches rather than one row at a time
- Require fine-grained control over compression, server settings, sessions, and operation-level configuration

The Java Client exposes ClickHouse capabilities directly. There is no JDBC abstraction layer between your code and the wire format.

### When to use JDBC

Choose JDBC when you:

- Must plug into an existing JDBC ecosystem (ORM, connection pool configured for JDBC, BI tool, Spark/Flink JDBC source)
- Need standard `Connection`, `Statement`, `PreparedStatement`, `ResultSet`, and `DatabaseMetaData` interfaces
- Prefer SQL-centric workflows with minimal custom client code
- Accept the performance and type-mapping trade-offs described below

---

## Why JDBC Is Often a Poor Fit for ClickHouse

JDBC was established as the standard Java API for relational database connectivity, specifically built to solve the challenges of interacting with traditional transactional databases (OLTP). While it excels at standardizing point lookups, row-by-row updates, and ACID transactions, these core design choices do not align naturally with an OLAP column store like ClickHouse.

### Designed for a different class of database

Traditional JDBC targets row-oriented, transactional enterprise databases. The API models:

- Row-by-row cursors (`ResultSet.next()`)
- Server-side prepared statements with bind parameters
- ACID transactions, updatable result sets, and scrollable cursors

ClickHouse is optimized for **analytical workloads**: bulk inserts, columnar storage, vectorized execution, and append-heavy tables. Applications that use JDBC with ClickHouse typically still need **glue code** to batch rows, choose formats, tune server settings, and work around JDBC semantics that do not map cleanly to ClickHouse behavior.

### Performance overhead

We can make JDBC fast, and type conversion itself is generally not the primary bottleneck. However, performance problems with JDBC lay in another dimension: the development friction and lack of tooling for direct access to data streams.

JDBC's rigid API contract prevents applications from easily connecting ClickHouse's output directly to modern data processing tools or columnar consumers. Instead of passing a raw data stream directly to a tool that natively processes formats like Parquet or JSON, developers are forced to write and maintain boilerplate glue code.

On the write side, JDBC batching helps, but you still route data through SQL rendering rather than directly streaming pre-serialized format payloads from your source systems. 

Another aspect of development performance is the slow adoption of new ClickHouse features in JDBC. Because the JDBC specification is rigid, surfacing new database capabilities often requires complex workarounds or "hacks" within the driver. In contrast, it is much simpler to add a helper method to the Java Client than to force a new capability through the JDBC abstraction. The Java Client bypasses these limitations by offering direct access to the underlying format streams, enabling integration with other tools and yielding much higher throughput with less development effort.

### ClickHouse-specific types and features

JDBC maps ClickHouse types to a fixed set of JDBC/SQL types. The driver covers common types well, but ClickHouse's strength is its rich type system:

- Nested structures, `Array`, `Map`, `Tuple`, `Variant`, `Dynamic`, `JSON`, geometry types
- Engine-specific settings and table features
- Format-specific ingest and export paths

For anything beyond common scalar types, JDBC users often hit mapping gaps and must fall back to strings, custom parsing, or raw SQL — losing type safety and performance.

We support common JDBC types, but **ClickHouse-specific types and features are best accessed through the Java Client**, which reads and writes them through binary format readers, generic records, and POJO binding.

### The row-oriented hard limit

This is the most important structural constraint: **JDBC is row-oriented by specification**.

The driver can send and receive native/binary wire formats internally, but the public JDBC API always presents data as rows. You cannot, through JDBC alone:

- Read a `Native` or `RowBinary` response as a parallel column stream
- Fan out block processing across threads while staying inside the JDBC contract
- Skip the `ResultSet` row loop for high-throughput ETL

If your application needs column-oriented or parallel block processing, the Java Client is the correct tool.

---

## Side-by-Side Comparison

| Concern | Java Client | JDBC Driver |
|---------|-------------|-------------|
| API style | Native async/streaming API | Standard JDBC interfaces |
| Read model | Streaming formats, `Records`, POJOs, binary readers | `ResultSet` (row-by-row) |
| Write model | Stream insert, POJO insert, format writers | `INSERT` SQL, batched `PreparedStatement` |
| Formats | Full format selection per operation | Limited to what JDBC workflow allows |
| ClickHouse-specific types | Binary readers, POJO SerDe, generic records | JDBC type mapping + `getObject` overrides |
| Parallel processing | Possible with format streams and async API | Not possible within JDBC row contract |
| Tooling compatibility | Requires application code | Works with JDBC tools and ORMs |
| Underlying transport | HTTP(S) via Apache HttpClient | Same — wraps `client-v2` |
| Configuration | `Client.Builder`, `ClientConfigProperties` | JDBC URL + `Properties`, passthrough to client |
| Best for | Pipelines, services, custom analytics | Legacy stacks, JDBC-only integrations |

---

## Decision Checklist

Use this quick checklist before choosing:

1. **Does the application already require JDBC?** → JDBC ([integration-jdbc.md](integration-jdbc.md))
2. **Is throughput or format choice critical?** → Java Client ([integration-client.md](integration-client.md))
3. **Do you rely on ClickHouse-specific types (`Variant`, `Dynamic`, nested structures, geometry)?** → Prefer Java Client
4. **Do you need parallel block/column processing?** → Java Client only
5. **Do you need `DatabaseMetaData` for a JDBC-compatible tool?** → JDBC
6. **Are you starting a greenfield service?** → Java Client

---

## Next Steps

| Your choice | Continue with |
|-------------|---------------|
| Java Client | [integration-client.md](integration-client.md) |
| JDBC Driver | [integration-jdbc.md](integration-jdbc.md) |

Both paths share the same authentication and TLS building blocks. See [authentication.md](authentication.md) for username/password, bearer tokens, mTLS, and custom HTTP headers.
