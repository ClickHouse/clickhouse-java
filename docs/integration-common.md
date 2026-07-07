# ClickHouse Java Integration

## Summary

This document is the starting point for integrating ClickHouse into a Java application. It explains when to use the **Java Client** (`com.clickhouse:client-v2`) versus the **JDBC Driver** (`com.clickhouse:clickhouse-jdbc`), and points you to the detailed integration guides for each path.

| Document | Audience | Link |
|----------|----------|------|
| This guide | Anyone evaluating options | — (you are here) |
| Java Client path | New applications, high-throughput pipelines, custom data processing | [integration-client.md](integration-client.md) |
| JDBC path | Existing JDBC-based stacks, BI tools, ORMs | [integration-jdbc.md](integration-jdbc.md) |

---

## Overview of Libraries

The `clickhouse-java` repository ships two modern integration layers built on the same HTTP transport:

| Component | Maven artifact | Role |
|-----------|----------------|------|
| **Java Client** | `com.clickhouse:client-v2` | Native API for queries, inserts, commands, and streaming data |
| **JDBC Driver** | `com.clickhouse:clickhouse-jdbc` | JDBC 4.2 driver that wraps the Java Client internally |

**A note on names.** The current JDBC driver is often called "JDBC V2". This refers to the `jdbc-v2` source module in this repository, which is the modern rewrite of the driver on top of `client-v2`. You do not depend on `jdbc-v2` directly: the published artifact you add to your build is `com.clickhouse:clickhouse-jdbc`, which bundles the `jdbc-v2` implementation. In short, `clickhouse-jdbc` **is** the JDBC V2 driver.

Both components communicate with ClickHouse over HTTP(S). The JDBC driver is not a separate protocol stack — every JDBC connection is backed by a `Client` instance internally.

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

## Choosing Between the Java Client and JDBC

The main question is whether standard JDBC compatibility is a hard requirement. JDBC is a row-oriented, transaction-centric API, so a JDBC-only design cannot express some ClickHouse capabilities well — data streaming, for example. Weigh that against the convenience of a uniform `java.sql.*` API.

### When to use the Java Client (recommended for new work)

Choose the Java Client when you:

- Build a new ingestion or analytics pipeline and control the application code
- Need maximum read/write throughput
- Want to work with ClickHouse **native or binary formats** (`Native`, `RowBinary`, `Parquet`, `JSONEachRow`, ...)
- Need typed POJO serialization/deserialization
- Process data in **column-oriented** way. 
- Require fine-grained control over compression, server settings, sessions, and operation-level configuration

The Java Client exposes ClickHouse capabilities directly, with no JDBC abstraction between your code and the wire format. For metadata it offers `Client.getTableSchema(String, String)` as an equivalent to `java.sql.DatabaseMetaData`.

### When to use the JDBC Driver

Choose the JDBC Driver (`com.clickhouse:clickhouse-jdbc`) when you:

- Must plug into an existing JDBC ecosystem — an ORM, a JDBC connection pool, a BI tool, or a Spark/Flink JDBC source. These tools speak `java.sql.*` and cannot call the Java Client directly.
- Want a single, standard API across several databases and accept trading some ClickHouse-specific power for that uniformity.
- Mainly need unified access to database metadata (`DatabaseMetaData`) and straightforward row-by-row data preview rather than high-throughput streaming.
- Can accept that ClickHouse-specific types still need handling in your own code. The driver maps types such as `JSON`, `Geometry`, or `Tuple` to Java objects, but your application must interpret them — for example, casting the result of `ResultSet.getObject("coords")` to the expected type, or parsing a `JSON` column that comes back as a `String`.

---

## Side-by-Side Comparison

| Concern | Java Client | JDBC Driver |
|---------|-------------|-------------|
| API style | Native async/streaming API | Standard JDBC interfaces |
| Read model | Streaming formats, `Records`, POJOs, binary readers | `ResultSet` (row-by-row) |
| Write model | Stream insert, POJO insert, format writers | `INSERT` SQL, batched `PreparedStatement` |
| Formats | RowBinary & Native built-in + custom reader  | RowBinary |
| ClickHouse-specific types | Binary readers, POJO serialization/deserialization, generic records | JDBC type mapping + `getObject` overrides |
| Tooling compatibility | Requires application code | Works with JDBC tools and ORMs |
| Underlying transport | HTTP(S) via Apache HttpClient | Same — wraps `client-v2` |
| Configuration | `Client.Builder`, `ClientConfigProperties` | JDBC URL + `Properties`, passthrough to client |
| Best for | Pipelines, services, custom analytics | Existing JDBC stacks, JDBC-only integrations |

---

## Limitations of the JDBC Driver Path

If you choose the JDBC path, keep the following constraints in mind:

- **Row-oriented by specification.** The public JDBC API always presents data as rows (`ResultSet.next()`), even though the driver can move native/binary formats internally. Column-oriented or parallel block processing is not expressible through JDBC.
- **No direct access to data streams.** JDBC cannot hand a raw ClickHouse output stream to a columnar consumer (for example, a tool that reads Parquet or JSON natively). You end up writing and maintaining glue code instead.
- **Fewer supported formats.** The driver exposes fewer ClickHouse data formats than the Java Client.
- **Slower feature adoption.** Each new ClickHouse capability must fit the fixed JDBC contract, so features tend to arrive later and sometimes only as workarounds. The Java Client can expose a new feature as a simple helper method.

---

## Next Steps

| Your choice | Continue with |
|-------------|---------------|
| Java Client | [integration-client.md](integration-client.md) |
| JDBC Driver | [integration-jdbc.md](integration-jdbc.md) |
