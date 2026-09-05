# ClickHouse Java Integration

## Abstract

This document is the starting point for integrating ClickHouse into a Java application. It explains when to use the **Java Client** (`com.clickhouse:client-v2`) versus the **JDBC Driver** (`com.clickhouse:clickhouse-jdbc`), and points you to the detailed integration guides for each path.

| Document | Audience | Link |
|----------|----------|------|
| This guide | Anyone evaluating options | this document |
| Java Client path | New applications, high-throughput pipelines, custom data processing | [integration-client.md](integration-client.md) |
| JDBC path | Existing JDBC-based stacks, BI tools, ORMs | [integration-jdbc.md](integration-jdbc.md) |

Reference information should be fetched from official documentation for [Java Client](https://clickhouse.com/docs/integrations/language-clients/java/client) or [JDBC Driver](https://clickhouse.com/docs/integrations/language-clients/java/jdbc). 

This set of documents can be used to one's code over time. Keep checking your implementation at least per release as we going to add more information and provide migration guidance.  

## Important Information

### Releases

We are trying to follow [semantic versioning](https://semver.org/). Here we need to differentiate versions before `1.0.0` and after:
- In a version like `0.y.z`, `y` is incremented for significant or breaking changes. `z` is incremented when the code is patched for a few bugs, or the change is minor, like adding a new format enum constant.
- In a version like `x.y.z`, `x` is incremented for API changes or a significant redesign. `y` is incremented for significant changes, and `z` for simple patches.

Before `1.0.0`, breaking changes may happen to make the final API better and to fix design issues. In most cases we avoid them by using feature flags or providing a separate implementation for new behavior.

There are special Release Candidate releases. They have the suffix `-rcX`, where `X` is the serial number for such a release within a larger one. We will talk about them in [Version Upgrade](#version-upgrade).

### Version Upgrade

ClickHouse is a very fast-developing project: many new features, extensions, and improvements with each release. Thus, keeping the client up to date is a very important job.

Why is it so important?
- Fix security issues, if any.
- Support your users with new features.
- Adopt new database behavior so your users can work with the latest ClickHouse version.
- Fix critical issues that block normal work.

When the version lag is significant, it is almost impossible to upgrade quickly to the latest version. In the case of an emergency fix, it multiplies the problems.
It is fine to skip a few patch versions if there is an established upgrade process every few months. However, skipping a single minor version (the middle digit, where significant changes happen) will cause problems. They are usually found only after something is broken.

Minor versions may have many changes, and some of them need a preview from your side. In this case, we release an `-rc` version and let you know. This version is **only** for preview and not for production use. The preview lasts for a few weeks to let everyone send their feedback. If changes are needed, we will release a new `-rc` and repeat the cycle.

We also recommend using a rolling upgrade so that only a limited set of users get the new version. It minimizes the blast radius.

**Version Upgrade Summary**
- Establish a regular check for a new client version
- Plan upgrades several times per year
- Upgrade to the new minor version as soon as possible
- `-rc` releases are only for preview. Provide your feedback in a timely manner
- Roll out the new version to a limited set of users

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
- Spring demo: [examples/demo-spring-service](../examples/demo-spring-service)

---

## Choosing Between the Java Client and JDBC

When choosing between the Java Client and JDBC Driver, start by considering how ClickHouse differs from typical OLTP databases. ClickHouse is a columnar, analytical database—designed for high-performance analytics, massive scans, and parallel data processing across large datasets, not for transactional (OLTP) workloads like MySQL or PostgreSQL. If you use JDBC simply because it's familiar or widely supported, you may miss out on ClickHouse's true strengths, such as efficient streaming, custom data formats, and bulk operations. JDBC is built around row-oriented, transaction-first APIs, which can be limiting for analytical use cases and may not align with ClickHouse's architecture or optimal access patterns.

### When to use the Java Client (recommended for new work)

Choose the Java Client when you:

- Build a new ingestion or analytics pipeline and control the application code
- Need maximum read/write throughput
- Want to work with ClickHouse **native or binary formats** (`Native`, `RowBinary`, `Parquet`, `JSONEachRow`, ...)
- Need typed POJO serialization/deserialization
- Process data in most effecient way. Client support different binary formats and reads data without additional conversion.  
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
| Performance | Client gives access to output/input stream making it possible to use wide veriaty of performant formats. | JDBC reads/writes data via own API that may become performance bottle-neck in some cases. |

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
