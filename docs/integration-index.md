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
- Adopt new database behavior so your users can work with the latest ClickHouse version.
- Fix critical issues that block normal work.
- Fix security issues, if any.
- Support your users with new features.

When the version lag is significant, it is almost impossible to upgrade quickly to the latest version. In the case of an
emergency fix, it multiplies the problems. It is fine to skip a few patch versions if there is an established upgrade
process every few months. However, skipping a single minor version (the middle digit, where significant changes happen)
will cause problems. They are usually found only after something is broken.

We highlight breaking and significant changes in release notes. See also release specific migration guides in `docs/releases/` in this repo.
Migration guide for V1-to-V2 is in main documentation separetly for [Client](https://clickhouse.com/docs/integrations/language-clients/java/client#migration_guide) 
and [JDBC](https://clickhouse.com/docs/integrations/language-clients/java/jdbc#migration-guide). 

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

| Component | Maven artifact | Description |
|-----------|----------------|------|
| **Java Client** | `com.clickhouse:client-v2` | Main DB API for JVM applications.  |
| **JDBC Driver** | `com.clickhouse:clickhouse-jdbc` | JDBC driver for ClickHouse. It uses Java Client underneath.  |

**A note on names.** The current JDBC driver is often called "JDBC V2". This refers to the `jdbc-v2` source module in this repository, which is the modern rewrite of the driver on top of `client-v2`. You do not depend on `jdbc-v2` directly: the published artifact you add to your build is `com.clickhouse:clickhouse-jdbc`, which bundles the `jdbc-v2` implementation. In short, `clickhouse-jdbc` **is** the JDBC V2 driver.

Client is the main library providing fundamental APIs and classes. The client can be used directly by an application and is the simplest way to access all ClickHouse advantages: native binary formats, high-throughput streaming, and advanced data types.  
The JDBC driver is a thin adaptation layer that uses the Java Client underneath to communicate with ClickHouse. As a result, it can only use features implemented in the client. The main limitation of the driver, as with any adapter, is its API: JDBC was designed for traditional row-based database access. Because of JDBC's fixed type model, applications often need to work with generic objects or add custom handling to fully leverage ClickHouse-specific data types. We discuss library selection in the next section.  

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

Making the right choice between the client and driver is crucial for application architecture and long-term maintainability. 

- **Java Client (`client-v2`)** provides maximum performance and flexibility. It offers modular abstractions that allow custom extensions, direct binary stream readers/writers, typed POJO mapping, and fine-grained control over ClickHouse-specific settings without JDBC protocol overhead.
- **JDBC Driver (`clickhouse-jdbc`)** enables fast adoption in applications already built around the JDBC API. However, the rigid `java.sql.*` interface is designed for traditional row-based DB access and can become a limiting factor for advanced features (such as direct wire streams, server-bound parameters, and native formats).

Each use case is unique, and in complex applications, combining both libraries (for instance, JDBC for framework integration and Java Client for high-throughput batch ingestion) is an effective strategy.

### Use-Case Decision Matrix

| Application Requirement / Use Case | Recommended Tool | Key Reasons & Capabilities | Trade-Offs & Notes |
|-------------------------------------|------------------|----------------------------|--------------------|
| **Loading data from different formats** | **Java Client** (`client-v2`) | Direct support for ClickHouse binary and streaming formats (`RowBinary`, `Native`, `Parquet`, `JSONEachRow`, `CSV`); processes input streams directly without intermediate row conversion overhead. | Requires application code written against `client-v2` APIs instead of standard `java.sql.*`. *(ETL bulk ingestion pipelines are an example).* |
| **Fetching data as fast as possible** | **Java Client** (`client-v2`) | Peak read/write throughput; avoids JDBC `ResultSet` object allocation per row and leverages stream deserialization and native formats. | Application code interacts directly with ClickHouse binary readers or POJO mappings. |
| **Fetching data in a uniform, DB-agnostic way** | **JDBC Driver** (`clickhouse-jdbc`) | Standard `java.sql.Connection` and `ResultSet` APIs enable a single, uniform data access layer across ClickHouse and relational databases. | Cannot easily leverage ClickHouse-specific wire formats or server-bound parameters. |
| **Rendering database metadata across databases** | **JDBC Driver** (`clickhouse-jdbc`) | Standard `java.sql.DatabaseMetaData` interface provides a unified API for inspecting catalogs, schemas, tables, and column data types across engines. | *(Note: `client-v2` offers `Client.getTableSchema()` for ClickHouse-specific schema discovery).* |
| **Providing a general view of the database** | **JDBC Driver** (`clickhouse-jdbc`) | Standard `ResultSet` and `ResultSetMetaData` interfaces support generic tabular rendering, schema browsing, and row-by-row data preview without format-specific parsers. | Row-oriented `ResultSet` iteration can become a throughput bottleneck for massive analytical exports. *(BI tools and SQL consoles are examples).* |
| **Integrating with JDBC frameworks & pools** | **JDBC Driver** (`clickhouse-jdbc`) | Plug-and-play integration with standard Java persistence frameworks (Hibernate, MyBatis), connection pools (HikariCP), and generic JDBC sources/sinks. | Abstraction layer adds overhead; ClickHouse-specific types require manual casting (`ResultSet.getObject()`). |
| **Fine-grained execution control & settings** | **Java Client** (`client-v2`) | Direct control over query settings, session state, data compression, operation-level timeouts, and HTTP transport customization. | Operational settings are managed via `Client.Builder` or `ClientConfigProperties` rather than JDBC properties. |
| **Hybrid usage: Structure viewing via JDBC + Direct stream manipulation via Client** | **Both** (`JDBC` + `client-v2`) | Uses standard JDBC `DatabaseMetaData` for unified database structure viewing and schema inspection, while data manipulation/ingestion accesses `Client` directly or unwraps `conn.unwrap(ConnectionImpl.class).getClient()` for high-throughput streaming. | Requires unwrapping `Connection` or holding both API references, but combines standard metadata introspection with raw ClickHouse stream performance. |

### Decision Summary

- **Choose the Java Client (`client-v2`) when:** Your application loads data from various wire formats, requires maximum data fetch throughput, uses typed POJOs, or needs fine-grained operational control over ClickHouse settings.
- **Choose the JDBC Driver (`clickhouse-jdbc`) when:** Your application needs uniform database access, renders metadata across multiple database engines, provides a general database view, or integrates with standard JDBC frameworks and connection pools.
- **Combine both (Hybrid Approach) when:** Your application uses standard JDBC for database structure exploration and schema discovery (`DatabaseMetaData`), while switching to the underlying Java Client (`conn.unwrap(ConnectionImpl.class).getClient()`) for high-performance data manipulation, streaming, or binary format processing.

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
| Performance | Client gives access to output/input stream making it possible to use wide variety of performant formats. | JDBC reads/writes data via own API that may become performance bottleneck in some cases. |

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
