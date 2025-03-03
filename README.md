<div align="center">
<p>
    <a href="https://github.com/ClickHouse/clickhouse-java/releases/latest"><img src="https://img.shields.io/github/v/release/ClickHouse/clickhouse-java?include_prereleases&label=Latest%20Release&color=green"/></a>
    <a href="https://s01.oss.sonatype.org/content/repositories/snapshots/com/clickhouse/"><img src="https://img.shields.io/nexus/s/com.clickhouse/clickhouse-java?label=Nightly%20Build&server=https%3A%2F%2Fs01.oss.sonatype.org&color=green"/></a>
    <a href="https://github.com/ClickHouse/clickhouse-java/releases/"><img src="https://img.shields.io/github/downloads/ClickHouse/clickhouse-java/latest/total"/></a>
</p>
<p><img src="https://github.com/ClickHouse/clickhouse-js/blob/a332672bfb70d54dfd27ae1f8f5169a6ffeea780/.static/logo.svg" width="200px" align="center"></p>
<h1>ClickHouse Java Client & JDBC Driver</h1>
</div>

Table of Contents
* [About The project](#about-the-project)
* [Important](#important)
* [Installation](#installation)
* [Client V2](#client-v2)
  * [Artifacts](#artifacts)
  * [Examples](#examples)
* [Client V1](#client-v1)
  * [Artifacts](#artifacts-1)
  * [Examples](#examples-1)
* [Contributing](#contributing)

## About the Project

This is a repo of the Java Client and JDBC Driver for ClickHouse Database (https://github.com/ClickHouse/Clickhouse) supported by the ClickHouse team. The Java Client is the core component that provides an API to interact with the database via HTTP Protocol.    
The JDBC driver component implements the JDBC specification and communicates with ClickHouse using the Java Client API. 
Historically, there are two versions of both components. The previous version of the Java client required a significant rewrite, so we decided to create a new one, `client-v2`, not to disturb anyone's work and to give time for migration. The JDBC driver also required changes to be compatible with the new client and comply more with JDBC specs, and we created `jdbc-v2`. This component will replace an old version (to keep the artifact name).

## Client Features

| Name                                         | Client V2 | Client V1 | Comments
|----------------------------------------------|:---------:|:---------:|:---------:|
| Http Connection                              |✔       |✔      | |
| Http Compression (LZ4)                       |✔       |✔      | |
| Server Response Compression - LZ4            |✔       |✔      | | 
| Client Request Compression - LZ4             |✔       |✔      | |
| HTTPS                                        |✔       |✔      | |
| Client SSL Cert (mTLS)                       |✔       |✔      | |
| Http Proxy with Authentication               |✔       |✔      | |
| Java Object SerDe                            |✔       |✗      | |
| Connection Pool                              |✔       |✔      | Apache HTTP Client only |
| Named Parameters                             |✔       |✔      | |
| Retry on failure                             |✔       |✔      | |
| Failover                                     |✗       |✔      | |
| Load-balancing                               |✗       |✔      | |
| Server auto-discovery                        |✗       |✔      | |
| Log Comment                                  |✔       |✔      | |
| Session Roles                                |✔       |✔      | |
| SSL Client Authentication                    |✔       |✔      | |
| Session timezone                             |✔       |✔      | |


## Important

### Upcoming deprecations:
| Component                      | Version | Comment                                          |
|--------------------------------|---------|--------------------------------------------------|
| ClickHouse Java v1 | TBC | We'll be deprecating Java v1 in 2025 |


## Installation

Releases: Maven Central (web site https://mvnrepository.com/artifact/com.clickhouse)

Nightly Builds: https://s01.oss.sonatype.org/content/repositories/snapshots/com/clickhouse/

## Client V2

### Artifacts

| Component                 | Maven Central Link | Javadoc Link | Documentation Link | 
|---------------------------|--------------------|--------------|---------------------|
| ClickHouse Java Client V2 | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/client-v2)](https://mvnrepository.com/artifact/com.clickhouse/client-v2) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/client-v2/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/client-v2) | [docs](https://clickhouse.com/docs/integrations/java) | 

### Examples

[Begin-with Usage Examples](../../tree/main/examples/client-v2)

[Spring Demo Service](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/demo-service) 


## JDBC Driver

### Artifacts

| Component | Maven Central Link | Javadoc Link | Documentation Link |
|-----------|--------------------|--------------|--------------------|
| ClickHouse JDBC Driver | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-jdbc)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc) |[![javadoc](https://javadoc.io/badge2/com.clickhouse/clickhouse-jdbc/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/clickhouse-jdbc)|[docs](https://clickhouse.com/docs/integrations/language-clients/java/jdbc)|

### Examples

See [JDBC examples](../../tree/main/examples/jdbc)

## R2DBC Driver

### Artifacts

| Component | Maven Central Link | Javadoc Link | Documentation Link |
|-----------|--------------------|--------------|--------------------|
| ClickHouse R2DBC Driver | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-r2dbc)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-r2dbc) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/clickhouse-r2dbc/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/clickhouse-r2dbc) | [docs](https://clickhouse.com/docs/integrations/java/r2dbc)|


### Misc Artifacts 

| Component | Maven Central Link | Javadoc Link |
|-----------|--------------------|--------------|
| ClickHouse Java Unified Client | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-client)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-client) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/clickhouse-client/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/clickhouse-client) |
| ClickHouse Java HTTP Client | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-http-client)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-http-client) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/clickhouse-http-client/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/clickhouse-http-client) |

## Compatibility

- All projects in this repo are tested with all [active LTS versions](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) of ClickHouse.
- [Support policy](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md#security-change-log-and-support)
- We recommend to upgrade client continuously to not miss security fixes and new improvements
  - If you have an issue with migration - create and issue and we will respond! 

## Contributing

Please see our [contributing guide](./CONTRIBUTING.md).
