<div align="center">
<p>
    <a href="https://github.com/ClickHouse/clickhouse-java/releases/latest"><img src="https://img.shields.io/github/v/release/ClickHouse/clickhouse-java?include_prereleases&label=Latest%20Release"/></a>
    <a href="https://s01.oss.sonatype.org/content/repositories/snapshots/com/clickhouse/"><img src="https://img.shields.io/nexus/s/com.clickhouse/clickhouse-java?label=Nightly%20Build&server=https%3A%2F%2Fs01.oss.sonatype.org"/></a>
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

This repository is of the official Java Client and JDBC for ClickHouse Database (https://github.com/ClickHouse/Clickhouse) source code. Java Client is the core component and provides an API to interact with the database. In 2023, this component and its API were refactored into a new component, `client-v2`. Both versions are available, but the older one will be deprecated soon. However, it will receive security and critical bug fixes. The new `client-v2` has a stable API, and we are working on performance and feature parity to make it production-ready.   
The JDBC driver component is an implementation of JDBC API. It uses Java Client API to interact with the database server. 

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
| ClickHouse Java Client V2 | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/client-v2)](https://mvnrepository.com/artifact/com.clickhouse/client-v2) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/client-v2/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/client-v2) | [docs](https://clickhouse.com/docs/integrations/java/client-v2) | 

### Examples

[Begin-with Usage Examples](../../tree/main/examples/client-v2)

[Spring Demo Service](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/demo-service) 


## JDBC Driver

### Artifacts

| Component | Maven Central Link | Javadoc Link | Documentation Link |
|-----------|--------------------|--------------|--------------------|
| ClickHouse JDBC Driver | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-jdbc)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc) |[![javadoc](https://javadoc.io/badge2/com.clickhouse/clickhouse-jdbc/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/clickhouse-jdbc)|[docs](https://clickhouse.com/docs/integrations/java/jdbc-v2)|

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
