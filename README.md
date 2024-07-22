<div align="center">
<p>
    <a href="https://github.com/ClickHouse/clickhouse-java/releases/latest"><img src="https://img.shields.io/github/v/release/ClickHouse/clickhouse-java?include_prereleases&label=Latest%20Release"/></a>
    <a href="https://s01.oss.sonatype.org/content/repositories/snapshots/com/clickhouse/"><img src="https://img.shields.io/nexus/s/com.clickhouse/clickhouse-java?label=Nightly%20Build&server=https%3A%2F%2Fs01.oss.sonatype.org"/></a>
    <a href="https://github.com/ClickHouse/clickhouse-java/milestone/4"><img src="https://img.shields.io/github/milestones/progress-percent/ClickHouse/clickhouse-java/16"/></a>
    <a href="https://github.com/ClickHouse/clickhouse-java/releases/"><img src="https://img.shields.io/github/downloads/ClickHouse/clickhouse-java/latest/total"/></a>
</p>
<p><img src="https://github.com/ClickHouse/clickhouse-js/blob/a332672bfb70d54dfd27ae1f8f5169a6ffeea780/.static/logo.svg" width="200px" align="center"></p>
<h1>ClickHouse Java Client & JDBC Driver</h1>
</div>

Table of Contents
* [About The project](#about-the-project)
* [Important Updates](#important-updates)
* [Installation](#installation)
* [Client V2](#client-v2)
  * [Artifacts](#artifacts)
  * [Features](#features)
  * [Examples](#examples)
* [Client V1](#client-v1)
  * [Artifacts](#artifacts-1)
  * [Features](#features-1)
  * [Examples](#examples-1)
* [Documentation](#documentation)
* [Contributing](#contributing)

## About the Project

This is the official Java Client and JDBC for ClickHouse Database (https://github.com/ClickHouse/Clickhouse).
Java client is the base component and has own API for working with ClickHouse in a "direct" way. JDBC driver is
a library implementing JDBC API 1.3 on top of the Java client.

Currently, there is a new version of the Java Client in developer preview. It is going to replace the old one very soon.
Source code of the new version is located in the `client-v2` directory and usage examples are in `examples/client-v2` directory.

## Important Updates

Next components are deprecated and will be removed in the future releases:
| Component                      | Version | Comment                                          |
|--------------------------------|---------|--------------------------------------------------|
| Clickhouse ClI Client          | 0.7.0   |                                                  |
| ClickHouse GRPC Client         | 0.7.0   | Please use the clickhouse http protocol instead  |

## Installation

Release versions are available in the Maven Central Repository. Nightly builds are available in the Sonatype Nexus Repository.

Nightly Builds Repo: https://s01.oss.sonatype.org/content/repositories/snapshots/com/clickhouse/

## Client V2

### Artifacts

| Component                 | Maven Central Link |
|---------------------------|--------------------|
| ClickHouse Java Client V2 | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/client-v2)](https://mvnrepository.com/artifact/com.clickhouse/client-v2) |

### Compatibility

| ClickHouse Version | Client Version | Comment                                                                                                                                          |
|--------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| Server >= 23.0     | 0.6.2          |                                                                                                                               |


### Features

- Http API for ClickHouse support
- Bi-directional Compression
  - LZ4
- Proxy support

### Examples

See [java client examples](../../tree/main/examples/client-v2)

## Client V1

### Artifacts

| Component | Maven Central Link |
|-----------|--------------------|
| ClickHouse Java Client | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-client)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-client) |
| ClickHouse JDBC Driver | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-jdbc)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc) |

### Compatibility

| ClickHouse Version | Client Version | Comment                                                                                                                                      |
|--------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| Server < 20.7      | 0.3.1-patch    | use 0.3.1-patch(or 0.2.6 if you're stuck with JDK 7)                                                                                         |
| Server >= 20.7     | 0.3.2          | use 0.3.2 or above. All [active releases](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) are supported. |
| Server >= 23.0     | 0.6.0          | use 0.6.0 or above.                                                                                                                          |

### Features

- Http API for ClickHouse support
- Bi-directional Compression
  - LZ4
- Proxy support
- SSL & mTLS support


### Examples

See [java client examples](../../tree/main/examples/client)

See [JDBC examples](../../tree/main/examples/jdbc)

## Documentation

[Java Client V1 Docs :: ClickHouse website](https://clickhouse.com/docs/en/integrations/language-clients/java/client)

[JDBC Docs :: ClickHouse website](https://clickhouse.com/docs/en/integrations/language-clients/java/jdbc).


## Contributing

Please see our [contributing guide](./CONTRIBUTING.md).
