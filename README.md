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

This is official Java Client and JDBC for ClickHouse Database (https://github.com/ClickHouse/Clickhouse). Java Client is the core component and provides API to interact with the database. In 2023 this component and its API was refactored into a new component `client-v2`. Both version are available but older one will be deprecated soon. However it will receive security and critical bug fixes. New `client-v2` has stable API and we are working on performance and feature parity to make it a production ready.   
JDBC driver component is an implementation of JDBC API. It uses Java Client API to interact with the database server. 

**Benefits of using Client-V2:**
- Stable API. 
- Minimal functionality is implemented
    - SSL & mTLS support 
    - RowBinary* formats support for reading 
    - Proxy support
    - HTTP protocol
- New Insert API that accepts a list of POJOs
- New Query API that returns a list of GenericRecords that cant be used as DTOs
- Native format reader 
- Performance improvements
    - Less number of internal buffers compare to the old client
    - More configuration for performance tuning
    - Less object allocation 
- Upcoming new features

Old client still be used when:
- using JDBC driver ( we are working on its refactoring ) 


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

| Component                 | Maven Central Link | Javadoc Link |
|---------------------------|--------------------|--------------|
| ClickHouse Java Client V2 | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/client-v2)](https://mvnrepository.com/artifact/com.clickhouse/client-v2) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/client-v2/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/client-v2) |

### Compatibility

| ClickHouse Version | Client Version | Comment                                                                                                                                          |
|--------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| Server >= 23.0     | 0.6.2+         |                                                                                                                               |


### Features

- Http API for ClickHouse support
- Bi-directional Compression
  - LZ4
- Insert from POJO (data is provided as list of java objects)
- Query formats support: 
  - RowBinary readers
  - Native format reader
- Apache HTTP Client as HTTP client
  - Connection pooling
  - Failures on retry  
- SSL support
- Cloud support
- Proxy support

### Examples

[Begin-with Usage Examples](../../tree/main/examples/client-v2)

[Spring Demo Service](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/demo-service) 

Minimal client setup: 
```java
String endpoint = "https://<db-instance hostname>:8443/"
Client client = new Client.Builder()
        .addEndpoint(endpoint)
        .setUsername(user)
        .setPassword(password)
        .setDefaultDatabase(database)
        .build();
```                

Insert POJOs example:
```java 

client.register(
  ArticleViewEvent.class, // your DTO class  
  client.getTableSchema(TABLE_NAME)); // corresponding table

List<ArticleViewEvents> events = // load data 

try (InsertResponse response = client.insert(TABLE_NAME, events).get(1, TimeUnit.SECONDS)) {
  // process results 
}

```

Query results reader example:

```java
// Default format is RowBinaryWithNamesAndTypesFormatReader so reader have all information about columns
try (QueryResponse response = client.query(sql).get(3, TimeUnit.SECONDS);) {

    // Create a reader to access the data in a convenient way
    ClickHouseBinaryFormatReader reader = new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream(),
            response.getSettings());

    while (reader.hasNext()) {
        reader.next(); // Read the next record from stream and parse it

        double id = reader.getDouble("id");
        String title = reader.getString("title");
        String url = reader.getString("url");

        // result processing 
    }
}

```


Query result as list of object example:

```java 

// Data is read completely and returned as list of objects.
client.queryAll(sql).forEach(row -> {
              double id = row.getDouble("id");
              String title = row.getString("title");
              String url = row.getString("url");

              // result processing
            });

```

Connecting to the ClickHouse Cloud instance or DB server having not a self-signed certificate: 
```java 
Client client = new Client.Builder()
  .addEndpoint("https://" + dbHost + ":8443")
  .setUsername("default")
  .setPassword("")
  .build(),

```

Connecting to a database instance with self-signed certificate:
```java 
Client client = new Client.Builder()
  .addEndpoint("https://" + dbHost + ":8443")
  .setUsername("default")
  .setPassword("")
  .setRootCertificate("localhost.crt") // path to the CA certificate
  //.setClientKey("user.key") // user private key 
  //.setClientCertificate("user.crt") // user public certificate
  .build(),

```


## Client V1

### Artifacts

| Component | Maven Central Link | Javadoc Link |
|-----------|--------------------|--------------|
| ClickHouse Java Unified Client | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-client)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-client) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/clickhouse-client/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/clickhouse-client) |
| ClickHouse Java HTTP Client | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-http-client)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-http-client) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/clickhouse-http-client/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/clickhouse-http-client) |
| ClickHouse JDBC Driver | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-jdbc)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/clickhouse-jdbc/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/clickhouse-jdbc) |
| ClickHouse R2DBC Driver | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-r2dbc)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-r2dbc) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/clickhouse-r2dbc/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/clickhouse-r2dbc) |
| ClickHouse gRPC Driver | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-grpc-client)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-grpc-client) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/clickhouse-jdbc/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/clickhouse-grpc-client) | [![javadoc](https://javadoc.io/badge2/com.clickhouse/clickhouse-grpc-client/javadoc.svg)](https://javadoc.io/doc/com.clickhouse/clickhouse-grpc-client) |


### Features

- Http API for ClickHouse support
- Bi-directional Compression
  - LZ4
- Apache HTTP Client as HTTP client
  - Connection pooling
  - Failures on retry  
- SSL & mTLS support
- Cloud support
- Proxy support

### Examples

See [java client examples](../../tree/main/examples/client)

See [JDBC examples](../../tree/main/examples/jdbc)

## Compatibility

- All projects in this repo are tested with all [active LTS versions](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) of ClickHouse.
- [Support policy](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md#security-change-log-and-support)
- We recommend to upgrade client continuously to not miss security fixes and new improvements
  - If you have an issue with migration - create and issue and we will respond! 

## Documentation

[Java Client V1 Docs :: ClickHouse website](https://clickhouse.com/docs/en/integrations/language-clients/java/client)

[JDBC Docs :: ClickHouse website](https://clickhouse.com/docs/en/integrations/language-clients/java/jdbc).


## Contributing

Please see our [contributing guide](./CONTRIBUTING.md).
