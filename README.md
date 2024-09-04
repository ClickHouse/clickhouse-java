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

There are two implementations of the Java Client: 
- client-v1 - initial implementation (projects: clickhouse-client, clickhouse-data, clickhouse-http-client)
  - still maintained
  - only critical fixes & features
- client-v2 - refactored implementation (projects: client-v2)
  - essential functionality is implemented
  - works with cloud
  - we are working on performance right now
  - also we will refactor JDBC driver to use this client


## Important

### Upcomming deprecations:
| Component                      | Version | Comment                                          |
|--------------------------------|---------|--------------------------------------------------|
| Clickhouse CLI Client          | 0.7.0   |                                                  |
| ClickHouse GRPC Client         | 0.7.0   | Please use the clickhouse http protocol instead  |

## Installation

Releases: Maven Central (web site https://mvnrepository.com/artifact/com.clickhouse)

Nightly Builds: https://s01.oss.sonatype.org/content/repositories/snapshots/com/clickhouse/

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

| Component | Maven Central Link |
|-----------|--------------------|
| ClickHouse Java HTTP Client | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-client)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-http-client) |
| ClickHouse JDBC Driver | [![Maven Central](https://img.shields.io/maven-central/v/com.clickhouse/clickhouse-jdbc)](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc) |

### Compatibility

| ClickHouse Version | Client Version | Comment                                                                                                                                      |
|--------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| Server < 20.7      | 0.3.1-patch    | use 0.3.1-patch(or 0.2.6 if you're stuck with JDK 7)                                                                                         |
| Server >= 20.7     | 0.3.2          | use 0.3.2 or above. All [active releases](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) are supported. |
| Server >= 23.0     | >0.6.0          | use 0.6.0 or above.                                                                                                                          |

### Features

- Http API for ClickHouse support
- Bi-directional Compression
  - LZ4
- Apache HTTP Client as HTTP client
  - Connection pooling
  - Failures on retry  
- SSL support
- Cloud support
- Proxy support

### Examples

See [java client examples](../../tree/main/examples/client)

See [JDBC examples](../../tree/main/examples/jdbc)

## Documentation

[Java Client V1 Docs :: ClickHouse website](https://clickhouse.com/docs/en/integrations/language-clients/java/client)

[JDBC Docs :: ClickHouse website](https://clickhouse.com/docs/en/integrations/language-clients/java/jdbc).


## Contributing

Please see our [contributing guide](./CONTRIBUTING.md).
