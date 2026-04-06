# Client V2 Example

## Overview

This example demonstrates how to use the client V2 to interact with the ClickHouse server.

## How to Run

Apache Maven or IDE with Maven support is required to run this example.

First we need to compile the example :
```shell
mvn clean compile
```

To run:
```shell
 mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.Main"
```

Addition options can be passed to the application:
- `-DchEndpoint` - Endpoint to connect in the format of URL (default: http://localhost:8123/)
- `-DchUser` - ClickHouse user name (default: default)
- `-DchPassword` - ClickHouse user password (default: empty)
- `-DchDatabase` - ClickHouse database name (default: default)

## Secure Connection Example (Private CA + Host Verification)

Run the SSL example:

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.ssl.SecureConnectionMain"
```

Behavior:
- If `--host` is set, the example uses an external ClickHouse server.
- If `--host` is not set, the example starts a local ClickHouse test container.
- If `--root-ca` is set without `--host`, local mode uses the provided CA to sign a local server certificate. This is useful to test your own CA material.

### Local mode (auto-generated CA and server certificates)

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.ssl.SecureConnectionMain"
```

### Local mode with user-provided CA certificate and key

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.ssl.SecureConnectionMain" -Dexec.args="--root-ca /path/to/ca.crt --root-ca-key /path/to/ca.key"
```

`--root-ca-key` is optional only when `--root-ca` PEM already includes an unencrypted private key.

### External mode (no docker container started)

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.ssl.SecureConnectionMain" -Dexec.args="--host clickhouse.example.com --port 8443 --user default --password secret --root-ca /path/to/ca.crt --database default"
```