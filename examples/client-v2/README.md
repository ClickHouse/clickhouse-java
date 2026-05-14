# Client V2 Example

## Overview

This module contains runnable examples demonstrating how to use `client-v2` to interact with a ClickHouse server.

## How to Run

Apache Maven or IDE with Maven support is required to run this example.

First compile the examples:
```shell
mvn clean compile
```

Both `Main` and `Sessions` are executable entry points and use the same connection properties.

Run the general end-to-end example:
```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.Main"
```

Run the sessions example:
```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.Sessions"
```

Additional options can be passed to either application:
- `-DchEndpoint` - Endpoint to connect in the format of URL (default: http://localhost:8123/)
- `-DchUser` - ClickHouse user name (default: default)
- `-DchPassword` - ClickHouse user password (default: empty)
- `-DchDatabase` - ClickHouse database name (default: default)

Example with custom connection properties:
```shell
mvn exec:java \
  -Dexec.mainClass="com.clickhouse.examples.client_v2.Sessions" \
  -DchEndpoint="http://localhost:8123" \
  -DchUser="default" \
  -DchPassword="" \
  -DchDatabase="default"
```

## Executable Examples

`com.clickhouse.examples.client_v2.Main`
- Shows the existing read and write flows for `client-v2`.

`com.clickhouse.examples.client_v2.Sessions`
- Shows client-wide session configuration with `Client.Builder.use(session)`.
- Shows operation-wide session configuration with `settings.use(session)`.
- Shows using two different sessions with one client.
- Shows how to detect and handle session timeout using `session_check`.

Note: HTTP sessions require server affinity. Use a single endpoint or sticky routing when requests go through a load balancer.

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