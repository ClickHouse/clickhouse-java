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

## Runtime Credentials Switch Demo (Two Users)

This standalone example creates two users and demonstrates switching credentials
on the same `Client` instance at runtime via `setCredentials()`.

Run it with endpoint only (admin user defaults to `default`):

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.RuntimeCredentialsTwoUsers" -Dexec.args="http://localhost:8123"
```

Run it with explicit admin credentials:

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.RuntimeCredentialsTwoUsers" -Dexec.args="http://localhost:8123 admin_user admin_password"
```

Notes:
- First argument is server location (endpoint).
- Example uses admin credentials to `CREATE USER` and `DROP USER`.
- Optional database can be overridden with `-DchDatabase=<db>`.