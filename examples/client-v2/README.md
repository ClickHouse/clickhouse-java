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