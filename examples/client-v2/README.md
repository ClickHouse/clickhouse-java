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
on the same `Client` instance at runtime via `updateUserAndPassword()`.

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

## SSL Examples

`com.clickhouse.examples.client_v2.SSLExamples` shows how to connect securely to a server:

- **Custom CA certificate** - the server certificate is signed by a custom (private) CA. Only the
  CA certificate is passed to the client with `Client.Builder.setRootCertificate()` (as a file path
  or directly as a PEM string) - no trust store configuration is required, and the JVM default
  trust store stays untouched.
- **Self-signed certificate without verification** - `Client.Builder.setSSLMode(SSLMode.Trust)`
  accepts any server certificate and skips hostname verification. The connection is encrypted, but
  the server identity is not verified - use it only for testing or in fully trusted environments.

The example runs in one of two modes.

### Local mode (dockerized server, default)

Verifies the whole scenario end to end: the example generates a private CA and a server
certificate, starts a local ClickHouse server in Docker configured with them, and connects using
only the generated CA certificate. Requires a running Docker daemon.

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.SSLExamples"
```

Optional:
- `-DchImage` - Docker image to use (default: `clickhouse/clickhouse-server:latest`)

### Standalone mode (your own server)

Once the local scenario works, verify your own instance by passing its address and the CA
certificate that signed its server certificate:

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.SSLExamples" \
  -DchHost="clickhouse.example.com" \
  -DchPort="8443" \
  -DchUser="default" \
  -DchPassword="secret" \
  -DchDatabase="default" \
  -DchRootCert="/path/to/ca.crt"
```

`-DchRootCert` must point to the CA certificate in PEM format. When it is omitted, only the
self-signed (`SSLMode.Trust`) example runs - useful when you do not have the CA certificate at hand.

### Setting up a Docker dev instance with a self-signed certificate manually

The local mode does all of this automatically, but the same setup can be created by hand.

1. Generate a private CA and a server certificate signed by it (`CN`/SAN must match the hostname
you will connect to, `localhost` in this example):

```shell
# Private CA
openssl req -x509 -newkey rsa:2048 -days 365 -nodes \
  -keyout ca.key -out ca.crt -subj "/CN=ExamplePrivateCA"

# Server key and certificate signing request
openssl req -newkey rsa:2048 -nodes \
  -keyout server.key -out server.csr -subj "/CN=localhost"

# Server certificate signed by the CA, with SANs for localhost
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 365 -out server.crt \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")
  
# Only for demo purpose we make key readable by all. On production should be readable only by owner, not even by group.
chmod a+r server.key
```

2. Create a `config.d` overlay enabling the HTTPS interface, e.g. `my_ssl.xml`:

```xml
<clickhouse>
    <https_port>8443</https_port>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
            <verificationMode>none</verificationMode>
            <loadDefaultCAFile>true</loadDefaultCAFile>
            <disableProtocols>sslv2,sslv3</disableProtocols>
            <preferServerCiphers>true</preferServerCiphers>
        </server>
    </openSSL>
</clickhouse>
```

3. Start the server with the certificates and the configuration mounted:

```shell
docker run -d --name clickhouse-ssl -p 8443:8443 \
  -v "$PWD/server.crt:/etc/clickhouse-server/certs/server.crt:ro" \
  -v "$PWD/server.key:/etc/clickhouse-server/certs/server.key:ro" \
  -v "$PWD/my_ssl.xml:/etc/clickhouse-server/config.d/my_ssl.xml:ro" \
  -e CLICKHOUSE_PASSWORD="secret" \
  clickhouse/clickhouse-server:latest
```

4. Run the example in standalone mode with `-DchHost=localhost -DchRootCert="$PWD/ca.crt"`.

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.client_v2.SSLExamples" \
  -DchHost="localhost" \
  -DchPort="8443" \
  -DchUser="default" \
  -DchPassword="secret" \
  -DchDatabase="default" \
  -DchRootCert="$PWD/ca.crt"
```

The full description of the server-side TLS configuration is in the official documentation:
[Configuring SSL-TLS](https://clickhouse.com/docs/en/guides/sre/configuring-ssl).