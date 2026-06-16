
# Example of ClickHouse JDBC Client Usage 

This example application uses JDBC to interact with ClickHouse Server.


## Usage 

Apache Maven or IDE with Maven support is required to run this example.  

To compile:
```shell
mvn clean compile
```

To run simplified example:
```shell
 mvn exec:java -Dexec.mainClass="com.clickhouse.examples.jdbc.Basic"
```

Addition options can be passed to the application:
- `-DchUrl` - ClickHouse JDBC URL. Default is `jdbc:clickhouse://localhost:8123/default`
- `-Dclickhouse.jdbc.v2=true` - Use JDBC V2 implementation

## SSL Examples

`com.clickhouse.examples.jdbc.SSLExamples` shows how to connect securely to a server whose
certificate is signed by a custom (private) CA. Only the CA certificate is passed with the
`sslrootcert` connection property - no trust store configuration is required, and the JVM default
trust store stays untouched.

The example runs in one of two modes.

### Local mode (dockerized server, default)

Verifies the whole scenario end to end: the example generates a private CA and a server
certificate, starts a local ClickHouse server in Docker configured with them, and connects using
only the generated CA certificate. Requires a running Docker daemon.

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.jdbc.SSLExamples"
```

Optional:
- `-DchImage` - Docker image to use (default: `clickhouse/clickhouse-server:latest`)

### Standalone mode (your own server)

Once the local scenario works, verify your own instance by passing its JDBC URL and the CA
certificate that signed its server certificate:

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.jdbc.SSLExamples" \
  -DchUrl="jdbc:clickhouse://clickhouse.example.com:8443/default" \
  -DchUser="default" \
  -DchPassword="secret" \
  -DchRootCert="/path/to/ca.crt"
```

`-DchRootCert` is required in this mode and must point to the CA certificate in PEM format.

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
  
# Only for demo purpose we make key readable by all. On production should be readable only by owner, not even by group.^M
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

4. Run the example in standalone mode with
`-DchUrl="jdbc:clickhouse://localhost:8443/default" -DchRootCert="$PWD/ca.crt"`.

```shell
mvn exec:java -Dexec.mainClass="com.clickhouse.examples.jdbc.SSLExamples" \
  -DchUrl="jdbc:clickhouse:https://localhost:8443/default" \
  -DchUser="default" \
  -DchPassword="secret" \
  -DchRootCert="$PWD/ca.crt"
```


The full description of the server-side TLS configuration is in the official documentation:
[Configuring SSL-TLS](https://clickhouse.com/docs/en/guides/sre/configuring-ssl).