# V1 to V2 client construction

Canonical V1: `examples/client/src/main/java/com/clickhouse/examples/jdbc/Main.java`, `examples/client/src/main/java/com/clickhouse/examples/formats/ProtobufMain.java`.

Canonical V2: `examples/client-v2/.../SimpleReader.java`, `SSLExamples.java`, `Sessions.java`, `Authentication.java`, `RuntimeCredentialsTwoUsers.java`.

## Builder mapping

| V1 | V2 |
|---|---|
| `ClickHouseNode.builder().host(h).port(ClickHouseProtocol.HTTP, p)` | `.addEndpoint("http://h:p")` |
| Same node with SSL or an `https` URI | `.addEndpoint("https://h:p")` |
| `ClickHouseNode.of(uri)` | `.addEndpoint(uri)` when the URI is one HTTP(S) endpoint |
| `.database(db)` / path on the URI | `.setDefaultDatabase(db)` |
| `ClickHouseCredentials.fromUserAndPassword(user, pass)` | `.setUsername(user).setPassword(pass)` |
| `ClickHouseCredentials.fromAccessToken(token)` | `.setAccessToken(token)` |
| Bearer token sent by the application | `.useBearerTokenAuth(token)` (adds the `Bearer ` prefix) |
| Client certificate and key | `.setClientCertificate(path).setClientKey(path).useSSLAuthentication(true)` |
| Custom CA (`SSL_ROOT_CERTIFICATE`) | `.setRootCertificate(pathOrPem)` |
| Trust-any server certificate | `.setSSLMode(SSLMode.TRUST)` plus an `https://` endpoint. `SSLMode` is `com.clickhouse.client.api.enums.SSLMode`. `STRICT` is the default. |
| `ClickHouseClient.newInstance(protocol)` or `.builder().build()` | `new Client.Builder()...build()` |
| `ClickHouseClient.builder().config(new ClickHouseConfig(map))` | Leave `map` for `migrate-v1-client-properties` |
| `client.read(ClickHouseNodes.of("http://a,http://b"))` when the app only lists known endpoints | `.addEndpoint("http://a").addEndpoint("http://b")` |
| `load_balancing_policy`, `failover`, `auto_discovery`, `ClickHouseCluster` | No equivalent. Stop. |
| `ClickHouseProtocol.TCP` or `GRPC` | No equivalent. Stop. |

`addEndpoint(Protocol, host, port, secure)` exists. Prefer the URL overload unless the code already splits scheme, host, and port.

## Lifetime

`examples/client/.../jdbc/Main.java` builds a `ClickHouseClient` inside each method and a `ClickHouseNode` once. The node settings belong on one shared `Client`. Do not keep a new client per `read` or `write`.

```java
Client client = new Client.Builder()
        .addEndpoint("http://localhost:8123")
        .setUsername("default")
        .setPassword("")
        .setDefaultDatabase("default")
        .build();
```

## Sessions and headers

Client-wide `session(...)` on the V1 request becomes `Client.Builder.use(Session)` or `setSessionId`. See `examples/client-v2/.../Sessions.java`. Per-operation sessions stay on the operation settings in the read or write skill.

`httpHeader` and `serverSetting` on the builder are client-wide. Property-bag conversions belong to `migrate-v1-client-properties`.
