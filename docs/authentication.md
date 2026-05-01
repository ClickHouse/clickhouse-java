# ClickHouse Authentication Mechanisms

ClickHouse supports several authentication mechanisms to ensure secure access to the database. The ClickHouse Java Client (`client-v2`) provides built-in support for the most common authentication methods, as well as the flexibility to use custom approaches.

This document describes the available authentication mechanisms and how to configure them in the Java client.

---

## 1. Basic Authentication (Username and Password)

### Overview
Basic authentication is the standard and most commonly used method in ClickHouse. It involves providing a valid username and a password. By default, ClickHouse installations come with a `default` user and no password, though administrators typically configure additional users with strong passwords.

### Client Configuration
You can configure the username and password during the initialization of the client using `Client.Builder`:

```java
Client client = new Client.Builder()
    .addEndpoint("http://localhost:8123")
    .setUsername("my_user")
    .setPassword("my_password")
    .build();
```

If you need to update the credentials dynamically at runtime (for subsequent requests), you can use the client instance:

```java
client.updateUserAndPassword("new_user", "new_password");
```

> Note: the runtime updaters do not switch the authentication method.
> A client built with username/password can only be updated with
> `updateUserAndPassword(...)`; calling a token-based updater on it throws
> `ClientMisconfigurationException`.

---

## 2. Token-based Authentication (Access / Bearer Token)

### Overview
Token-based authentication allows clients to authenticate using a pre-generated access token (such as an API key) instead of a traditional username and password. In HTTP interactions, this token is typically sent in the `Authorization: Bearer <token>` header.

### Client Configuration
For the standard `Authorization: Bearer <token>` scheme use `useBearerTokenAuth(...)` on the builder. The client will prepend the `Bearer ` prefix for you:

```java
Client client = new Client.Builder()
    .addEndpoint("http://localhost:8123")
    .useBearerTokenAuth("my_access_token")
    .build();
```

If you need to send a non-`Bearer` scheme (or you already have the full header value), use `setAccessToken(...)`. The value is sent verbatim as the `Authorization` header, so include the scheme prefix yourself:

```java
Client client = new Client.Builder()
    .addEndpoint("http://localhost:8123")
    .setAccessToken("Bearer my_access_token") // value used as-is
    .build();
```

You can also update the token dynamically at runtime. `updateBearerToken(...)` adds the `Bearer ` prefix; `updateAccessToken(...)` writes the value as-is:

```java
client.updateBearerToken("new_access_token");      // -> Authorization: Bearer new_access_token
client.updateAccessToken("Bearer new_access_token"); // -> Authorization: Bearer new_access_token
```

> Note: as with username/password, a client built without an access token cannot
> be promoted to token-based authentication at runtime.

---

## 3. Mutual TLS (mTLS) / Client Certificates

### Overview
Mutual TLS (mTLS) provides a high level of security by requiring both the server and the client to authenticate each other using SSL/TLS certificates. The client must present a valid client certificate and its corresponding private key to authenticate successfully with the ClickHouse server.

### Client Configuration
To use mTLS authentication in the Java client, you need to enable SSL authentication and provide the paths to your client certificate and private key:

```java
Client client = new Client.Builder()
    .addEndpoint("https://localhost:8443")
    .useSSLAuthentication(true)
    .setClientCertificate("/path/to/client.crt")
    .setClientKey("/path/to/client.key")
    // Optionally provide the root CA certificate if the server uses a self-signed cert
    .setRootCertificate("/path/to/ca.crt") 
    .build();
```

*Note: You can also configure a TrustStore using `setSSLTrustStore`, `setSSLTrustStorePassword`, and `setSSLTrustStoreType` instead of raw certificates.*

---

## 4. Custom HTTP Headers

### Overview
For advanced or custom authentication proxies (such as OAuth gateways, custom API gateways, or specific ClickHouse HTTP handler configurations), you may need to pass custom authentication headers (e.g., `X-ClickHouse-Key`, or a custom `Authorization` scheme).

### Client Configuration
You can inject arbitrary HTTP headers into all requests made by the client using the builder's `httpHeader` method:

```java
Client client = new Client.Builder()
    .addEndpoint("http://localhost:8123")
    // Example: Passing a custom API key header
    .httpHeader("X-API-Key", "my_custom_api_key")
    // Example: Passing a custom Authorization header
    .httpHeader("Authorization", "CustomScheme my_token")
    .build();
```

--- 
## Migration from Client V1

When migrating from Client V1 (`clickhouse-client`) to Client V2 (`client-v2`), the configuration properties related to authentication have been unified under `ClientConfigProperties` and exposed directly on `Client.Builder`. You no longer need to use `ClickHouseCredentials` objects.

| Client V1 (`ClickHouseClientOption` / `ClickHouseCredentials`) | Client V2 (`ClientConfigProperties` / Builder methods) | Description |
|----------------------------------------------------------------|--------------------------------------------------------|-------------|
| `ClickHouseCredentials(user, password)`                        | `user`, `password` / `setUsername()`, `setPassword()`  | Basic auth. |
| `ClickHouseCredentials.fromAccessToken(token)`                 | `access_token` / `setAccessToken()`                    | Token auth. |
| `SSL` (`ssl`)                                                  | `ssl_authentication` / `useSSLAuthentication()`        | Enable mTLS authentication. |
| `SSL_CERTIFICATE` (`sslcert`)                                  | `sslcert` / `setClientCertificate()`                   | Path to the client certificate. |
| `SSL_KEY` (`sslkey`)                                           | `ssl_key` / `setClientKey()`                           | Path to the client private key. |
| `SSL_ROOT_CERTIFICATE` (`sslrootcert`)                         | `sslrootcert` / `setRootCertificate()`                 | Path to the CA/Root certificate. |
| `TRUST_STORE` (`trust_store`)                                  | `trust_store` / `setSSLTrustStore()`                   | Path to the Trust Store. |
| `KEY_STORE_PASSWORD` (`key_store_password`)                    | `key_store_password` / `setSSLTrustStorePassword()`    | Password for the Trust/Key Store. |
| `KEY_STORE_TYPE` (`key_store_type`)                            | `key_store_type` / `setSSLTrustStoreType()`            | Type of the Trust/Key Store (e.g. JKS). |



---

## Summary

When configuring the client, ensure that you only provide the necessary credentials for a single authentication mechanism to avoid misconfiguration errors. The `CredentialsManager` inside `client-v2` validates that only one primary form of authentication (e.g. SSL Auth, Password, or Token) is actively used per client setup.