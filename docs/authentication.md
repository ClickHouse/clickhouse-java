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
client.setCredentials("new_user", "new_password");
```

---

## 2. Token-based Authentication (Access / Bearer Token)

### Overview
Token-based authentication allows clients to authenticate using a pre-generated access token (such as an API key) instead of a traditional username and password. In HTTP interactions, this token is typically sent in the `Authorization: Bearer <token>` header.

### Client Configuration
To configure an access token, use the `setAccessToken` method in the builder:

```java
Client client = new Client.Builder()
    .addEndpoint("http://localhost:8123")
    .setAccessToken("my_access_token")
    .build();
```

You can also update the token dynamically at runtime:

```java
client.setAccessToken("new_access_token");
```

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

## Summary

When configuring the client, ensure that you only provide the necessary credentials for a single authentication mechanism to avoid misconfiguration errors. The `CredentialsManager` inside `client-v2` validates that only one primary form of authentication (e.g. SSL Auth, Password, or Token) is actively used per client setup.