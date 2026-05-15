# Secure Connections Guide

This guide explains how to configure the ClickHouse Java client (Client-v2) for secure (TLS/HTTPS) connections. It covers the most common deployment scenarios and describes what to expect from the client in each one.

## How TLS Validation Works

A secure connection does two independent things:

| Check | What it verifies | Why it matters |
|---|---|---|
| **Certificate validation** | The server's certificate is trusted (signed by a known CA or explicitly trusted) | Prevents connecting to a server impersonating ClickHouse |
| **Hostname verification** | The certificate's subject matches the hostname the client is connecting to | Prevents connecting to the right CA's certificate on the wrong server |

Both checks are needed for a fully secure connection. Skipping either one leaves a gap even if the other check passes.

> **Note:** When troubleshooting a TLS failure, the first step is to determine which of the two checks is failing. The error message from the client will usually indicate whether the problem is with the certificate chain or the hostname.

---

## Use Cases

### 1. Public CA

Common for managed cloud services and public-facing deployments. The server certificate is issued by a well-known authority such as Let's Encrypt, DigiCert, or Amazon, so no custom certificates need to be distributed — the JVM truststore already trusts these issuers.

| Chain validation | Hostname verification | Direct certificate trust |
|---|---|---|
| ✓ Against JVM truststore | ✓ Enabled | — |

```java
Client client = new Client.Builder()
        .addEndpoint("https://clickhouse.example.com:8443")
        .setUsername("default")
        .setPassword("secret")
        .build();
```

---

### 2. Private CA with Hostname Verification

Mainly used in regulated environments and zero-trust networks where the organization runs its own certificate authority and strict server identity verification is required. Provide the CA certificate so the client can validate the chain, and the hostname in the connection URL must match the certificate exactly.

| Chain validation | Hostname verification | Direct certificate trust |
|---|---|---|
| ✓ Against provided CA | ✓ Enabled | — |

#### Option A — CA certificate as a PEM file

```java
Client client = new Client.Builder()
        .addEndpoint("https://clickhouse.internal:8443")
        .setUsername("default")
        .setPassword("secret")
        .setRootCertificate("/path/to/company-ca.crt")
        .build();
```

#### Option B — CA certificate via a Java truststore

```java
Client client = new Client.Builder()
        .addEndpoint("https://clickhouse.internal:8443")
        .setUsername("default")
        .setPassword("secret")
        .setSSLTrustStore("/path/to/truststore.jks")
        .setSSLTrustStorePassword("changeit")
        .setSSLTrustStoreType("JKS")
        .build();
```

#### Connecting via IP, proxy, or load balancer

When the connection target does not match the certificate subject (e.g. an IP address or proxy hostname), use `sslSocketSNI` to tell the client which hostname to expect on the certificate.

```java
Client client = new Client.Builder()
        .addEndpoint("https://10.0.0.1:8443")
        .setUsername("default")
        .setPassword("secret")
        .setRootCertificate("/path/to/company-ca.crt")
        .sslSocketSNI("clickhouse.internal")
        .build();
```

---

### 3. Private CA without Hostname Verification

Common in corporate and on-premise deployments where the hostname used to reach the server does not match the certificate subject or SANs. The certificate chain is still validated against the private CA, but the hostname check is skipped.

| Chain validation | Hostname verification | Direct certificate trust |
|---|---|---|
| ✓ Against provided CA | ✗ Disabled | — |

#### Why hostname verification fails in this scenario

A server certificate is issued for a specific hostname (e.g. `clickhouse.internal`). When you connect to the server, the client checks that the hostname in the URL matches the certificate. This check fails whenever the connection goes through an intermediary that has its own address:

- **IP address** — the certificate is issued for a hostname, not an IP, so connecting via `https://10.0.0.5:8443` fails even if the cert is otherwise valid.
- **Load balancer or reverse proxy** — the connection URL targets the load balancer hostname, which is different from the hostname on the ClickHouse certificate.
- **Cloud private endpoints** — AWS PrivateLink, GCP Private Service Connect, and Azure Private Link each expose a private DNS name within your network (e.g. `xxxxxxxxxx.us-west-2.vpce.aws.clickhouse.cloud`). This address routes traffic privately to ClickHouse, but the ClickHouse server certificate is issued for the service's own hostname, not the private endpoint address. See the ClickHouse Cloud documentation for details: [AWS PrivateLink](https://clickhouse.com/docs/en/manage/security/aws-privatelink), [GCP Private Service Connect](https://clickhouse.com/docs/manage/security/gcp-private-service-connect), [Azure Private Link](https://clickhouse.com/docs/en/cloud/security/azure-privatelink).

#### What sslSocketSNI does

SNI (Server Name Indication) is a standard TLS extension that the client sends during the handshake to tell the server which hostname it intends to reach. Servers that host multiple services behind a single IP use SNI to select the correct certificate to present.

Calling `.sslSocketSNI("expected-hostname")` on the client builder does two things:

1. **Sets the SNI value** in the TLS handshake to the provided hostname, so the server presents the certificate issued for that name rather than a default fallback.
2. **Disables hostname verification** in the client, so the check between the URL hostname and the certificate subject is skipped.

The result is that the certificate chain is still validated (the server must present a cert signed by your CA), but the mismatch between the connection URL and the certificate subject no longer causes a failure.

#### Option A — CA certificate as a PEM file

```java
// Connecting via a private endpoint or IP; the cert is issued for "clickhouse.internal"
Client client = new Client.Builder()
        .addEndpoint("https://10.0.0.5:8443")
        .setUsername("default")
        .setPassword("secret")
        .setRootCertificate("/path/to/company-ca.crt")
        .sslSocketSNI("clickhouse.internal")
        .build();
```

#### Option B — CA certificate via a Java truststore

```java
// Connecting via a private endpoint or IP; the cert is issued for "clickhouse.internal"
Client client = new Client.Builder()
        .addEndpoint("https://10.0.0.5:8443")
        .setUsername("default")
        .setPassword("secret")
        .setSSLTrustStore("/path/to/truststore.jks")
        .setSSLTrustStorePassword("changeit")
        .setSSLTrustStoreType("JKS")
        .sslSocketSNI("clickhouse.internal")
        .build();
```

---

### 4. Self-Signed Certificate

Common in local development, Docker Compose setups, and small internal instances. The server certificate is self-generated and not signed by any CA. Instead of trusting an issuer, you provide the server's own certificate directly so the client can recognize it.

#### With hostname verification enabled

Common when the hostname in the connection URL matches the certificate's subject or Subject Alternative Names (SANs).

| Chain validation | Hostname verification | Direct certificate trust |
|---|---|---|
| — | ✓ Enabled | ✓ Exact certificate match |

```java
Client client = new Client.Builder()
        .addEndpoint("https://clickhouse.local:8443")
        .setUsername("default")
        .setPassword("secret")
        .setRootCertificate("/path/to/server.crt")
        .build();
```

#### With hostname verification disabled

Common when the certificate was generated for `localhost` but you connect via an IP or container name. Providing the expected certificate name via `sslSocketSNI` disables automatic hostname matching.

| Chain validation | Hostname verification | Direct certificate trust |
|---|---|---|
| — | ✗ Disabled | ✓ Exact certificate match |

```java
Client client = new Client.Builder()
        .addEndpoint("https://localhost:8443")
        .setUsername("default")
        .setPassword("secret")
        .setRootCertificate("/path/to/server.crt")
        .sslSocketSNI("localhost")
        .build();
```

---

## Configuration API Summary

| Goal | Builder Method |
|---|---|
| Enable TLS | `.addEndpoint("https://host:port")` |
| Trust a CA certificate (PEM) | `.setRootCertificate("/path/to/ca.crt")` |
| Trust a Java truststore | `.setSSLTrustStore("/path/to/truststore.jks")` |
| Truststore password | `.setSSLTrustStorePassword("password")` |
| Truststore type | `.setSSLTrustStoreType("JKS")` |
| SNI override / disable hostname verification | `.sslSocketSNI("expected-hostname")` |
| mTLS client certificate | `.setClientCertificate("/path/to/client.crt")` |
| mTLS client key | `.setClientKey("/path/to/client.key")` |
| Enable SSL certificate authentication | `.useSSLAuthentication(true)` |

---

## Development Environments

It is strongly recommended to keep development setups as close to production as possible:

- Use TLS-enabled endpoints even in development.
- Use certificates signed by a development CA or another explicitly trusted issuer.
- Keep hostname verification enabled whenever the environment supports stable hostnames.

This approach catches integration issues earlier and reduces deployment-time surprises.

If you need a more relaxed setup for quick local experimentation, treat it as an exception rather than the default. CI pipelines and integration tests should still validate certificates and hosts to match real-world usage.

---

## Troubleshooting

| Symptom | Likely cause | What to check |
|---|---|---|
| `PKIX path building failed` | CA not trusted by the JVM | Add the CA cert via `.setRootCertificate()` or `.setSSLTrustStore()` |
| `unable to find valid certification path` | CA chain is incomplete | Check whether intermediate CA certificates are needed and include them |
| `Certificate expired` | Server cert has passed its expiry date | Renew the server certificate |
| `No subject alternative names` | Certificate has no SANs for the requested hostname | Use a certificate that includes the correct hostname as a SAN |
| Hostname mismatch | URL hostname does not match the certificate subject or SANs | Verify the hostname; use `.sslSocketSNI()` when connecting via IP or proxy |
| Hostname mismatch despite a valid cert | Connection goes through a proxy or load balancer | Use `.sslSocketSNI("expected-hostname")` to set the certificate's hostname |
| Connection times out | TLS not enabled on the server | Confirm the server is listening on the HTTPS port |
| Works in browser, fails in client | JVM truststore does not include the CA | Add the CA via `.setRootCertificate()` or `.setSSLTrustStore()` |
| Works locally, fails in production | TLS validation was skipped in dev | Review the builder configuration and ensure cert and hostname checks are in place |
