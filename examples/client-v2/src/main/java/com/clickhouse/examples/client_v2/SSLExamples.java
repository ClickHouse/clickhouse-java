package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.SSLMode;
import com.clickhouse.client.api.query.GenericRecord;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Examples showing how to configure secure (TLS/HTTPS) connections with Client-v2.
 *
 * <p>Currently covered:</p>
 * <ul>
 *     <li>Connecting to a server whose certificate is signed by a custom (private) CA -
 *     the CA certificate is passed with {@link Client.Builder#setRootCertificate(String)}.
 *     No trust store configuration is needed: the certificate is added to a trust store
 *     used only by this client, so the JVM default trust store stays untouched.</li>
 *     <li>Passing the CA certificate as a PEM string instead of a file path - useful when the
 *     certificate comes from an environment variable or a secret manager (typical for
 *     Kubernetes/cloud deployments) and you do not want to write it to disk.</li>
 *     <li>Connecting to a server with a self-signed certificate without any trust material -
 *     {@link SSLMode#TRUST} accepts any server certificate and skips hostname verification.
 *     Use it only for testing or in fully trusted environments.</li>
 * </ul>
 *
 * <p>More SSL examples (mTLS, trust stores, SNI) will be added to this class later.</p>
 *
 * <p>The example runs in one of two modes:</p>
 * <ul>
 *     <li><b>Local mode</b> (default, when {@code chHost} is not set) - starts a local ClickHouse
 *     server in Docker with a freshly generated self-signed certificate and verifies the whole
 *     scenario end to end. Requires a running Docker daemon.</li>
 *     <li><b>Standalone mode</b> (when {@code chHost} is set) - connects to your own server using
 *     the provided CA certificate. Use it to verify your own instance once the local scenario works.</li>
 * </ul>
 *
 * <p>Supported startup properties:</p>
 * <ul>
 *     <li>{@code chHost} - ClickHouse host. When set, standalone mode is used</li>
 *     <li>{@code chPort} - ClickHouse HTTPS port, default {@code 8443}</li>
 *     <li>{@code chDatabase} - database name, default {@code default}</li>
 *     <li>{@code chUser} and {@code chPassword} - credentials (standalone mode)</li>
 *     <li>{@code chRootCert} - path to the root CA certificate in PEM format. When omitted in
 *     standalone mode, only the self-signed (TRUST) example runs
 *     <li>{@code chImage} - Docker image for local mode, default {@code clickhouse/clickhouse-server:latest}</li>
 * </ul>
 */
@Slf4j
public class SSLExamples {

    public static void main(String[] args) {
        final String host = trimToNull(System.getProperty("chHost"));
        final String database = System.getProperty("chDatabase", "default");

        if (host != null) {
            // Standalone mode: verify a user-provided instance.
            final String port = System.getProperty("chPort", "8443");
            final String user = System.getProperty("chUser", "default");
            final String password = System.getProperty("chPassword", "");
            final String rootCert = trimToNull(System.getProperty("chRootCert"));

            log.info("Running in standalone mode against {}:{}", host, port);
            String endpoint = "https://" + host + ":" + port;
            connectToSelfSignedServer(endpoint, database, user, password);
            if (rootCert != null) {
                connectWithCustomRootCertificate(endpoint, database, user, password, rootCert);
                connectWithRootCertificateAsString(endpoint, database, user, password, rootCert);
            } else {
                log.info("chRootCert is not set - skipping the custom CA certificate examples. "
                        + "Pass the path to the CA certificate (PEM) that signed the server certificate to run them.");
            }
            return;
        }

        // Local mode: start a dockerized ClickHouse with a self-signed certificate and
        // verify the whole scenario end to end.
        final String image = System.getProperty("chImage", "clickhouse/clickhouse-server:latest");
        log.info("Running in local mode (set -DchHost to verify your own server)");
        try (SecureServerSupport server = SecureServerSupport.start(image)) {
            connectToSelfSignedServer(server.getEndpoint(), database,
                    SecureServerSupport.USER, SecureServerSupport.PASSWORD);
            connectWithCustomRootCertificate(server.getEndpoint(), database,
                    SecureServerSupport.USER, SecureServerSupport.PASSWORD, server.getCaCertPath());
            connectWithRootCertificateAsString(server.getEndpoint(), database,
                    SecureServerSupport.USER, SecureServerSupport.PASSWORD, server.getCaCertPath());
        } catch (Exception e) {
            log.error("Failed to run the SSL example against a local Docker server", e);
        }
    }

    /**
     * Connects to a ClickHouse server with a self-signed certificate without providing
     * any trust material. {@link SSLMode#TRUST} makes the client accept any server
     * certificate and skip hostname verification.
     *
     * <p><b>Warning:</b> the connection is encrypted, but the server identity is NOT verified,
     * which makes it susceptible to man-in-the-middle attacks. Use this mode only for testing
     * or in fully trusted environments. Prefer {@link Client.Builder#setRootCertificate(String)}
     * with the signing CA certificate whenever possible.</p>
     */
    static void connectToSelfSignedServer(String endpoint, String database, String user, String password) {
        log.info("Connecting to {} accepting any server certificate (SSLMode.TRUST)", endpoint);
        try (Client client = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .setDefaultDatabase(database)
                // Accept the self-signed certificate and skip hostname verification.
                .setSSLMode(SSLMode.TRUST)
                .build()) {

            List<GenericRecord> rows = client.queryAll("SELECT currentUser() AS user, version() AS version");
            log.info("Connected (server certificate not verified) as '{}' to ClickHouse {}",
                    rows.get(0).getString("user"), rows.get(0).getString("version"));
        } catch (Exception e) {
            log.error("Connection with SSLMode.TRUST failed", e);
        }
    }

    /**
     * Connects to a ClickHouse server using a custom root CA certificate.
     * Use this when the server certificate is signed by a private CA (corporate CA,
     * self-managed Kubernetes CA, etc.) that is not present in the JVM default trust store.
     */
    static void connectWithCustomRootCertificate(String endpoint, String database, String user, String password,
                                                 String rootCert) {
        log.info("Connecting to {} using root CA certificate from {}", endpoint, rootCert);
        try (Client client = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .setDefaultDatabase(database)
                // Only the CA certificate is required. The server certificate chain will be
                // validated against it, and hostname verification stays enabled.
                .setRootCertificate(rootCert)
                .build()) {

            List<GenericRecord> rows = client.queryAll("SELECT currentUser() AS user, version() AS version");
            log.info("Connected securely as '{}' to ClickHouse {}", rows.get(0).getString("user"),
                    rows.get(0).getString("version"));
        } catch (Exception e) {
            log.error("Secure connection with a custom root CA certificate failed", e);
        }
    }

    /**
     * Same as {@link #connectWithCustomRootCertificate}, but the CA certificate is passed as PEM
     * content instead of a file path. {@link Client.Builder#setRootCertificate(String)} accepts both:
     * any value containing a {@code -----BEGIN ...-----} block is treated as PEM content.
     *
     * <p>This is handy when the certificate is delivered through an environment variable or
     * a secret manager (e.g. a Kubernetes secret projected into {@code CLICKHOUSE_CA_CERT}),
     * so the application never has to write it to disk:</p>
     *
     * <pre>{@code
     * String caPem = System.getenv("CLICKHOUSE_CA_CERT");
     * Client client = new Client.Builder().setRootCertificate(caPem)...
     * }</pre>
     */
    static void connectWithRootCertificateAsString(String endpoint, String database, String user, String password,
                                                   String rootCertPath) {
        final String rootCertPem;
        try {
            // In a real application the PEM content would typically come from an env variable
            // or a secret manager; here we simply read the file generated for this example.
            rootCertPem = new String(Files.readAllBytes(Paths.get(rootCertPath)), StandardCharsets.US_ASCII);
        } catch (IOException e) {
            log.error("Failed to read the CA certificate from {}", rootCertPath, e);
            return;
        }

        log.info("Connecting to {} using root CA certificate passed as a PEM string", endpoint);
        try (Client client = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .setDefaultDatabase(database)
                // PEM content, not a path - detected by the "-----BEGIN" marker.
                .setRootCertificate(rootCertPem)
                .build()) {

            List<GenericRecord> rows = client.queryAll("SELECT currentUser() AS user, version() AS version");
            log.info("Connected securely (CA cert as string) as '{}' to ClickHouse {}",
                    rows.get(0).getString("user"), rows.get(0).getString("version"));
        } catch (Exception e) {
            log.error("Secure connection with a CA certificate passed as a string failed", e);
        }
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
