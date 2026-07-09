package com.clickhouse.examples.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Examples showing how to configure secure (TLS/HTTPS) connections with the JDBC driver.
 *
 * <p>Currently covered:</p>
 * <ul>
 *     <li>Connecting to a server whose certificate is signed by a custom (private) CA -
 *     the CA certificate is passed with the {@code sslrootcert} connection property.
 *     No trust store configuration is needed: the certificate is added to a trust store
 *     used only by this connection, so the JVM default trust store stays untouched.</li>
 *     <li>Passing the CA certificate as a PEM string instead of a file path - useful when the
 *     certificate comes from an environment variable or a secret manager (typical for
 *     Kubernetes/cloud deployments) and you do not want to write it to disk.</li>
 *     <li>Connecting to a server with a self-signed certificate without any trust material -
 *     the {@code ssl_mode=trust} connection property accepts any server certificate and skips
 *     hostname verification ({@code ssl_mode=none} is accepted as an alias). Use it only for
 *     testing or in fully trusted environments.</li>
 *     <li>Restricting the negotiated TLS cipher suites with the {@code ssl_cipher_suites} connection
 *     property (a comma-separated list) - useful to enforce a stronger or compliance-mandated set of
 *     cipher suites instead of the JVM defaults.</li>
 * </ul>
 *
 * <p>More SSL examples (mTLS, trust stores, SNI) will be added to this class later.</p>
 *
 * <p>The example runs in one of two modes:</p>
 * <ul>
 *     <li><b>Local mode</b> (default, when {@code chUrl} is not set) - starts a local ClickHouse
 *     server in Docker with a freshly generated self-signed certificate and verifies the whole
 *     scenario end to end. Requires a running Docker daemon.</li>
 *     <li><b>Standalone mode</b> (when {@code chUrl} is set) - connects to your own server using
 *     the provided CA certificate. Use it to verify your own instance once the local scenario works.</li>
 * </ul>
 *
 * <p>Supported startup properties:</p>
 * <ul>
 *     <li>{@code chUrl} - ClickHouse JDBC URL, e.g. {@code jdbc:clickhouse://my-host:8443/default}.
 *     When set, standalone mode is used</li>
 *     <li>{@code chUser} and {@code chPassword} - credentials (standalone mode)</li>
 *     <li>{@code chRootCert} - path to the root CA certificate in PEM format. When omitted in
 *     standalone mode, only the self-signed (trust) example runs</li>
 *     <li>{@code chImage} - Docker image for local mode, default {@code clickhouse/clickhouse-server:latest}</li>
 * </ul>
 */
public class SSLExamples {
    private static final Logger log = LoggerFactory.getLogger(SSLExamples.class);

    public static void main(String[] args) {
        final String url = trimToNull(System.getProperty("chUrl"));

        if (url != null) {
            // Standalone mode: verify a user-provided instance.
            final String user = System.getProperty("chUser", "default");
            final String password = System.getProperty("chPassword", "");
            final String rootCert = trimToNull(System.getProperty("chRootCert"));

            log.info("Running in standalone mode against {}", url);
            try {
                connectToSelfSignedServer(url, user, password);
                if (rootCert != null) {
                    connectWithCustomRootCertificate(url, user, password, rootCert);
                    connectWithRootCertificateAsString(url, user, password, rootCert);
                    connectWithCipherSuites(url, user, password, rootCert);
                } else {
                    log.info("chRootCert is not set - skipping the custom CA certificate examples. "
                            + "Pass the path to the CA certificate (PEM) that signed the server certificate to run them.");
                }
            } catch (SQLException | IOException e) {
                log.error("Secure connection failed", e);
            }
            return;
        }

        // Local mode: start a dockerized ClickHouse with a self-signed certificate and
        // verify the whole scenario end to end.
        final String image = System.getProperty("chImage", "clickhouse/clickhouse-server:latest");
        log.info("Running in local mode (set -DchUrl to verify your own server)");
        try (SecureServerSupport server = SecureServerSupport.start(image)) {
            connectToSelfSignedServer(server.getJdbcUrl(),
                    SecureServerSupport.USER, SecureServerSupport.PASSWORD);
            connectWithCustomRootCertificate(server.getJdbcUrl(),
                    SecureServerSupport.USER, SecureServerSupport.PASSWORD, server.getCaCertPath());
            connectWithRootCertificateAsString(server.getJdbcUrl(),
                    SecureServerSupport.USER, SecureServerSupport.PASSWORD, server.getCaCertPath());
            connectWithCipherSuites(server.getJdbcUrl(),
                    SecureServerSupport.USER, SecureServerSupport.PASSWORD, server.getCaCertPath());
        } catch (Exception e) {
            log.error("Failed to run the SSL example against a local Docker server", e);
            Runtime.getRuntime().exit(-1);
        }
        // Explicit exit: testcontainers keeps non-daemon threads alive after the scenario is done.
        Runtime.getRuntime().exit(0);
    }

    /**
     * Connects to a ClickHouse server with a self-signed certificate without providing
     * any trust material. The {@code ssl_mode=trust} connection property makes the driver
     * accept any server certificate and skip hostname verification. The traditional JDBC
     * value {@code ssl_mode=none} is accepted as an alias.
     *
     * <p><b>Warning:</b> the connection is encrypted, but the server identity is NOT verified,
     * which makes it susceptible to man-in-the-middle attacks. Use this mode only for testing
     * or in fully trusted environments. Prefer {@code sslrootcert} with the signing CA
     * certificate whenever possible.</p>
     */
    static void connectToSelfSignedServer(String url, String user, String password) throws SQLException {
        log.info("Connecting to {} accepting any server certificate (ssl_mode=trust)", url);

        Properties properties = new Properties();
        properties.setProperty(ClientConfigProperties.USER.getKey(), user); // user
        properties.setProperty(ClientConfigProperties.PASSWORD.getKey(), password); // password
        properties.setProperty("ssl", "true"); // enable TLS even if the URL has no https scheme
        // Accept the self-signed certificate and skip hostname verification.
        properties.setProperty(ClientConfigProperties.SSL_MODE.getKey(), "trust"); // ssl_mode

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT currentUser() AS user, version() AS version")) {
            if (rs.next()) {
                log.info("Connected (server certificate not verified) as '{}' to ClickHouse {}",
                        rs.getString("user"), rs.getString("version"));
            }
        }
    }

    /**
     * Connects to a ClickHouse server using a custom root CA certificate.
     * Use this when the server certificate is signed by a private CA (corporate CA,
     * self-managed Kubernetes CA, etc.) that is not present in the JVM default trust store.
     */
    static void connectWithCustomRootCertificate(String url, String user, String password, String rootCert)
            throws SQLException {
        log.info("Connecting to {} using root CA certificate from {}", url, rootCert);

        Properties properties = new Properties();
        properties.setProperty(ClientConfigProperties.USER.getKey(), user); // user
        properties.setProperty(ClientConfigProperties.PASSWORD.getKey(), password); // password
        properties.setProperty("ssl", "true"); // enable TLS even if the URL has no https scheme
        // Only the CA certificate is required. The server certificate chain will be
        // validated against it, and hostname verification stays enabled.
        properties.setProperty(ClientConfigProperties.CA_CERTIFICATE.getKey(), rootCert); // sslrootcert

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT currentUser() AS user, version() AS version")) {
            if (rs.next()) {
                log.info("Connected securely as '{}' to ClickHouse {}", rs.getString("user"), rs.getString("version"));
            }
        }
    }

    /**
     * Same as {@link #connectWithCustomRootCertificate}, but the CA certificate is passed as PEM
     * content instead of a file path. The {@code sslrootcert} property accepts both: any value
     * containing a {@code -----BEGIN ...-----} block is treated as PEM content.
     *
     * <p>This is handy when the certificate is delivered through an environment variable or
     * a secret manager (e.g. a Kubernetes secret projected into {@code CLICKHOUSE_CA_CERT}),
     * so the application never has to write it to disk:</p>
     *
     * <pre>{@code
     * properties.setProperty("sslrootcert", System.getenv("CLICKHOUSE_CA_CERT"));
     * }</pre>
     */
    static void connectWithRootCertificateAsString(String url, String user, String password, String rootCertPath)
            throws SQLException, IOException {
        // In a real application the PEM content would typically come from an env variable
        // or a secret manager; here we simply read the file generated for this example.
        String rootCertPem = new String(Files.readAllBytes(Paths.get(rootCertPath)), StandardCharsets.US_ASCII);

        log.info("Connecting to {} using root CA certificate passed as a PEM string", url);

        Properties properties = new Properties();
        properties.setProperty(ClientConfigProperties.USER.getKey(), user); // user
        properties.setProperty(ClientConfigProperties.PASSWORD.getKey(), password); // password
        properties.setProperty("ssl", "true"); // enable TLS even if the URL has no https scheme
        // PEM content, not a path - detected by the "-----BEGIN" marker.
        properties.setProperty(ClientConfigProperties.CA_CERTIFICATE.getKey(), rootCertPem); // sslrootcert

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT currentUser() AS user, version() AS version")) {
            if (rs.next()) {
                log.info("Connected securely (CA cert as string) as '{}' to ClickHouse {}",
                        rs.getString("user"), rs.getString("version"));
            }
        }
    }

    /**
     * Connects while restricting the TLS cipher suites the driver is allowed to negotiate, using the
     * {@code ssl_cipher_suites} connection property (a comma-separated list). Only the listed suites are
     * enabled on the socket (subject to what the JVM and the server support); this is useful to enforce a
     * stronger or compliance-mandated set of cipher suites rather than relying on the JVM defaults.
     *
     * <p>The CA certificate is still used to verify the server and hostname verification stays enabled -
     * cipher-suite selection is independent of the trust configuration and {@code ssl_mode}. Keep at least
     * one suite the server actually supports, or the handshake fails.</p>
     */
    static void connectWithCipherSuites(String url, String user, String password, String rootCert)
            throws SQLException {
        log.info("Connecting to {} with a restricted set of TLS cipher suites", url);

        Properties properties = new Properties();
        properties.setProperty(ClientConfigProperties.USER.getKey(), user); // user
        properties.setProperty(ClientConfigProperties.PASSWORD.getKey(), password); // password
        properties.setProperty("ssl", "true"); // enable TLS even if the URL has no https scheme
        properties.setProperty(ClientConfigProperties.CA_CERTIFICATE.getKey(), rootCert); // sslrootcert
        // Restrict negotiation to these cipher suites (TLS 1.3 and TLS 1.2), comma-separated.
        properties.setProperty(ClientConfigProperties.SSL_CIPHER_SUITES.getKey(),
                "TLS_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"); // ssl_cipher_suites

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT currentUser() AS user, version() AS version")) {
            if (rs.next()) {
                log.info("Connected securely (restricted cipher suites) as '{}' to ClickHouse {}",
                        rs.getString("user"), rs.getString("version"));
            }
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
