package com.clickhouse.examples.client_v2;

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Date;
import java.util.stream.Stream;

/**
 * Support class for SSL examples: starts a local ClickHouse server in Docker configured with a
 * freshly generated self-signed certificate (a private CA signs the server certificate).
 *
 * <p>All TLS material is generated at runtime and removed when the server is closed, so the
 * examples are fully self-contained. The same setup can be reproduced manually with
 * {@code openssl} - see the project README. The server-side TLS configuration is described in the
 * official documentation: <a href="https://clickhouse.com/docs/en/guides/sre/configuring-ssl">Configuring SSL-TLS</a>.</p>
 */
@Slf4j
public class SecureServerSupport implements AutoCloseable {

    /** Credentials of the user created in the local container. */
    public static final String USER = "ssl_demo";
    public static final String PASSWORD = "ssl_demo_password";

    private static final int HTTP_PORT = 8123;
    private static final int HTTPS_PORT = 8443;
    private static final long CERTIFICATE_DAYS_VALID = 365;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String BC_PROVIDER = BouncyCastleProvider.PROVIDER_NAME;

    static {
        if (Security.getProvider(BC_PROVIDER) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    private final GenericContainer<?> container;
    private final Path certDir;
    private final Path confDir;

    private SecureServerSupport(GenericContainer<?> container, Path certDir, Path confDir) {
        this.container = container;
        this.certDir = certDir;
        this.confDir = confDir;
    }

    /**
     * Generates a private CA and a server certificate, writes the ClickHouse SSL configuration and
     * starts a ClickHouse container with HTTPS enabled.
     */
    public static SecureServerSupport start(String image) throws Exception {
        Path certDir = Files.createTempDirectory("ch-ssl-example-certs-");
        Path confDir = Files.createTempDirectory("ch-ssl-example-config-");
        Path sslConfig = confDir.resolve("custom_ca_ssl.xml");

        log.info("Generating an ephemeral private CA and a server certificate in {}", certDir);
        generatePrivateCaAndServerCertificate(certDir);
        writeClickHouseSslConfig(sslConfig);
        // The TLS material must be readable by the 'clickhouse' user inside the container,
        // while temp directories are created accessible to the current user only.
        makeReadableByContainer(certDir);
        makeReadableByContainer(confDir);

        log.info("Starting ClickHouse container from image: {}", image);
        GenericContainer<?> container = new GenericContainer<>(image)
                .withExposedPorts(HTTP_PORT, HTTPS_PORT)
                .withEnv("CLICKHOUSE_USER", USER)
                .withEnv("CLICKHOUSE_PASSWORD", PASSWORD)
                .withFileSystemBind(certDir.toAbsolutePath().toString(),
                        "/etc/clickhouse-server/certs", BindMode.READ_ONLY)
                .withFileSystemBind(sslConfig.toAbsolutePath().toString(),
                        "/etc/clickhouse-server/config.d/custom_ca_ssl.xml", BindMode.READ_ONLY)
                .waitingFor(Wait.forHttp("/ping")
                        .forPort(HTTP_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        try {
            container.start();
        } catch (Exception e) {
            log.error("ClickHouse container failed to start. Container logs:\n{}", safeGetLogs(container));
            deleteRecursively(certDir);
            deleteRecursively(confDir);
            throw e;
        }
        log.info("ClickHouse container is ready on https://localhost:{}", container.getMappedPort(HTTPS_PORT));
        return new SecureServerSupport(container, certDir, confDir);
    }

    /** HTTPS endpoint of the started container. */
    public String getEndpoint() {
        return "https://localhost:" + container.getMappedPort(HTTPS_PORT);
    }

    /** Path to the CA certificate (PEM) that signed the server certificate. */
    public String getCaCertPath() {
        return certDir.resolve("ca.crt").toAbsolutePath().toString();
    }

    @Override
    public void close() {
        log.info("Stopping ClickHouse container and deleting temporary TLS artifacts");
        container.stop();
        deleteRecursively(certDir);
        deleteRecursively(confDir);
    }

    private static void generatePrivateCaAndServerCertificate(Path outputDir) throws Exception {
        KeyPair caKeys = generateRsaKeyPair();
        X500Name caSubject = new X500Name("CN=ExamplePrivateCA");
        X509Certificate caCertificate = generateCertificate(caSubject, caSubject,
                caKeys.getPublic(), caKeys.getPrivate(), caKeys.getPublic(), true, null);

        KeyPair serverKeys = generateRsaKeyPair();
        X500Name serverSubject = new X500Name("CN=localhost");
        GeneralNames serverSans = new GeneralNames(new GeneralName[]{
                new GeneralName(GeneralName.dNSName, "localhost"),
                new GeneralName(GeneralName.iPAddress, "127.0.0.1")
        });
        X509Certificate serverCertificate = generateCertificate(serverSubject, caSubject,
                serverKeys.getPublic(), caKeys.getPrivate(), caKeys.getPublic(), false, serverSans);

        writePemObject(outputDir.resolve("ca.crt"), caCertificate);
        writePemObject(outputDir.resolve("server.crt"), serverCertificate);
        writePemObject(outputDir.resolve("server.key"), serverKeys.getPrivate());
    }

    private static KeyPair generateRsaKeyPair() throws Exception {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048, SECURE_RANDOM);
        return keyPairGenerator.generateKeyPair();
    }

    private static X509Certificate generateCertificate(
            X500Name subject,
            X500Name issuer,
            PublicKey subjectPublicKey,
            PrivateKey issuerPrivateKey,
            PublicKey issuerPublicKey,
            boolean isCa,
            GeneralNames subjectAlternativeNames) throws Exception {
        Date notBefore = new Date(System.currentTimeMillis() - 60_000L);
        Date notAfter = new Date(System.currentTimeMillis() + Duration.ofDays(CERTIFICATE_DAYS_VALID).toMillis());
        BigInteger serial = new BigInteger(160, SECURE_RANDOM).abs().add(BigInteger.ONE);

        X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                issuer, serial, notBefore, notAfter, subject, subjectPublicKey);
        certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCa));
        certBuilder.addExtension(Extension.keyUsage, true, new KeyUsage(isCa
                ? KeyUsage.keyCertSign | KeyUsage.cRLSign
                : KeyUsage.digitalSignature | KeyUsage.keyEncipherment));

        JcaX509ExtensionUtils extensionUtils = new JcaX509ExtensionUtils();
        certBuilder.addExtension(Extension.subjectKeyIdentifier, false,
                extensionUtils.createSubjectKeyIdentifier(subjectPublicKey));
        certBuilder.addExtension(Extension.authorityKeyIdentifier, false,
                extensionUtils.createAuthorityKeyIdentifier(issuerPublicKey));
        if (subjectAlternativeNames != null) {
            certBuilder.addExtension(Extension.subjectAlternativeName, false, subjectAlternativeNames);
        }

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA")
                .setProvider(BC_PROVIDER)
                .build(issuerPrivateKey);
        X509Certificate certificate = new JcaX509CertificateConverter()
                .setProvider(BC_PROVIDER)
                .getCertificate(certBuilder.build(signer));
        certificate.checkValidity(new Date());
        certificate.verify(issuerPublicKey);
        return certificate;
    }

    private static void writePemObject(Path targetPath, Object value) throws IOException {
        try (Writer fileWriter = Files.newBufferedWriter(targetPath, StandardCharsets.US_ASCII);
             JcaPEMWriter pemWriter = new JcaPEMWriter(fileWriter)) {
            pemWriter.writeObject(value);
        }
    }

    /**
     * Writes a config.d overlay enabling the HTTPS interface. The full description of the
     * server-side options is in the official documentation:
     * https://clickhouse.com/docs/en/guides/sre/configuring-ssl
     */
    private static void writeClickHouseSslConfig(Path configPath) throws IOException {
        String config = "<clickhouse>\n"
                + "    <https_port>" + HTTPS_PORT + "</https_port>\n"
                + "    <openSSL>\n"
                + "        <server>\n"
                + "            <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>\n"
                + "            <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>\n"
                + "            <verificationMode>none</verificationMode>\n"
                + "            <loadDefaultCAFile>true</loadDefaultCAFile>\n"
                + "            <disableProtocols>sslv2,sslv3</disableProtocols>\n"
                + "            <preferServerCiphers>true</preferServerCiphers>\n"
                + "        </server>\n"
                + "    </openSSL>\n"
                + "</clickhouse>\n";
        Files.write(configPath, config.getBytes(StandardCharsets.UTF_8));
    }

    private static void makeReadableByContainer(Path dir) throws IOException {
        try (Stream<Path> targets = Files.walk(dir)) {
            for (Path target : targets.toArray(Path[]::new)) {
                File file = target.toFile();
                if (!file.setReadable(true, false)) {
                    log.warn("Failed to make {} world-readable", target);
                }
                if (Files.isDirectory(target) && !file.setExecutable(true, false)) {
                    log.warn("Failed to make {} world-executable", target);
                }
            }
        }
    }

    private static String safeGetLogs(GenericContainer<?> container) {
        try {
            return container.getLogs();
        } catch (Exception e) {
            return "<not available: " + e.getMessage() + ">";
        }
    }

    private static void deleteRecursively(Path path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (Stream<Path> targets = Files.walk(path)) {
            targets.sorted((left, right) -> right.getNameCount() - left.getNameCount())
                    .forEach(target -> {
                        try {
                            Files.deleteIfExists(target);
                        } catch (IOException ignored) {
                            // Best effort cleanup for temporary example files.
                        }
                    });
        } catch (IOException ignored) {
            // Best effort cleanup for temporary example files.
        }
    }
}
