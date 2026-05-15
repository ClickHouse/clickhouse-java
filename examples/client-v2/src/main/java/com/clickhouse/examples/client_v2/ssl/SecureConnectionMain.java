package com.clickhouse.examples.client_v2.ssl;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.Records;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;

@Command(
        name = "secure-connection",
        mixinStandardHelpOptions = true,
        description = "Private CA + hostname verification SSL example for Client-v2."
)
public class SecureConnectionMain implements Callable<Integer> {
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

    @Option(names = "--host",
            description = "ClickHouse host. If set, external server mode is used.")
    private String host;

    @Option(names = "--port",
            defaultValue = "8443",
            description = "ClickHouse HTTPS port. Default: ${DEFAULT-VALUE}.")
    private int port;

    @Option(names = "--user",
            defaultValue = "default",
            description = "ClickHouse username. Default: ${DEFAULT-VALUE}.")
    private String user;

    @Option(names = "--password",
            defaultValue = "",
            description = "ClickHouse password. Default: empty.")
    private String password;

    @Option(names = "--root-ca",
            description = "Path to root CA certificate PEM. Required for external mode; optional for local mode.")
    private Path rootCa;

    @Option(names = "--root-ca-key",
            description = "Path to root CA private key PEM. Used in local mode when --root-ca is provided.")
    private Path rootCaKey;

    @Option(names = "--clickhouse-image",
            defaultValue = "clickhouse/clickhouse-server:latest",
            description = "Docker image for local ClickHouse container. Default: ${DEFAULT-VALUE}.")
    private String clickHouseImage;

    @Option(names = "--database",
            defaultValue = "default",
            description = "Database used for write/read check. Default: ${DEFAULT-VALUE}.")
    private String database;

    @Override
    public Integer call() throws Exception {
        if (host != null && rootCa == null) {
            throw new CommandLine.ParameterException(
                    new CommandLine(this),
                    "--root-ca is required when --host is set.");
        }

        if (rootCa != null && !Files.exists(rootCa)) {
            throw new CommandLine.ParameterException(
                    new CommandLine(this),
                    "Root CA file does not exist: " + rootCa);
        }
        if (rootCaKey != null && !Files.exists(rootCaKey)) {
            throw new CommandLine.ParameterException(
                    new CommandLine(this),
                    "Root CA key file does not exist: " + rootCaKey);
        }
        if (host != null) {
            printInfo("Running in external-server mode");
            runScenario(
                    host,
                    port,
                    user,
                    password,
                    rootCa,
                    "external");
            return CommandLine.ExitCode.OK;
        }

        printInfo("Running in local-container mode");
        runLocalContainerScenario();
        return CommandLine.ExitCode.OK;
    }

    private void runLocalContainerScenario() throws Exception {
        Path certDir = Files.createTempDirectory("ch-private-ca-example-certs-");
        Path confDir = Files.createTempDirectory("ch-private-ca-example-config-");
        Path sslConfig = confDir.resolve("zzz_ssl.xml");
        Path caCert = certDir.resolve("ca.crt");
        Path serverCert = certDir.resolve("server.crt");
        Path serverKey = certDir.resolve("server.key");

        // Build server TLS material on the fly so this example is fully self-contained.
        if (rootCa != null) {
            printInfo("Using provided root CA to sign local server certificate: " + rootCa.toAbsolutePath());
            generateServerCertificateFromProvidedCa(certDir, rootCa, rootCaKey);
        } else {
            printInfo("Generating ephemeral root CA and local server certificate");
            generatePrivateCaAndServerCertificate(certDir);
        }
        writeClickHouseSslConfig(sslConfig, serverCert, serverKey);
        printInfo("Generated TLS files at " + certDir.toAbsolutePath());

        printInfo("Starting ClickHouse container from image: " + clickHouseImage);
        GenericContainer<?> container = new GenericContainer<>(clickHouseImage)
                .withExposedPorts(HTTP_PORT, HTTPS_PORT)
                .withEnv("CLICKHOUSE_USER", "ssl_demo")
                .withEnv("CLICKHOUSE_PASSWORD", "ssl_demo_password")
                .withFileSystemBind(certDir.toAbsolutePath().toString(),
                        "/etc/clickhouse-server/certs",
                        BindMode.READ_ONLY)
                .withFileSystemBind(sslConfig.toAbsolutePath().toString(),
                        "/etc/clickhouse-server/config.d/zzz_ssl.xml",
                        BindMode.READ_ONLY)
                .waitingFor(Wait.forHttp("/ping")
                        .forPort(HTTP_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));

        try {
            container.start();
            int mappedHttpsPort = container.getMappedPort(HTTPS_PORT);
            printInfo("ClickHouse container is ready on https://localhost:" + mappedHttpsPort);
            runScenario("localhost", mappedHttpsPort, "ssl_demo", "ssl_demo_password", caCert, "container");
        } finally {
            printInfo("Stopping ClickHouse container and deleting temporary TLS artifacts");
            container.stop();
            deleteIfExists(certDir);
            deleteIfExists(confDir);
        }
    }

    private void runScenario(
            String host,
            int httpsPort,
            String user,
            String password,
            Path rootCaCert,
            String mode) throws Exception {
        String endpoint = "https://" + host + ":" + httpsPort;
        String tableName = "ssl_private_ca_demo_" + UUID.randomUUID().toString().replace("-", "");
        printInfo("Preparing secure client for " + endpoint + " (mode=" + mode + ")");
        printInfo("Using root CA certificate: " + rootCaCert.toAbsolutePath());

        try (Client client = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .setDefaultDatabase(database)
                .setRootCertificate(rootCaCert.toAbsolutePath().toString())
                .setOption("sslmode", "strict")
                .build()) {
            boolean tableCreated = false;
            // Ping verifies both TLS handshake and basic connectivity.
            printInfo("Pinging ClickHouse endpoint");
            if (!client.ping()) {
                throw new IllegalStateException("Unable to ping ClickHouse over HTTPS at " + endpoint);
            }
            printInfo("Ping succeeded");

            printInfo("Creating demo table: " + tableName);
            client.query("CREATE TABLE " + tableName
                    + " (id UInt32, message String) ENGINE=MergeTree ORDER BY id")
                    .get(10, TimeUnit.SECONDS);
            tableCreated = true;

            printInfo("Inserting a demo row");
            client.query("INSERT INTO " + tableName + " VALUES (1, 'private-ca-host-verification-ok')")
                    .get(10, TimeUnit.SECONDS);

            String loadedMessage = null;
            printInfo("Reading inserted row back");
            try (Records records = client.queryRecords("SELECT message FROM " + tableName + " WHERE id = 1")
                    .get(10, TimeUnit.SECONDS)) {
                for (GenericRecord record : records) {
                    loadedMessage = record.getString("message");
                }
            }

            if (!"private-ca-host-verification-ok".equals(loadedMessage)) {
                throw new IllegalStateException("Unexpected query result: " + loadedMessage);
            }

            System.out.printf(
                    "Mode=%s; endpoint=%s; TLS chain and host verification succeeded; read value='%s'%n",
                    mode,
                    endpoint,
                    loadedMessage);
            if (tableCreated) {
                printInfo("Dropping demo table: " + tableName);
                client.query("DROP TABLE IF EXISTS " + tableName).get(10, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Secure private-CA scenario failed", e);
        }
    }

    private static void generatePrivateCaAndServerCertificate(Path outputDir)
            throws Exception {
        printInfo("Generating a temporary RSA key pair for root CA");
        KeyPair caKeys = generateRsaKeyPair();
        X500Name caSubject = new X500Name("CN=ExamplePrivateCA");
        X509Certificate caCertificate = generateCertificate(
                caSubject,
                caSubject,
                caKeys.getPublic(),
                caKeys.getPrivate(),
                caKeys.getPublic(),
                true,
                null);

        printInfo("Generating a temporary RSA key pair for server certificate");
        KeyPair serverKeys = generateRsaKeyPair();
        X500Name serverSubject = new X500Name("CN=localhost");
        GeneralNames serverSans = new GeneralNames(new GeneralName[] {
                new GeneralName(GeneralName.dNSName, "localhost"),
                new GeneralName(GeneralName.iPAddress, "127.0.0.1")
        });
        X509Certificate serverCertificate = generateCertificate(
                serverSubject,
                caSubject,
                serverKeys.getPublic(),
                caKeys.getPrivate(),
                caKeys.getPublic(),
                false,
                serverSans);

        writePemObject(outputDir.resolve("ca.crt"), caCertificate);
        writePemObject(outputDir.resolve("server.crt"), serverCertificate);
        writePemObject(outputDir.resolve("server.key"), serverKeys.getPrivate());
    }

    private static void generateServerCertificateFromProvidedCa(Path outputDir, Path caCertPath, Path caKeyPath)
            throws Exception {
        printInfo("Loading provided CA certificate from " + caCertPath.toAbsolutePath());
        X509Certificate caCertificate = readCertificateFromPem(caCertPath);
        PrivateKey caPrivateKey = (caKeyPath == null)
                ? readPrivateKeyFromPem(caCertPath)
                : readPrivateKeyFromPem(caKeyPath);

        if (caPrivateKey == null) {
            throw new IllegalStateException(
                    "Unable to read CA private key. Provide --root-ca-key or include unencrypted private key in --root-ca PEM.");
        }

        printInfo("Generating server certificate signed by provided CA");
        KeyPair serverKeys = generateRsaKeyPair();
        X500Name serverSubject = new X500Name("CN=localhost");
        GeneralNames serverSans = new GeneralNames(new GeneralName[] {
                new GeneralName(GeneralName.dNSName, "localhost"),
                new GeneralName(GeneralName.iPAddress, "127.0.0.1")
        });

        X509Certificate serverCertificate = generateCertificate(
                serverSubject,
                new X500Name(caCertificate.getSubjectX500Principal().getName()),
                serverKeys.getPublic(),
                caPrivateKey,
                caCertificate.getPublicKey(),
                false,
                serverSans);

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
        BigInteger serial = new BigInteger(160, SECURE_RANDOM).abs();
        if (BigInteger.ZERO.equals(serial)) {
            serial = BigInteger.ONE;
        }

        X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                issuer, serial, notBefore, notAfter, subject, subjectPublicKey);
        certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCa));
        certBuilder.addExtension(
                Extension.keyUsage,
                true,
                new KeyUsage(isCa
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

    private static X509Certificate readCertificateFromPem(Path pemPath) throws Exception {
        try (Reader reader = Files.newBufferedReader(pemPath, StandardCharsets.US_ASCII);
                PEMParser parser = new PEMParser(reader)) {
            Object pemObject;
            while ((pemObject = parser.readObject()) != null) {
                if (pemObject instanceof org.bouncycastle.cert.X509CertificateHolder) {
                    return new JcaX509CertificateConverter()
                            .setProvider(BC_PROVIDER)
                            .getCertificate((org.bouncycastle.cert.X509CertificateHolder) pemObject);
                }
            }
        }
        throw new IllegalStateException("No certificate found in PEM file: " + pemPath);
    }

    private static PrivateKey readPrivateKeyFromPem(Path pemPath) throws Exception {
        JcaPEMKeyConverter keyConverter = new JcaPEMKeyConverter().setProvider(BC_PROVIDER);
        try (Reader reader = Files.newBufferedReader(pemPath, StandardCharsets.US_ASCII);
                PEMParser parser = new PEMParser(reader)) {
            Object pemObject;
            while ((pemObject = parser.readObject()) != null) {
                if (pemObject instanceof PEMKeyPair) {
                    return keyConverter.getKeyPair((PEMKeyPair) pemObject).getPrivate();
                }
                if (pemObject instanceof PrivateKeyInfo) {
                    return keyConverter.getPrivateKey((PrivateKeyInfo) pemObject);
                }
                if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
                    throw new IllegalStateException("Encrypted private keys are not supported in this example: " + pemPath);
                }
            }
        }
        return null;
    }

    private static void printInfo(String message) {
        System.out.println("[secure-connection] " + message);
    }

    private static void writeClickHouseSslConfig(Path configPath, Path certificatePath, Path privateKeyPath)
            throws IOException {
        String config = "<clickhouse>\n"
                + "    <https_port>" + HTTPS_PORT + "</https_port>\n"
                + "    <openSSL>\n"
                + "        <server>\n"
                + "            <certificateFile>" + certificatePath.toAbsolutePath() + "</certificateFile>\n"
                + "            <privateKeyFile>" + privateKeyPath.toAbsolutePath() + "</privateKeyFile>\n"
                + "            <verificationMode>none</verificationMode>\n"
                + "            <cacheSessions>true</cacheSessions>\n"
                + "            <disableProtocols>sslv2,sslv3</disableProtocols>\n"
                + "            <preferServerCiphers>true</preferServerCiphers>\n"
                + "        </server>\n"
                + "    </openSSL>\n"
                + "</clickhouse>\n";
        try (OutputStream out = Files.newOutputStream(configPath)) {
            out.write(config.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static void deleteIfExists(Path path) {
        if (path == null) {
            return;
        }

        try {
            if (!Files.exists(path)) {
                return;
            }
            try (Stream<Path> targets = Files.walk(path)) {
                targets
                        .sorted((left, right) -> right.getNameCount() - left.getNameCount())
                        .forEach(target -> {
                            try {
                                Files.deleteIfExists(target);
                            } catch (IOException ignored) {
                                // Best effort cleanup for temporary example files.
                            }
                        });
            }
        } catch (IOException ignored) {
            // Best effort cleanup for temporary example files.
        }
    }

    public static void main(String... args) {
        System.exit(new CommandLine(new SecureConnectionMain()).execute(args));
    }
}
