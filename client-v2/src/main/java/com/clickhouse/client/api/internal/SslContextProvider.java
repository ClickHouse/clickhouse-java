package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.data.ClickHouseUtils;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * Builds {@link SSLContext} instances for the {@code client-v2} HTTP transport.
 *
 * <p>This is the {@code client-v2} owned counterpart of the deprecated v1
 * {@code ClickHouseDefaultSslContextProvider}. It is kept separate so the v1 provider can evolve
 * (or stay frozen) independently of {@code client-v2}.</p>
 *
 * <p>Contexts are assembled through {@link #builder()}, which sets key material (client
 * certificate/key for mTLS) and trust material (trust store, CA certificate, or "trust all")
 * independently, mirroring the structure of the v1 {@code getSslContextImpl}.</p>
 */
public class SslContextProvider {

    // Defaults copied from the v1 com.clickhouse.client.config.ClickHouseDefaults to avoid depending
    // on the deprecated v1 configuration classes.
    private static final String SSL_PROTOCOL = "TLS";
    private static final String KEY_ALGORITHM = "RSA";
    private static final String CERTIFICATE_TYPE = "X.509";

    static final String PEM_HEADER_PREFIX = "---BEGIN ";
    static final String PEM_HEADER_SUFFIX = " PRIVATE KEY---";
    static final String PEM_FOOTER_PREFIX = "---END ";

    /** Standard PEM encapsulation boundary (RFC 7468). Present in any PEM content, never in a file path. */
    static final String PEM_BEGIN_MARKER = "-----BEGIN";

    /**
     * Opens a stream over PEM material that may be supplied either as a file path (also searched in the home
     * directory and on the classpath) or directly as PEM content.
     *
     * @param certOrContent file path or PEM content of a certificate or a private key
     * @return stream over the PEM content
     * @throws IOException when the value is a path and the file cannot be opened
     */
    static InputStream getCertificateInputStream(String certOrContent) throws IOException {
        if (certOrContent.contains(PEM_BEGIN_MARKER)) {
            return new ByteArrayInputStream(certOrContent.getBytes(StandardCharsets.US_ASCII));
        }
        return ClickHouseUtils.getFileInputStream(certOrContent);
    }

    /**
     * An insecure {@link javax.net.ssl.TrustManager}, that don't validate the
     * certificate.
     */
    static class NonValidatingTrustManager implements X509TrustManager {
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        @Override
        @SuppressWarnings("squid:S4830")
        public void checkClientTrusted(X509Certificate[] certs, String authType) {
            // ignore
        }

        @Override
        @SuppressWarnings("squid:S4830")
        public void checkServerTrusted(X509Certificate[] certs, String authType) {
            // ignore
        }
    }

    static String getAlgorithm(String header, String defaultAlg) {
        int startIndex = header.indexOf(PEM_HEADER_PREFIX);
        int endIndex = startIndex < 0 ? startIndex
                : header.indexOf(PEM_HEADER_SUFFIX, (startIndex += PEM_HEADER_PREFIX.length()));
        return startIndex < endIndex ? header.substring(startIndex, endIndex) : defaultAlg;
    }

    public static PrivateKey getPrivateKey(String keyFile)
            throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        String algorithm = KEY_ALGORITHM;
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(getCertificateInputStream(keyFile)))) {
            String line = reader.readLine();
            if (line != null) {
                algorithm = getAlgorithm(line, algorithm);

                while ((line = reader.readLine()) != null) {
                    if (line.contains(PEM_FOOTER_PREFIX)) {
                        break;
                    }

                    builder.append(line);
                }
            }
        }
        byte[] encoded = Base64.getDecoder().decode(builder.toString());
        KeyFactory kf = KeyFactory.getInstance(algorithm);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
        return kf.generatePrivate(keySpec);
    }

    public KeyStore getKeyStore(String cert, String key) throws NoSuchAlgorithmException, InvalidKeySpecException,
            IOException, CertificateException, KeyStoreException {
        final KeyStore ks;
        try {
            ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null); // needed to initialize the key store
        } catch (KeyStoreException e) {
            throw new NoSuchAlgorithmException(
                    String.format("%s KeyStore not available", KeyStore.getDefaultType()));
        }

        try (InputStream in = getCertificateInputStream(cert)) {
            CertificateFactory factory = CertificateFactory.getInstance(CERTIFICATE_TYPE);
            if (key == null || key.isEmpty()) {
                int index = 1;
                for (Certificate c : factory.generateCertificates(in)) {
                    ks.setCertificateEntry("cert" + (index++), c);
                }
            } else {
                Certificate[] certChain = factory.generateCertificates(in).toArray(new Certificate[0]);
                ks.setKeyEntry("key", getPrivateKey(key), null, certChain);
            }
        }
        return ks;
    }

    /**
     * Creates a new {@link Builder} for assembling an {@link SSLContext}. Key material (client
     * certificate/key for mTLS) and trust material (trust store, CA certificate, or "trust all") are
     * configured independently, mirroring the structure of the v1 {@code getSslContextImpl}.
     *
     * @return new builder
     */
    public Builder builder() {
        return new Builder();
    }

    /**
     * Assembles an {@link SSLContext} from independently configured key and trust material.
     *
     * <p>The two are orthogonal:</p>
     * <ul>
     *     <li>{@link #clientCertificate(String, String)} sets the client certificate/key applied for
     *     mTLS; it is independent of how the server certificate is verified.</li>
     *     <li>{@link #trustAllCertificates()}, {@link #trustStore(String, String, String)} and
     *     {@link #rootCertificate(String)} are mutually exclusive trust strategies; the last one set
     *     wins. When none is set, the JVM default trust store is used.</li>
     * </ul>
     */
    public class Builder {

        private String clientCert;
        private String clientKey;
        private String trustStorePath;
        private String trustStorePassword;
        private String trustStoreType;
        private String rootCertificate;
        private boolean trustAll;

        /**
         * Sets the client certificate and key applied for mutual TLS. Independent of the trust strategy.
         *
         * @param clientCert client certificate, file path or PEM content; may be null
         * @param clientKey  client private key, file path or PEM content; may be null
         * @return this builder
         */
        public Builder clientCertificate(String clientCert, String clientKey) {
            this.clientCert = clientCert;
            this.clientKey = clientKey;
            return this;
        }

        /**
         * Trust strategy: accept any server certificate without validating it (no server identity check).
         *
         * @return this builder
         */
        public Builder trustAllCertificates() {
            this.trustAll = true;
            return this;
        }

        /**
         * Trust strategy: validate the server certificate against the given trust store.
         *
         * @param path     trust store file path
         * @param password trust store password; may be null
         * @param type     trust store type; when null or empty the JVM default type is used
         * @return this builder
         */
        public Builder trustStore(String path, String password, String type) {
            this.trustStorePath = path;
            this.trustStorePassword = password;
            this.trustStoreType = type;
            return this;
        }

        /**
         * Trust strategy: validate the server certificate against the given CA certificate.
         *
         * @param rootCertificate CA certificate, file path or PEM content; may be null
         * @return this builder
         */
        public Builder rootCertificate(String rootCertificate) {
            this.rootCertificate = rootCertificate;
            return this;
        }

        /**
         * Builds the SSL context from the configured key and trust material.
         *
         * @return SSL context
         * @throws ClientMisconfigurationException when the context cannot be created
         */
        public SSLContext build() {
            try {
                KeyManager[] kms = null;
                if (clientCert != null && !clientCert.isEmpty()) {
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(getKeyStore(clientCert, clientKey), null);
                    kms = kmf.getKeyManagers();
                }

                TrustManager[] tms = null;
                if (trustAll) {
                    tms = new TrustManager[]{new NonValidatingTrustManager()};
                } else if (trustStorePath != null && !trustStorePath.isEmpty()) {
                    String type = trustStoreType == null || trustStoreType.isEmpty()
                            ? KeyStore.getDefaultType() : trustStoreType;
                    try (InputStream in = ClickHouseUtils.getFileInputStream(trustStorePath)) {
                        KeyStore trustStore = KeyStore.getInstance(type);
                        trustStore.load(in, trustStorePassword == null ? null : trustStorePassword.toCharArray());
                        TrustManagerFactory tmf = TrustManagerFactory
                                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                        tmf.init(trustStore);
                        tms = tmf.getTrustManagers();
                    }
                } else if (rootCertificate != null && !rootCertificate.isEmpty()) {
                    TrustManagerFactory tmf = TrustManagerFactory
                            .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(getKeyStore(rootCertificate, null));
                    tms = tmf.getTrustManagers();
                }

                SSLContext ctx = SSLContext.getInstance(SSL_PROTOCOL);
                ctx.init(kms, tms, new SecureRandom());
                return ctx;
            } catch (GeneralSecurityException | IOException e) {
                throw new ClientMisconfigurationException("Failed to create SSL context", e);
            }
        }
    }
}
