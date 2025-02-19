package com.clickhouse.client.config;

import java.io.*;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Optional;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.data.ClickHouseUtils;

@Deprecated
public class ClickHouseDefaultSslContextProvider implements ClickHouseSslContextProvider {
    static final String PEM_HEADER_PREFIX = "---BEGIN ";
    static final String PEM_HEADER_SUFFIX = " PRIVATE KEY---";
    static final String PEM_FOOTER_PREFIX = "---END ";

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
        String algorithm = (String) ClickHouseDefaults.SSL_KEY_ALGORITHM.getEffectiveDefaultValue();
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(ClickHouseUtils.getFileInputStream(keyFile)))) {
            String line = reader.readLine();
            if (line != null) {
                algorithm = getAlgorithm(line, algorithm);

                while ((line = reader.readLine()) != null) {
                    if (line.indexOf(PEM_FOOTER_PREFIX) >= 0) {
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
                    ClickHouseUtils.format("%s KeyStore not available", KeyStore.getDefaultType()));
        }

        try (InputStream in = ClickHouseUtils.getFileInputStream(cert)) {
            CertificateFactory factory = CertificateFactory
                    .getInstance((String) ClickHouseDefaults.SSL_CERTIFICATE_TYPE.getEffectiveDefaultValue());
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

    public SSLContext getJavaSslContext(ClickHouseConfig config) throws SSLException {
        ClickHouseSslMode sslMode = config.getSslMode();
        String clientCert = config.getSslCert();
        String clientKey = config.getSslKey();
        String sslRootCert = config.getSslRootCert();
        String truststorePath = config.getTrustStore();
        String truststorePassword = config.getTrustStorePassword();
        String keyStoreType = (!config.getKeyStoreType().isEmpty() && config.getKeyStoreType() != null) ? config.getKeyStoreType() : KeyStore.getDefaultType();

        return getSslContextImpl(sslMode, clientCert, clientKey, sslRootCert, truststorePath, truststorePassword,
                keyStoreType);
    }

    public SSLContext getSslContextFromCerts(String clientCert, String clientKey, String sslRootCert) throws SSLException {
        return getSslContextImpl(ClickHouseSslMode.STRICT,
                clientCert, clientKey, sslRootCert, null, null, KeyStore.getDefaultType());
    }

    public SSLContext getSslContextFromKeyStore(String truststorePath, String truststorePassword, String keyStoreType) throws SSLException {
        return getSslContextImpl(ClickHouseSslMode.STRICT, null, null, null, truststorePath, truststorePassword, keyStoreType);
    }

    private SSLContext getSslContextImpl(ClickHouseSslMode sslMode, String clientCert, String clientKey, String sslRootCert, String truststorePath, String truststorePassword, String keyStoreType) throws SSLException {
        SSLContext ctx;
        try {
            ctx = SSLContext.getInstance((String) ClickHouseDefaults.SSL_PROTOCOL.getEffectiveDefaultValue());
            TrustManager[] tms = null;
            KeyManager[] kms = null;
            SecureRandom sr = null;

            if (sslMode == ClickHouseSslMode.NONE) {
                tms = new TrustManager[]{new NonValidatingTrustManager()};
                kms = new KeyManager[0];
                sr = new SecureRandom();
            } else if (sslMode == ClickHouseSslMode.STRICT) {
                if (truststorePath != null && !truststorePath.isEmpty()) {

                    try (InputStream in = ClickHouseUtils.getFileInputStream(truststorePath)) {
                        KeyStore myTrustStore = KeyStore.getInstance(keyStoreType);
                        myTrustStore.load(in, truststorePassword.toCharArray());
                        TrustManagerFactory factory = TrustManagerFactory
                                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                        factory.init(myTrustStore);
                        tms = factory.getTrustManagers();

                    }
                } else {
                    if (clientCert != null && !clientCert.isEmpty()) {
                        KeyManagerFactory factory = KeyManagerFactory
                                .getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        factory.init(getKeyStore(clientCert, clientKey), null);
                        kms = factory.getKeyManagers();
                    }

                    if (sslRootCert != null && !sslRootCert.isEmpty()) {
                        TrustManagerFactory factory = TrustManagerFactory
                                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                        factory.init(getKeyStore(sslRootCert, null));
                        tms = factory.getTrustManagers();
                    }
                }

                sr = new SecureRandom();
            } else {
                throw new IllegalArgumentException(ClickHouseUtils.format("unspported ssl mode '%s'", sslMode));
            }

            ctx.init(kms, tms, sr);
        } catch (KeyManagementException | InvalidKeySpecException | NoSuchAlgorithmException | KeyStoreException
                 | CertificateException | IOException | UnrecoverableKeyException e) {
            throw new SSLException("Failed to get SSL context", e);
        }

        return ctx;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<T> getSslContext(Class<? extends T> sslContextClass, ClickHouseConfig config)
            throws SSLException {
        return SSLContext.class == sslContextClass ? Optional.of((T) getJavaSslContext(config)) : Optional.empty();
    }
}
