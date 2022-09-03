package com.clickhouse.client.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
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
import com.clickhouse.client.ClickHouseUtils;

public class ClickHouseDefaultSslContextProvider implements ClickHouseSslContextProvider {
    static final String PEM_BEGIN_PART1 = "---BEGIN ";
    static final String PEM_BEGIN_PART2 = " PRIVATE KEY---";

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
        public void checkClientTrusted(X509Certificate[] certs, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] certs, String authType) {
        }
    }

    protected KeyStore getKeyStore(String cert, String key)
            throws NoSuchAlgorithmException, InvalidKeySpecException, IOException, CertificateException,
            KeyStoreException {
        KeyStore ks;
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
                String algorithm = (String) ClickHouseDefaults.SSL_KEY_ALGORITHM.getEffectiveDefaultValue();
                StringBuilder builder = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(ClickHouseUtils.getFileInputStream(key)))) {
                    String str;
                    boolean started = false;
                    while ((str = reader.readLine()) != null) {
                        if (!started) {
                            int startIndex = str.indexOf(PEM_BEGIN_PART1);
                            int endIndex = startIndex < 0 ? -1
                                    : str.indexOf(PEM_BEGIN_PART2, (startIndex += PEM_BEGIN_PART1.length() - 1));
                            if (startIndex < endIndex) {
                                algorithm = str.substring(startIndex, endIndex);
                            }
                            started = true;
                        } else if (str.indexOf("---END ") < 0) {
                            builder.append(str);
                        } else {
                            break;
                        }
                    }
                }
                byte[] encoded = Base64.getDecoder().decode(builder.toString());
                KeyFactory kf = KeyFactory.getInstance(algorithm);
                PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
                Certificate[] certChain = factory.generateCertificates(in).toArray(new Certificate[0]);
                ks.setKeyEntry("key", kf.generatePrivate(keySpec), null, certChain);
            }
        }
        return ks;
    }

    @SuppressWarnings("lgtm[java/insecure-trustmanager]")
    protected SSLContext getJavaSslContext(ClickHouseConfig config) throws SSLException {
        ClickHouseSslMode sslMode = config.getSslMode();
        String clientCert = config.getSslCert();
        String clientKey = config.getSslKey();
        String sslRootCert = config.getSslRootCert();

        SSLContext ctx;
        try {
            ctx = SSLContext.getInstance((String) ClickHouseDefaults.SSL_PROTOCOL.getEffectiveDefaultValue());
            TrustManager[] tms = null;
            KeyManager[] kms = null;
            SecureRandom sr = null;

            if (sslMode == ClickHouseSslMode.NONE) {
                tms = new TrustManager[] { new NonValidatingTrustManager() };
                kms = new KeyManager[0];
                sr = new SecureRandom();
            } else if (sslMode == ClickHouseSslMode.STRICT) {
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
