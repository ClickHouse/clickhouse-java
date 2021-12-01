package com.clickhouse.client.config;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Optional;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.ClickHouseUtils;

public class ClickHouseDefaultSslContextProvider implements ClickHouseSslContextProvider {
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

    protected KeyStore getKeyStore(String sslRootCert)
            throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException {
        KeyStore ks;
        try {
            ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null); // needed to initialize the key store
        } catch (KeyStoreException e) {
            throw new NoSuchAlgorithmException(
                    ClickHouseUtils.format("%s KeyStore not available", KeyStore.getDefaultType()));
        }

        try (InputStream in = ClickHouseUtils.getFileInputStream(sslRootCert)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            int index = 0;
            for (Certificate cert : factory.generateCertificates(in)) {
                ks.setCertificateEntry("cert" + (index++), cert);
            }

            return ks;
        }
    }

    protected SSLContext getJavaSslContext(ClickHouseConfig config) throws SSLException {
        ClickHouseSslMode sslMode = config.getSslMode();
        String sslRootCert = config.getSslRootCert();

        SSLContext ctx;
        try {
            ctx = SSLContext.getInstance("TLS");
            TrustManager[] tms = null;
            KeyManager[] kms = null;
            SecureRandom sr = null;

            if (sslMode == ClickHouseSslMode.NONE) {
                tms = new TrustManager[] { new NonValidatingTrustManager() };
                kms = new KeyManager[] {};
                sr = new SecureRandom();
            } else if (sslMode == ClickHouseSslMode.STRICT) {
                if (sslRootCert != null && !sslRootCert.isEmpty()) {
                    TrustManagerFactory tmf = TrustManagerFactory
                            .getInstance(TrustManagerFactory.getDefaultAlgorithm());

                    tmf.init(getKeyStore(sslRootCert));
                    tms = tmf.getTrustManagers();
                    kms = new KeyManager[] {};
                    sr = new SecureRandom();
                }
            } else {
                throw new IllegalArgumentException(ClickHouseUtils.format("unspported ssl mode '%s'", sslMode));
            }

            ctx.init(kms, tms, sr);
        } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | CertificateException
                | IOException e) {
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
