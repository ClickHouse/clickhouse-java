package com.clickhouse.client.api.ssl.context;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.ssl.keystore.CertificateKeyStoreManager;
import com.clickhouse.client.api.ssl.keystore.DefaultCertificateKeyStoreManager;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;

public final class FromCertSslContextStrategy implements SslContextStrategy {

    private final CertificateKeyStoreManager certificateKeyStoreManager;

    private final Map<String, Object> config;

    public FromCertSslContextStrategy(Map<String, Object> config) {
        this(new DefaultCertificateKeyStoreManager(config), config);
    }

    public FromCertSslContextStrategy(CertificateKeyStoreManager certificateKeyStoreManager, Map<String, Object> config) {
        this.certificateKeyStoreManager = certificateKeyStoreManager;
        this.config = config;
    }

    @Override
    public boolean support() {
        return this.config.get(ClientConfigProperties.CA_CERTIFICATE.getKey()) != null ||
                this.config.get(ClientConfigProperties.SSL_CERTIFICATE.getKey()) != null ||
                this.config.get(ClientConfigProperties.SSL_KEY.getKey()) != null;
    }

    @Override
    public SSLContext createSSLContext() {
        try {
            SSLContext ctx = SSLContext.getInstance((String) config.get(ClientConfigProperties.SSL_PROTOCOL.getKey()));

            String clientCert = (String) config.get(ClientConfigProperties.SSL_CERTIFICATE.getKey());
            String clientKey = (String) config.get(ClientConfigProperties.SSL_KEY.getKey());
            String sslRootCert = (String) config.get(ClientConfigProperties.CA_CERTIFICATE.getKey());

            ctx.init(getKeyManger(clientCert, clientKey), getTrustManager(sslRootCert), new SecureRandom());

            return ctx;
        } catch (UnrecoverableKeyException | KeyManagementException | InvalidKeySpecException | KeyStoreException |
                 IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new ClientMisconfigurationException("Failed to create SSL context from certificates", e);
        }
    }

    private KeyManager[] getKeyManger(String clientCert, String clientKey) throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException, InvalidKeySpecException, UnrecoverableKeyException {
        if (clientCert != null && !clientCert.isEmpty()) {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory
                    .getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(certificateKeyStoreManager.getKeyStore(clientCert, clientKey), null);
            return keyManagerFactory.getKeyManagers();
        }
        return null;
    }

    private TrustManager[] getTrustManager(String sslRootCert) throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException {
        if (sslRootCert != null && !sslRootCert.isEmpty()) {
            TrustManagerFactory factoryFactory = TrustManagerFactory
                    .getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factoryFactory.init(certificateKeyStoreManager.getKeyStore(sslRootCert));
            return factoryFactory.getTrustManagers();
        }
        return null;
    }
}

