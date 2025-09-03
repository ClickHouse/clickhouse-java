package com.clickhouse.client.api.ssl.context;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.data.ClickHouseUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.Map;

public final class FromKeyStoreSslContextStrategy implements SslContextStrategy {

    private final Map<String, Object> config;

    public FromKeyStoreSslContextStrategy(Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public boolean support() {
        return this.config.get(ClientConfigProperties.SSL_TRUST_STORE.getKey()) != null;
    }

    @Override
    public SSLContext createSSLContext() {
        String trustStorePath = (String) config.get(ClientConfigProperties.SSL_TRUST_STORE.getKey());
        String truststorePassword = (String) config.get(ClientConfigProperties.SSL_KEY_STORE_PASSWORD.getKey());
        String keyStoreType = (String) config.get(ClientConfigProperties.SSL_KEYSTORE_TYPE.getKey());
        try (InputStream in = ClickHouseUtils.getFileInputStream(trustStorePath)) {
            SSLContext ctx = SSLContext.getInstance((String) config.get(ClientConfigProperties.SSL_PROTOCOL.getKey()));
            KeyStore myTrustStore = KeyStore.getInstance(keyStoreType);
            myTrustStore.load(in, truststorePassword != null ? truststorePassword.toCharArray() : null);
            TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(myTrustStore);
            ctx.init(null, factory.getTrustManagers(), new SecureRandom());
            return ctx;
        } catch (KeyManagementException | KeyStoreException | NoSuchAlgorithmException | CertificateException |
                 IOException e) {
            throw new ClientMisconfigurationException("Failed to create SSL context from a keystore", e);
        }
    }
}
