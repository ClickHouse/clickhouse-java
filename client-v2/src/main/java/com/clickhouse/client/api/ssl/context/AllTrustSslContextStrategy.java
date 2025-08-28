package com.clickhouse.client.api.ssl.context;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public final class AllTrustSslContextStrategy implements SslContextStrategy {

    private final Map<String, Object> config;

    public AllTrustSslContextStrategy(Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public boolean support() {
        return this.config.get(ClientConfigProperties.SSL_TRUST_ALL_STRATEGY.getKey()) != null &&
                (boolean) this.config.get(ClientConfigProperties.SSL_TRUST_ALL_STRATEGY.getKey());
    }

    @Override
    public SSLContext createSSLContext() {
        try {
            return SSLContextBuilder.create()
                    .loadTrustMaterial(new TrustAllStrategy())
                    .build();
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new ClientMisconfigurationException("Failed to create SSL context with all trust strategy", e);
        }
    }
}
