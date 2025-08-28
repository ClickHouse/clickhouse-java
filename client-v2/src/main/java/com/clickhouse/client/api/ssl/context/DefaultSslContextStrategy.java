package com.clickhouse.client.api.ssl.context;

import com.clickhouse.client.api.ClientMisconfigurationException;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;

public final class DefaultSslContextStrategy implements SslContextStrategy {

    @Override
    public boolean support() {
        return true;
    }

    @Override
    public SSLContext createSSLContext() {
        try {
            return SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new ClientMisconfigurationException("Failed to create default SSL context", e);
        }
    }
}
