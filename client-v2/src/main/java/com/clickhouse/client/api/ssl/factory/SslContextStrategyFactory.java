package com.clickhouse.client.api.ssl.factory;

import com.clickhouse.client.api.ssl.context.AllTrustSslContextStrategy;
import com.clickhouse.client.api.ssl.context.DefaultSslContextStrategy;
import com.clickhouse.client.api.ssl.context.FromCertSslContextStrategy;
import com.clickhouse.client.api.ssl.context.FromKeyStoreSslContextStrategy;
import com.clickhouse.client.api.ssl.context.SslContextStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Factory for creating SSL context strategies based on configuration.
 * Selects the first supported strategy from the available implementations.
 *
 * @author sbushemelev
 */
public final class SslContextStrategyFactory {

    private final List<SslContextStrategy> sslContextStrategies;

    public SslContextStrategyFactory(Map<String, Object> config) {
        this.sslContextStrategies = new ArrayList<>();
        this.sslContextStrategies.add(new AllTrustSslContextStrategy(config));
        this.sslContextStrategies.add(new FromKeyStoreSslContextStrategy(config));
        this.sslContextStrategies.add(new FromCertSslContextStrategy(config));
    }

    /**
     * Returns the first supported SSL context strategy.
     * Falls back to default strategy if none are supported.
     */
    public SslContextStrategy getSslContextStrategy() {
        for (SslContextStrategy strategy : sslContextStrategies) {
            if (strategy.support()) {
                return strategy;
            }
        }
        return new DefaultSslContextStrategy();
    }

}
