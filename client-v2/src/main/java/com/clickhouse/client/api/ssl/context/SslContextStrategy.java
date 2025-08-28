package com.clickhouse.client.api.ssl.context;

import com.clickhouse.client.api.ClientMisconfigurationException;

import javax.net.ssl.SSLContext;

/**
 * Strategy interface for creating and configuring SSLContext instances based on different
 * SSL configuration approaches.
 *
 * <p><b>Typical usage pattern:</b></p>
 * <pre>
 * {@code
 * // Create appropriate strategy based on configuration
 * SslContextStrategy strategy = SslContextStrategyFactory.createStrategy(config);
 *
 * // Check if the strategy supports the current configuration
 * if (strategy.support()) {
 *     // Create and use the SSL context
 *     SSLContext sslContext = strategy.createSSLContext();
 *     // Configure HTTP client or other components with the SSL context
 * }
 * }
 * </pre>
 *
 * @author sbushemelev
 */
public interface SslContextStrategy {

    /**
     * Checks if this strategy supports the current configuration.
     *
     * @return true if required configuration parameters are present and valid
     */
    boolean support();

    /**
     * Creates and configures an SSLContext instance.
     *
     * @return configured SSLContext ready for use
     * @throws ClientMisconfigurationException if context creation fails due to configuration errors or security issues
     */
    SSLContext createSSLContext();

}
