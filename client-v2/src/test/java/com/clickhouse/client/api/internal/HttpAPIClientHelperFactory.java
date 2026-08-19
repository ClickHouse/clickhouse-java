package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.observability.DefaultSpanRecorder;
import net.jpountz.lz4.LZ4Factory;

import java.util.Map;

/**
 * Builds {@link HttpAPIClientHelper} instances for tests. Tests construct the helper through this
 * factory instead of calling the constructor directly so that a change to the constructor signature
 * is applied in one place instead of in every test.
 */
public final class HttpAPIClientHelperFactory {

    private HttpAPIClientHelperFactory() {
    }

    /**
     * Creates a helper with no metrics registry, no SSL context and observability disabled - the
     * configuration every test that does not exercise those features needs.
     *
     * @param configuration - client configuration
     * @param lz4Factory    - LZ4 factory the helper should use
     * @return a new helper instance
     */
    public static HttpAPIClientHelper newHelper(Map<String, Object> configuration, LZ4Factory lz4Factory) {
        return new HttpAPIClientHelper(configuration, null, false, lz4Factory, DefaultSpanRecorder.NOOP);
    }
}
