package com.clickhouse.client.api.metrics;

import com.clickhouse.client.api.ClientMisconfigurationException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.httpcomponents.hc5.PoolingHttpClientConnectionManagerMetricsBinder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;

public class MicrometerLoader {

    public static void applyPoolingMetricsBinder(Object registry, String metricsGroupName, PoolingHttpClientConnectionManager phccm) {
        if (registry instanceof MeterRegistry) {
            new PoolingHttpClientConnectionManagerMetricsBinder(phccm, metricsGroupName).bindTo((MeterRegistry) registry);
        } else {
            throw new ClientMisconfigurationException("Unsupported registry type." + registry.getClass());
        }
    }
}
