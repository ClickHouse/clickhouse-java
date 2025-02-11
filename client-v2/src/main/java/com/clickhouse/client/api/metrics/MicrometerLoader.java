package com.clickhouse.client.api.metrics;

import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.internal.HttpAPIClientHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
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

    public static void applyConnectionMetricsBinder(Object registry, String metricsGroupName, HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory factory) {
        if (registry instanceof MeterRegistry) {
            Gauge.builder("httpcomponents.httpclient.connect.time", factory, HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory::getTime)
                    .description("The running average connection creation time.")
                    .tag("httpclient", metricsGroupName)
                    .register((MeterRegistry) registry);
        } else {
            throw new ClientMisconfigurationException("Unsupported registry type." + registry.getClass());
        }
    }

    public static void applyFailureRatioMetricsBinder(Object registry, String metricsGroupName, HttpAPIClientHelper httpAPIClientHelper) {
        if (registry instanceof MeterRegistry) {
            Gauge.builder("httpcomponents.httpclient.request.ratio", httpAPIClientHelper, HttpAPIClientHelper::getRequestRatio)
                    .description("The ratio of total requests to failures via timeout.")
                    .tag("httpclient", metricsGroupName)
                    .register((MeterRegistry) registry);
        } else {
            throw new ClientMisconfigurationException("Unsupported registry type." + registry.getClass());
        }
    }
}
