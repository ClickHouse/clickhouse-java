package com.clickhouse.client.api.metrics;

import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.internal.HttpAPIClientHelper;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.pool.ConnPoolControl;

public class MicrometerLoader {

    public static void applyPoolingMetricsBinder(Object registry, String metricsGroupName, PoolingHttpClientConnectionManager phccm) {
        if (registry instanceof MeterRegistry) {
            Iterable<Tag> tags = Tags.of("httpclient", metricsGroupName);
            Gauge
                    .builder("httpcomponents.httpclient.pool.total.max", phccm,
                            (connPoolControl) -> connPoolControl.getTotalStats().getMax())
                    .description("The configured maximum number of allowed persistent connections for all routes.")
                    .tags(tags)
                    .register((MeterRegistry) registry);
            Gauge
                    .builder("httpcomponents.httpclient.pool.total.connections", phccm,
                            (connPoolControl) -> connPoolControl.getTotalStats().getAvailable())
                    .description("The number of persistent and available connections for all routes.")
                    .tags(tags)
                    .tag("state", "available")
                    .register((MeterRegistry) registry);
            Gauge
                    .builder("httpcomponents.httpclient.pool.total.connections", phccm,
                            (connPoolControl) -> connPoolControl.getTotalStats().getLeased())
                    .description("The number of persistent and leased connections for all routes.")
                    .tags(tags)
                    .tag("state", "leased")
                    .register((MeterRegistry) registry);
            Gauge
                    .builder("httpcomponents.httpclient.pool.total.pending", phccm,
                            (connPoolControl) -> connPoolControl.getTotalStats().getPending())
                    .description("The number of connection requests being blocked awaiting a free connection for all routes.")
                    .tags(tags)
                    .register((MeterRegistry) registry);
            Gauge
                    .builder("httpcomponents.httpclient.pool.route.max.default", phccm,
                            ConnPoolControl::getDefaultMaxPerRoute)
                    .description("The configured default maximum number of allowed persistent connections per route.")
                    .tags(tags)
                    .register((MeterRegistry) registry);

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

}
