package com.clickhouse.client.api.internal;

import org.apache.hc.core5.http.config.CharCodingConfig;
import org.apache.hc.core5.http.config.Http1Config;
import org.apache.hc.core5.http.impl.io.DefaultHttpResponseParserFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.Socket;

public class MeteredConnectionFactoryTest {

    private static HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory newFactory() {
        return new HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory(
                Http1Config.DEFAULT, CharCodingConfig.DEFAULT, DefaultHttpResponseParserFactory.INSTANCE);
    }

    @Test(groups = {"unit"})
    public void testConnectionTimesNotRecordedWithoutMetrics() throws Exception {
        HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory factory = newFactory();
        for (int i = 0; i < 10; i++) {
            factory.createConnection(new Socket());
        }
        Assert.assertEquals(factory.times.size(), 0, "Connection times must not accumulate when metrics are disabled");
    }

    @Test(groups = {"unit"})
    public void testConnectionTimesRecordedAndDrainedWithMetrics() throws Exception {
        HttpAPIClientHelper.MeteredManagedHttpClientConnectionFactory factory = newFactory();
        factory.enableMetrics();
        for (int i = 0; i < 10; i++) {
            factory.createConnection(new Socket());
        }
        Assert.assertEquals(factory.times.size(), 10);
        factory.getTime();
        Assert.assertEquals(factory.times.size(), 0);
    }
}
