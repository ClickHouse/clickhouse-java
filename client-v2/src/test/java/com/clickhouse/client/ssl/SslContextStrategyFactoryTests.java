package com.clickhouse.client.ssl;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ssl.context.AllTrustSslContextStrategy;
import com.clickhouse.client.api.ssl.context.DefaultSslContextStrategy;
import com.clickhouse.client.api.ssl.context.FromCertSslContextStrategy;
import com.clickhouse.client.api.ssl.context.FromKeyStoreSslContextStrategy;
import com.clickhouse.client.api.ssl.context.SslContextStrategy;
import com.clickhouse.client.api.ssl.factory.SslContextStrategyFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Test(groups = {"unit"})
public class SslContextStrategyFactoryTests {

    @Test(dataProvider = "sslContextData")
    void defaultSslContextStrategy(Map<String, Object> config, Class<? extends SslContextStrategy> expectedClass) throws IOException {
        Assert.assertEquals(new SslContextStrategyFactory(config).getSslContextStrategy().getClass(), expectedClass);
    }

    @DataProvider(name = "sslContextData")
    private static Object[][] createStringParameterValues() {
        Map<String, Object> allTrustSslContext = new HashMap<>();
        allTrustSslContext.put(ClientConfigProperties.SSL_TRUST_ALL_STRATEGY.getKey(), true);

        Map<String, Object> fromCertSslContext = new HashMap<>();
        fromCertSslContext.put(ClientConfigProperties.CA_CERTIFICATE.getKey(), "localhost.crt");
        fromCertSslContext.put(ClientConfigProperties.SSL_CERTIFICATE.getKey(), "localhost.pem");
        fromCertSslContext.put(ClientConfigProperties.SSL_KEY.getKey(), "localhost.key");

        Map<String, Object> fromKeyStoreSslContext = new HashMap<>();
        fromKeyStoreSslContext.put(ClientConfigProperties.SSL_TRUST_STORE.getKey(), "trustStore");

        Map<String, Object> defaultSslContext = new HashMap<>();
        return new Object[][]{
                {allTrustSslContext, AllTrustSslContextStrategy.class},
                {fromCertSslContext, FromCertSslContextStrategy.class},
                {fromKeyStoreSslContext, FromKeyStoreSslContextStrategy.class},
                {defaultSslContext, DefaultSslContextStrategy.class}
        };
    }

}
