package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import org.apache.hc.core5.http.HttpHeaders;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class WorkloadIdentitySupportTest {

    @Test
    public void testUserAgentComposedOncePerClient() {
        Map<String, String> rawConfig = new HashMap<>();
        rawConfig.put(ClientConfigProperties.CLIENT_NAME.getKey(), "my-app/1.0");
        rawConfig.put(ClientConfigProperties.CLIENT_METADATA.getKey(), "api=table");
        Map<String, Object> config = ClientConfigProperties.parseConfigMap(rawConfig);
        WorkloadIdentitySupport identity = new WorkloadIdentitySupport(config);

        String userAgent = identity.getUserAgent(null, config);
        Assert.assertEquals(userAgent, userAgent("my-app/1.0 ", "api:table; "));
        Assert.assertSame(identity.getUserAgent(null, new HashMap<>(config)), userAgent);
        Assert.assertSame(identity.getUserAgent(" ", config), userAgent);

        Map<String, Object> requestConfig = new HashMap<>(config);
        requestConfig.put(ClientConfigProperties.CLIENT_NAME.getKey(), "op-app/3.0");
        Assert.assertEquals(identity.getUserAgent(null, requestConfig), userAgent("op-app/3.0 ", "api:table; "));
        Assert.assertEquals(identity.getUserAgent(" custom-agent/2.0 ", config),
                userAgent("custom-agent/2.0 ", "api:table; "));

        Assert.assertSame(identity.getUserAgent(null, config), userAgent);
    }

    @Test
    public void testUserAgentComposedOnceForConfiguredHeader() {
        Map<String, String> rawConfig = new HashMap<>();
        rawConfig.put(ClientConfigProperties.CLIENT_NAME.getKey(), "my-app/1.0");
        rawConfig.put(ClientConfigProperties.httpHeader(HttpHeaders.USER_AGENT), "custom-agent/2.0");
        Map<String, Object> config = ClientConfigProperties.parseConfigMap(rawConfig);
        WorkloadIdentitySupport identity = new WorkloadIdentitySupport(config);

        String userAgent = identity.getUserAgent("custom-agent/2.0", config);
        Assert.assertEquals(userAgent, userAgent("custom-agent/2.0 ", ""));
        Assert.assertSame(identity.getUserAgent("custom-agent/2.0", config), userAgent);
        Assert.assertEquals(identity.getUserAgent(null, config), userAgent("my-app/1.0 ", ""));
    }

    @Test
    public void testUserAgentWithoutClientMetadata() {
        Map<String, Object> withMetadata = ClientConfigProperties.parseConfigMap(
                Collections.singletonMap(ClientConfigProperties.CLIENT_METADATA.getKey(), "api=table"));
        WorkloadIdentitySupport identityWithMetadata = new WorkloadIdentitySupport(withMetadata);

        Map<String, Object> config = Collections.emptyMap();
        Assert.assertEquals(new WorkloadIdentitySupport(config).getUserAgent(null, config), userAgent("", ""));

        Map<String, Object> emptyMetadata = ClientConfigProperties.parseConfigMap(
                Collections.singletonMap(ClientConfigProperties.CLIENT_METADATA.getKey(), ""));
        Assert.assertEquals(new WorkloadIdentitySupport(emptyMetadata).getUserAgent(null, emptyMetadata),
                userAgent("", ""));

        Assert.assertEquals(identityWithMetadata.getUserAgent("my-app/1.0", withMetadata),
                userAgent("my-app/1.0 ", "api:table; "));
    }

    private static String userAgent(String application, String metadata) {
        return application + "clickhouse-java-v2/" + Client.clientVersion + " (" + metadata
                + "lv:java/" + System.getProperty("java.version") + "; os:" + System.getProperty("os.name") + ")";
    }
}
