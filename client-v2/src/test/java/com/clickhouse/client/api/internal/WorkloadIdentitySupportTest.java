package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import org.apache.hc.core5.http.HttpHeaders;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class WorkloadIdentitySupportTest {

    private static final String NAME = ClientConfigProperties.CLIENT_NAME.getKey();

    private static final String METADATA = ClientConfigProperties.CLIENT_ID_METADATA.getKey();

    private static final String USER_AGENT = ClientConfigProperties.httpHeader(HttpHeaders.USER_AGENT);

    private static final String LANGUAGE_VERSION = "lv:java/" + System.getProperty("java.version");

    private static final String OS = "os:" + System.getProperty("os.name");

    private static final String HTTP_CLIENT = "hc:ahc_" + Versions.APACHE_HTTP_CLIENT;

    @Test(dataProvider = "requestUserAgents")
    public void testGetUserAgent(Map<String, String> clientSettings, String customUserAgent, String requestClientName,
                                 String expected) {
        Map<String, Object> config = ClientConfigProperties.parseConfigMap(clientSettings);
        WorkloadIdentitySupport identity = new WorkloadIdentitySupport(config);
        String clientUserAgent = clientUserAgent(identity, config);
        Map<String, Object> requestConfig = new HashMap<>(config);
        if (requestClientName != null) {
            requestConfig.put(NAME, requestClientName);
        }

        String userAgent = identity.getUserAgent(customUserAgent, requestConfig);

        Assert.assertEquals(userAgent, expected);
        if (expected.equals(clientUserAgent)) {
            Assert.assertSame(userAgent, clientUserAgent);
        }
        Assert.assertSame(clientUserAgent(identity, config), clientUserAgent);
    }

    @DataProvider(name = "requestUserAgents")
    public static Object[][] requestUserAgents() {
        return new Object[][]{
                {settings(), null, null, userAgent(null)},
                {settings(METADATA, ""), null, null, userAgent(null)},
                {settings(NAME, "my-app/1.0"), null, null, userAgent("my-app/1.0")},
                {settings(NAME, " my-app/1.0 "), null, null, userAgent("my-app/1.0")},
                {settings(NAME, "my-app/1.0", METADATA, "api=table"), null, null,
                        userAgent("my-app/1.0", "api:table")},
                {settings(NAME, "my-app/1.0"), " ", null, userAgent("my-app/1.0")},
                {settings(NAME, "my-app/1.0"), null, "", userAgent("my-app/1.0")},
                {settings(NAME, "my-app/1.0"), "", " ", userAgent("my-app/1.0")},
                {settings(NAME, "my-app/1.0", METADATA, "api=table"), null, "op-app/3.0",
                        userAgent("op-app/3.0", "api:table")},
                {settings(NAME, "my-app/1.0", METADATA, "api=table"), " custom-agent/2.0 ", null,
                        userAgent("custom-agent/2.0", "api:table")},
                {settings(NAME, "my-app/1.0", USER_AGENT, "custom-agent/2.0"), "custom-agent/2.0", null,
                        userAgent("custom-agent/2.0")},
                {settings(NAME, "my-app/1.0", USER_AGENT, "custom-agent/2.0"), null, null,
                        userAgent("my-app/1.0")},
        };
    }

    @Test(dataProvider = "clientNameUpdates")
    public void testUpdateClientName(Map<String, String> clientSettings, String clientName, String expected) {
        Map<String, Object> config = ClientConfigProperties.parseConfigMap(clientSettings);
        WorkloadIdentitySupport identity = new WorkloadIdentitySupport(config);
        Map<String, Object> updatedConfig = new HashMap<>(config);
        updatedConfig.put(NAME, clientName);

        identity.updateClientName(updatedConfig);
        String userAgent = clientUserAgent(identity, updatedConfig);

        Assert.assertEquals(userAgent, expected);
        Assert.assertSame(clientUserAgent(identity, updatedConfig), userAgent);
    }

    @DataProvider(name = "clientNameUpdates")
    public static Object[][] clientNameUpdates() {
        return new Object[][]{
                {settings(NAME, "my-app/1.0"), "new-app/2.0", userAgent("new-app/2.0")},
                {settings(NAME, "my-app/1.0", METADATA, "api=table"), "new-app/2.0",
                        userAgent("new-app/2.0", "api:table")},
                {settings(NAME, "my-app/1.0", USER_AGENT, "custom-agent/2.0"), "new-app/2.0",
                        userAgent("custom-agent/2.0")},
        };
    }

    @Test(dataProvider = "clientEntriesMetadata")
    public void testClientMetadataReplacesClientEntries(String metadata, String expectedComment) {
        Map<String, Object> config = ClientConfigProperties.parseConfigMap(settings(METADATA, metadata));
        Assert.assertEquals(new WorkloadIdentitySupport(config).getUserAgent(null, config),
                "clickhouse-java-v2/" + Client.clientVersion + " (" + expectedComment + ")");
    }

    @DataProvider(name = "clientEntriesMetadata")
    public static Object[][] clientEntriesMetadata() {
        return new Object[][]{
                {"os=Android 14", "os:Android 14; " + LANGUAGE_VERSION + "; " + HTTP_CLIENT},
                {"api=table,lv=kotlin/2.0", "api:table; lv:kotlin/2.0; " + OS + "; " + HTTP_CLIENT},
                {"hc=okhttp_4.12", "hc:okhttp_4.12; " + LANGUAGE_VERSION + "; " + OS},
                {"OS=Android 14,LV=kotlin/2.0", "OS:Android 14; LV:kotlin/2.0; " + HTTP_CLIENT},
                {"os=Android 14,hc=okhttp_4.12,api=table,lv=kotlin/2.0",
                        "os:Android 14; hc:okhttp_4.12; api:table; lv:kotlin/2.0"},
        };
    }

    @Test
    public void testClientPartComposedPerInstance() {
        Map<String, Object> withMetadata = ClientConfigProperties.parseConfigMap(settings(METADATA, "api=table"));
        WorkloadIdentitySupport identityWithMetadata = new WorkloadIdentitySupport(withMetadata);
        Map<String, Object> withoutMetadata = ClientConfigProperties.parseConfigMap(settings());
        WorkloadIdentitySupport identityWithoutMetadata = new WorkloadIdentitySupport(withoutMetadata);

        Assert.assertEquals(identityWithMetadata.getUserAgent("my-app/1.0", withMetadata),
                userAgent("my-app/1.0", "api:table"));
        Assert.assertEquals(identityWithoutMetadata.getUserAgent("my-app/1.0", withoutMetadata),
                userAgent("my-app/1.0"));
    }

    /**
     * Returns the value for a request without operation settings. The transport passes the configured
     * {@code User-Agent} header as the custom value.
     */
    private static String clientUserAgent(WorkloadIdentitySupport identity, Map<String, Object> config) {
        return identity.getUserAgent((String) config.get(USER_AGENT), config);
    }

    private static Map<String, String> settings(String... keysAndValues) {
        Map<String, String> settings = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            settings.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return settings;
    }

    private static String userAgent(String application) {
        return userAgent(application, null);
    }

    private static String userAgent(String application, String metadata) {
        return (application == null ? "" : application + " ") + "clickhouse-java-v2/" + Client.clientVersion + " ("
                + (metadata == null ? "" : metadata + "; ") + LANGUAGE_VERSION + "; " + OS + "; " + HTTP_CLIENT + ")";
    }
}
