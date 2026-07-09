package com.clickhouse.client.api;

import com.clickhouse.client.api.enums.SSLMode;
import com.clickhouse.client.api.internal.HttpAPIClientHelper;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientBuilderTest {

    @Test
    public void testAddEndpointToleratesUnderscoreHostname() throws Exception {
        try (Client client = new Client.Builder()
                .addEndpoint("http://host_with_underscore:8123")
                .setUsername("default")
                .setPassword("")
                .build()) {

            String firstEndpoint = extractFirstEndpointUri(client);
            Assert.assertEquals(firstEndpoint, "http://host_with_underscore:8123/",
                    "Endpoint URI should preserve original hostname");
        }
    }

    @Test(dataProvider = "testSslModeViaString_DP")
    public void testSslModeViaString(String modeStr, SSLMode mode) {
        try (Client client = new Client.Builder()
                .addEndpoint("http://localhost:8123")
                .setUsername("default")
                .setPassword("")
                .setOption(ClientConfigProperties.SSL_MODE.getKey(), modeStr)
                .build()) {
            Assert.assertEquals(client.getConfiguration().get(ClientConfigProperties.SSL_MODE.getKey()),
                    mode.name());
        }

        try (Client client = new Client.Builder()
                .addEndpoint("http://localhost:8123")
                .setUsername("default")
                .setPassword("")
                .setSSLMode(mode)
                .build()) {
            Assert.assertEquals(client.getConfiguration().get(ClientConfigProperties.SSL_MODE.getKey()),
                    mode.name());
        }
    }

    @DataProvider
    public static Object[][] testSslModeViaString_DP() {
        return new Object[][] {
                {"DISABLED", SSLMode.DISABLED},
                {"disabled", SSLMode.DISABLED},
                {"TRUST", SSLMode.TRUST},
                {"trust", SSLMode.TRUST},
                {"VERIFY_CA", SSLMode.VERIFY_CA},
                {"verify_ca", SSLMode.VERIFY_CA},
                {"STRICT", SSLMode.STRICT},
                {"strict", SSLMode.STRICT},
        };
    }


    @Test
    public void testSslModeDisabledRejectedForHttpsRegardlessOfCase() {
        // Value supplied as a raw (non-canonical case) string via setOption must still be recognized
        // as DISABLED and rejected with ClientMisconfigurationException for an https endpoint.
        for (String value : new String[] { "DISABLED", "disabled", "Disabled" }) {
            Assert.expectThrows(ClientMisconfigurationException.class, () -> new Client.Builder()
                    .addEndpoint("https://localhost:8443")
                    .setUsername("default")
                    .setPassword("")
                    .setOption(ClientConfigProperties.SSL_MODE.getKey(), value)
                    .build());
        }
    }

    @Test
    public void testSslModeInvalidValueRejected() {
        Assert.expectThrows(ClientMisconfigurationException.class, () -> new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .setOption(ClientConfigProperties.SSL_MODE.getKey(), "verify-ca")
                .build());
    }

    @Test
    public void testSetSSLContextStoredInConfiguration() throws Exception {
        SSLContext customContext = SSLContext.getInstance("TLS");
        customContext.init(null, null, null);

        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .setSSLContext(customContext)
                .build()) {
            Assert.assertSame(extractConfiguration(client).get(ClientConfigProperties.SSL_CONTEXT.getKey()),
                    customContext, "The application-supplied SSLContext should be stored in the configuration");
        }

        // Without setSSLContext the key must be absent so the client builds its own context.
        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .build()) {
            Assert.assertNull(extractConfiguration(client).get(ClientConfigProperties.SSL_CONTEXT.getKey()),
                    "No SSLContext should be stored when none is supplied");
        }
    }

    @Test
    public void testCreateSSLContextReturnsCustomContext() throws Exception {
        SSLContext customContext = SSLContext.getInstance("TLS");
        customContext.init(null, null, null);

        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .setSSLContext(customContext)
                .build()) {
            HttpAPIClientHelper helper = extractHttpClientHelper(client);
            SSLContext resolved = helper.createSSLContext(extractConfiguration(client));
            Assert.assertSame(resolved, customContext,
                    "createSSLContext must return the application-supplied context as is");
        }

        // When no custom context is configured, createSSLContext builds a context (not the custom one).
        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .build()) {
            HttpAPIClientHelper helper = extractHttpClientHelper(client);
            Map<String, Object> configWithCustom = new HashMap<>(extractConfiguration(client));
            configWithCustom.put(ClientConfigProperties.SSL_CONTEXT.getKey(), customContext);
            Assert.assertSame(helper.createSSLContext(configWithCustom), customContext,
                    "createSSLContext must honor a custom context supplied via the configuration map");
            Assert.assertNotSame(helper.createSSLContext(extractConfiguration(client)), customContext,
                    "createSSLContext must build its own context when none is supplied");
        }
    }

    private static HttpAPIClientHelper extractHttpClientHelper(Client client) throws Exception {
        Field helperField = Client.class.getDeclaredField("httpClientHelper");
        helperField.setAccessible(true);
        return (HttpAPIClientHelper) helperField.get(client);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractConfiguration(Client client) throws Exception {
        Field configField = Client.class.getDeclaredField("configuration");
        configField.setAccessible(true);
        return (Map<String, Object>) configField.get(client);
    }

    private static String extractFirstEndpointUri(Client client) throws Exception {
        Field endpointsField = Client.class.getDeclaredField("endpoints");
        endpointsField.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<com.clickhouse.client.api.transport.Endpoint> endpoints =
                (List<com.clickhouse.client.api.transport.Endpoint>) endpointsField.get(client);
        return endpoints.get(0).getURI().toString();
    }
}
