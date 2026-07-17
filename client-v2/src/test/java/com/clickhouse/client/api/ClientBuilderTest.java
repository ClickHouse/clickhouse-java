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
    public void testStringSSLContextRejectedBySetOption() {
        Assert.expectThrows(ClientMisconfigurationException.class,
                () -> new Client.Builder().setOption(ClientConfigProperties.SSL_CONTEXT.getKey(), "not-a-context"));
    }

    @DataProvider(name = "sslMaterialWithCustomContext_DP")
    public static Object[][] sslMaterialWithCustomContext_DP() {
        return new Object[][] {
                { ClientConfigProperties.SSL_TRUST_STORE.getKey(), "/path/to/truststore.jks" },
                { ClientConfigProperties.SSL_KEYSTORE_TYPE.getKey(), "JKS" },
                { ClientConfigProperties.SSL_KEY_STORE_PASSWORD.getKey(), "secret" },
                { ClientConfigProperties.SSL_KEY.getKey(), "/path/to/client.key" },
                { ClientConfigProperties.CA_CERTIFICATE.getKey(), "/path/to/ca.crt" },
                { ClientConfigProperties.SSL_CERTIFICATE.getKey(), "/path/to/client.crt" },
        };
    }

    @Test(dataProvider = "sslMaterialWithCustomContext_DP")
    public void testCustomSSLContextRejectsOtherSslMaterial(String materialKey, String materialValue) throws Exception {
        SSLContext customContext = SSLContext.getInstance("TLS");
        customContext.init(null, null, null);

        Assert.expectThrows(ClientMisconfigurationException.class, () -> new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .setOption(materialKey, materialValue)
                .setSSLContext(customContext)
                .build());
    }

    @Test
    public void testCustomSSLContextAllowsSslMode() throws Exception {
        SSLContext customContext = SSLContext.getInstance("TLS");
        customContext.init(null, null, null);

        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .setSSLMode(SSLMode.VERIFY_CA)
                .setSSLContext(customContext)
                .build()) {
            Assert.assertSame(extractConfiguration(client).get(ClientConfigProperties.SSL_CONTEXT.getKey()),
                    customContext);
            Assert.assertEquals(client.getConfiguration().get(ClientConfigProperties.SSL_MODE.getKey()),
                    SSLMode.VERIFY_CA.name());
        }
    }

    @Test
    public void testTrustStoreAndClientCertificateConflictRejectedWithoutCustomContext() {
        // Contrast: without a custom SSLContext the trust-store/certificate conflict is still rejected.
        Assert.expectThrows(ClientMisconfigurationException.class, () -> new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .setSSLTrustStore("/path/to/truststore.jks")
                .setClientCertificate("client.crt")
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
    public void testCreateSSLContextIgnoresCustomContext() throws Exception {
        SSLContext customContext = SSLContext.getInstance("TLS");
        customContext.init(null, null, null);

        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .build()) {
            HttpAPIClientHelper helper = extractHttpClientHelper(client);
            Map<String, Object> configWithCustom = new HashMap<>(extractConfiguration(client));
            configWithCustom.put(ClientConfigProperties.SSL_CONTEXT.getKey(), customContext);
            // createSSLContext only builds from trust/key material; selecting a custom context is
            // createHttpClient's responsibility, so this must not return the supplied context.
            Assert.assertNotSame(helper.createSSLContext(configWithCustom), customContext);
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
