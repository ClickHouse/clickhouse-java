package com.clickhouse.client.api;

import com.clickhouse.client.api.enums.SSLMode;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
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
    public void testSetSSLCipherSuitesStoredInConfiguration() throws Exception {
        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .setSSLCipherSuites("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256")
                .build()) {
            Assert.assertEquals(extractConfiguration(client).get(ClientConfigProperties.SSL_CIPHER_SUITES.getKey()),
                    Arrays.asList("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"),
                    "Cipher suites set via the builder should be stored as a parsed list");
        }
    }

    @Test
    public void testSSLCipherSuitesViaSetOptionParsedAsList() throws Exception {
        // The comma-separated string form is the path used by URL/JDBC properties.
        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .setOption(ClientConfigProperties.SSL_CIPHER_SUITES.getKey(),
                        "TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256")
                .build()) {
            Assert.assertEquals(extractConfiguration(client).get(ClientConfigProperties.SSL_CIPHER_SUITES.getKey()),
                    Arrays.asList("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"),
                    "Comma-separated cipher suites should be parsed into a list");
        }
    }

    @Test
    public void testClientBuildsWithCipherSuitesOverHttps() {
        // Exercises the HTTPS connection-socket-factory path with cipher suites configured (STRICT mode,
        // no SNI): the client must build without error.
        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:8443")
                .setUsername("default")
                .setPassword("")
                .setSSLCipherSuites("TLS_AES_256_GCM_SHA384")
                .build()) {
            Assert.assertNotNull(client);
        }
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
