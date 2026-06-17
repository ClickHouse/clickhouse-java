package com.clickhouse.client.api;

import com.clickhouse.client.api.enums.SSLMode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.List;

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
                .setOption(ClientConfigProperties.SSL_MODE.getKey(), "insecure")
                .build());
    }

    @Test
    public void testSslModeNormalizedToCanonicalName() throws Exception {
        // A non-canonical case value is accepted and normalized to the canonical enum name.
        try (Client client = new Client.Builder()
                .addEndpoint("http://localhost:8123")
                .setUsername("default")
                .setPassword("")
                .setOption(ClientConfigProperties.SSL_MODE.getKey(), "trust")
                .build()) {
            Assert.assertEquals(client.getConfiguration().get(ClientConfigProperties.SSL_MODE.getKey()),
                    SSLMode.TRUST.name());
        }
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
