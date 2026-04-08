package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import org.apache.hc.core5.http.HttpHeaders;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class CredentialsManagerTest {
    private static final String AUTHORIZATION_HEADER_KEY =
            ClientConfigProperties.httpHeader(HttpHeaders.AUTHORIZATION);

    @DataProvider(name = "conflictingAuthConfig")
    public Object[][] conflictingAuthConfig() {
        return new Object[][]{
                {ClientConfigProperties.ACCESS_TOKEN.getKey(), "token"},
                {ClientConfigProperties.PASSWORD.getKey(), "password"}
        };
    }

    @Test(groups = {"unit"})
    public void testValidateAuthConfigRejectsMissingAuthenticationConfiguration() {
        ClientMisconfigurationException exception = Assert.expectThrows(ClientMisconfigurationException.class,
                () -> CredentialsManager.validateAuthConfig(new HashMap<>()));

        Assert.assertTrue(exception.getMessage().contains("required"));
    }

    @Test(groups = {"unit"}, dataProvider = "conflictingAuthConfig")
    public void testValidateAuthConfigRejectsSslAuthCombinedWithAnotherCredential(String conflictingKey,
                                                                                   String conflictingValue) {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ClientConfigProperties.SSL_AUTH.getKey(), Boolean.TRUE.toString());
        configuration.put(conflictingKey, conflictingValue);

        ClientMisconfigurationException exception = Assert.expectThrows(ClientMisconfigurationException.class,
                () -> CredentialsManager.validateAuthConfig(configuration));

        Assert.assertEquals(exception.getMessage(),
                "Only one of password, access token or SSL authentication can be used per client.");
    }

    @Test(groups = {"unit"})
    public void testValidateAuthConfigRejectsSslAuthWithoutCertificate() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ClientConfigProperties.SSL_AUTH.getKey(), Boolean.TRUE.toString());

        ClientMisconfigurationException exception = Assert.expectThrows(ClientMisconfigurationException.class,
                () -> CredentialsManager.validateAuthConfig(configuration));

        Assert.assertEquals(exception.getMessage(), "SSL authentication requires a client certificate");
    }

    @Test(groups = {"unit"})
    public void testValidateAuthConfigRejectsTrustStoreAndCertificateTogether() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ClientConfigProperties.USER.getKey(), "user");
        configuration.put(ClientConfigProperties.SSL_TRUST_STORE.getKey(), "trust-store.jks");
        configuration.put(ClientConfigProperties.SSL_CERTIFICATE.getKey(), "client-cert.pem");

        ClientMisconfigurationException exception = Assert.expectThrows(ClientMisconfigurationException.class,
                () -> CredentialsManager.validateAuthConfig(configuration));

        Assert.assertEquals(exception.getMessage(), "Trust store and certificates cannot be used together");
    }

    @Test(groups = {"unit"})
    public void testConstructorReadsInitialCredentialsWithoutChangingSourceConfiguration() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ClientConfigProperties.USER.getKey(), "user");
        configuration.put(ClientConfigProperties.PASSWORD.getKey(), "password");

        CredentialsManager credentialsManager = new CredentialsManager(configuration);

        Map<String, Object> snapshot = credentialsManager.snapshot();
        Assert.assertEquals(snapshot.get(ClientConfigProperties.USER.getKey()), "user");
        Assert.assertEquals(snapshot.get(ClientConfigProperties.PASSWORD.getKey()), "password");
        Assert.assertEquals(configuration.get(ClientConfigProperties.USER.getKey()), "user");
        Assert.assertEquals(configuration.get(ClientConfigProperties.PASSWORD.getKey()), "password");
    }

    @Test(groups = {"unit"})
    public void testConstructorMaterializesAccessTokenAsAuthorizationHeader() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ClientConfigProperties.ACCESS_TOKEN.getKey(), "token");

        CredentialsManager credentialsManager = new CredentialsManager(configuration);

        Map<String, Object> snapshot = credentialsManager.snapshot();
        Assert.assertEquals(snapshot.get(ClientConfigProperties.ACCESS_TOKEN.getKey()), "token");
        Assert.assertEquals(snapshot.get(AUTHORIZATION_HEADER_KEY), "Bearer token");
        Assert.assertEquals(configuration.get(ClientConfigProperties.ACCESS_TOKEN.getKey()), "token");
    }

    @Test(groups = {"unit"})
    public void testConstructorReadsSslAuthFlagFromStringConfiguration() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ClientConfigProperties.USER.getKey(), "user");
        configuration.put(ClientConfigProperties.SSL_AUTH.getKey(), Boolean.TRUE.toString());
        configuration.put(ClientConfigProperties.SSL_CERTIFICATE.getKey(), "--- certificate goes here --");

        CredentialsManager credentialsManager = new CredentialsManager(configuration);

        Map<String, Object> snapshot = credentialsManager.snapshot();
        Assert.assertEquals(snapshot.get(ClientConfigProperties.USER.getKey()), "user");
        Assert.assertEquals(snapshot.get(ClientConfigProperties.SSL_AUTH.getKey()), Boolean.TRUE);
    }

    @Test(groups = {"unit"})
    public void testApplyCredentialsDoesNotResetCallerProvidedTargetEntries() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ClientConfigProperties.USER.getKey(), "user");
        configuration.put(ClientConfigProperties.PASSWORD.getKey(), "password");

        CredentialsManager credentialsManager = new CredentialsManager(configuration);
        Map<String, Object> requestSettings = new HashMap<>();
        requestSettings.put(ClientConfigProperties.ACCESS_TOKEN.getKey(), "old-token");
        requestSettings.put(AUTHORIZATION_HEADER_KEY, "Bearer old-token");
        requestSettings.put(ClientConfigProperties.SSL_AUTH.getKey(), Boolean.TRUE);

        credentialsManager.applyCredentials(requestSettings);

        Assert.assertEquals(requestSettings.get(ClientConfigProperties.USER.getKey()), "user");
        Assert.assertEquals(requestSettings.get(ClientConfigProperties.PASSWORD.getKey()), "password");
        Assert.assertEquals(requestSettings.get(ClientConfigProperties.ACCESS_TOKEN.getKey()), "old-token");
        Assert.assertEquals(requestSettings.get(AUTHORIZATION_HEADER_KEY), "Bearer old-token");
        Assert.assertEquals(requestSettings.get(ClientConfigProperties.SSL_AUTH.getKey()), Boolean.TRUE);
    }

    @Test(groups = {"unit"})
    public void testSetAccessTokenClearsUsernameAndPasswordCredentials() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ClientConfigProperties.USER.getKey(), "user");
        configuration.put(ClientConfigProperties.PASSWORD.getKey(), "password");

        CredentialsManager credentialsManager = new CredentialsManager(configuration);
        credentialsManager.setAccessToken("token");

        Map<String, Object> snapshot = credentialsManager.snapshot();
        Assert.assertEquals(snapshot.get(ClientConfigProperties.ACCESS_TOKEN.getKey()), "token");
        Assert.assertEquals(snapshot.get(AUTHORIZATION_HEADER_KEY), "Bearer token");
        Assert.assertFalse(snapshot.containsKey(ClientConfigProperties.USER.getKey()));
        Assert.assertFalse(snapshot.containsKey(ClientConfigProperties.PASSWORD.getKey()));
    }
}
