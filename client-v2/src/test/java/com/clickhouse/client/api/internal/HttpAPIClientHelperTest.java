package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.enums.SSLMode;
import com.clickhouse.client.api.internal.HttpAPIClientHelper.CustomSSLConnectionFactory;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.testng.annotations.Test;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class HttpAPIClientHelperTest {

    /**
     * The configured cipher suites must be forwarded to the base {@link SSLConnectionSocketFactory}, which is
     * what enables them on each secure connection. This is the core of the cipher-suite feature.
     */
    @Test
    public void testCipherSuiteConstructorForwardsSuitesToBaseFactory() throws Exception {
        String[] suites = {"TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"};
        CustomSSLConnectionFactory factory = new CustomSSLConnectionFactory(
                null, SSLContext.getDefault(), (hostname, session) -> true, suites);

        assertEquals(baseSupportedCipherSuites(factory), suites,
                "configured cipher suites must reach the base socket factory that enables them per connection");
    }

    /**
     * The three-argument constructor is retained for backward compatibility and must delegate with no cipher
     * restriction, so callers that do not configure cipher suites keep the JVM defaults.
     */
    @Test
    public void testLegacyConstructorAppliesNoCipherRestriction() throws Exception {
        CustomSSLConnectionFactory factory = new CustomSSLConnectionFactory(
                "legacy.example.com", SSLContext.getDefault(), (hostname, session) -> true);

        assertNull(baseSupportedCipherSuites(factory),
                "the legacy constructor must not restrict cipher suites");
    }

    /**
     * A configured SNI host is applied to every prepared socket via the standard SSL parameters.
     */
    @Test
    public void testConfiguredSniAppliedToPreparedSocket() throws Exception {
        CustomSSLConnectionFactory factory = new CustomSSLConnectionFactory(
                "sni.example.com", SSLContext.getDefault(), (hostname, session) -> true, null);

        SSLSocket socket = mock(SSLSocket.class);
        when(socket.getSSLParameters()).thenReturn(new SSLParameters());

        factory.prepareSocket(socket, null);

        ArgumentCaptor<SSLParameters> params = ArgumentCaptor.forClass(SSLParameters.class);
        verify(socket).setSSLParameters(params.capture());
        List<SNIServerName> serverNames = params.getValue().getServerNames();
        assertEquals(serverNames.size(), 1, "the configured SNI host must be applied to the socket");
        assertEquals(((SNIHostName) serverNames.get(0)).getAsciiName(), "sni.example.com");
    }

    /**
     * A blank SNI is treated as unset: the socket's SSL parameters must be left untouched so the defaults
     * (and any cipher suites the base factory already applied) are preserved.
     */
    @Test
    public void testBlankSniLeavesSocketParametersUntouched() throws Exception {
        CustomSSLConnectionFactory factory = new CustomSSLConnectionFactory(
                "  ", SSLContext.getDefault(), (hostname, session) -> true, null);

        SSLSocket socket = mock(SSLSocket.class);
        factory.prepareSocket(socket, null);

        verify(socket, never()).setSSLParameters(any());
    }

    /**
     * When cipher suites are configured in STRICT mode (no SNI), {@code createHttpClient} must build the
     * cipher-aware {@link CustomSSLConnectionFactory}, forward the configured suites to it, and pass a
     * {@code null} hostname verifier so the base factory keeps its default (verifying) behaviour - i.e.
     * restricting cipher suites must not silently disable hostname verification.
     */
    @Test
    public void testCreateHttpClientStrictWithCipherSuitesForwardsSuitesAndKeepsHostnameVerification() {
        Map<String, Object> config = new HashMap<>();
        config.put(ClientConfigProperties.SSL_CIPHER_SUITES.getKey(),
                Arrays.asList("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"));

        List<List<?>> calls = captureCustomFactoryConstruction(config);

        assertEquals(calls.size(), 1, "STRICT + cipher suites must build the cipher-aware custom factory");
        List<?> args = calls.get(0); // (socketSNI, sslContext, hostnameVerifier, enabledCipherSuites)
        assertNull(args.get(2), "STRICT must keep default hostname verification: the verifier passed to the "
                + "factory must be null so the base factory verifies hostnames");
        assertEquals((String[]) args.get(3), new String[]{"TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"},
                "the configured cipher suites must be forwarded to the connection socket factory");
    }

    /**
     * TRUST mode opts out of hostname verification, so the factory must receive a permissive (non-null)
     * verifier; with no cipher suites configured the factory must keep the JVM defaults (no restriction).
     */
    @Test
    public void testCreateHttpClientTrustModeInstallsPermissiveVerifierWithoutCipherRestriction() {
        Map<String, Object> config = new HashMap<>();
        config.put(ClientConfigProperties.SSL_MODE.getKey(), SSLMode.TRUST);

        List<List<?>> calls = captureCustomFactoryConstruction(config);

        assertEquals(calls.size(), 1, "TRUST mode must build the custom factory to skip hostname verification");
        List<?> args = calls.get(0);
        assertNotNull(args.get(2), "TRUST mode must install a permissive hostname verifier");
        assertNull(args.get(3), "TRUST mode without configured cipher suites must not restrict cipher suites");
    }

    /**
     * A custom SNI host will not match the server certificate, so hostname verification is skipped (a
     * permissive verifier is installed) and the configured SNI is passed through to the factory.
     */
    @Test
    public void testCreateHttpClientCustomSniInstallsPermissiveVerifier() {
        Map<String, Object> config = new HashMap<>();
        config.put(ClientConfigProperties.SSL_SOCKET_SNI.getKey(), "sni.example.com");

        List<List<?>> calls = captureCustomFactoryConstruction(config);

        assertEquals(calls.size(), 1, "a custom SNI must build the custom factory");
        List<?> args = calls.get(0);
        assertEquals(args.get(0), "sni.example.com", "the configured SNI must be passed to the factory");
        assertNotNull(args.get(2), "a custom SNI host won't match the certificate, so a permissive verifier "
                + "is installed");
    }

    /**
     * Contrast case: the default STRICT path with no SNI and no cipher restriction must be unchanged - it
     * must NOT build the custom factory, so the plain verifying {@link SSLConnectionSocketFactory} is used.
     */
    @Test
    public void testCreateHttpClientStrictWithoutSniOrCiphersUsesPlainFactory() {
        List<List<?>> calls = captureCustomFactoryConstruction(new HashMap<>());

        assertEquals(calls.size(), 0, "default STRICT with no SNI and no cipher suites must keep using the "
                + "plain SSLConnectionSocketFactory (unchanged behaviour)");
    }

    /**
     * VERIFY_CA validates the certificate chain but skips hostname verification (the connection hostname may
     * legitimately differ), so - like TRUST - it must install a permissive verifier. This pins the second
     * mode that opts out of hostname verification, distinct from the TRUST path.
     */
    @Test
    public void testCreateHttpClientVerifyCaModeInstallsPermissiveVerifier() {
        Map<String, Object> config = new HashMap<>();
        config.put(ClientConfigProperties.SSL_MODE.getKey(), SSLMode.VERIFY_CA);

        List<List<?>> calls = captureCustomFactoryConstruction(config);

        assertEquals(calls.size(), 1, "VERIFY_CA must build the custom factory to skip hostname verification");
        List<?> args = calls.get(0);
        assertNotNull(args.get(2), "VERIFY_CA must install a permissive hostname verifier");
        assertNull(args.get(3), "VERIFY_CA without configured cipher suites must not restrict cipher suites");
    }

    /**
     * SNI and cipher suites are independent concerns and must combine: a custom SNI still installs the
     * permissive verifier while the configured cipher suites are forwarded to the same factory - i.e. cipher
     * forwarding is not tied to the STRICT/cipher-triggered path.
     */
    @Test
    public void testCreateHttpClientSniAndCipherSuitesCombine() {
        Map<String, Object> config = new HashMap<>();
        config.put(ClientConfigProperties.SSL_SOCKET_SNI.getKey(), "sni.example.com");
        config.put(ClientConfigProperties.SSL_CIPHER_SUITES.getKey(),
                Arrays.asList("TLS_AES_256_GCM_SHA384"));

        List<List<?>> calls = captureCustomFactoryConstruction(config);

        assertEquals(calls.size(), 1, "SNI + cipher suites must build the custom factory");
        List<?> args = calls.get(0);
        assertEquals(args.get(0), "sni.example.com", "the configured SNI must be passed to the factory");
        assertNotNull(args.get(2), "a custom SNI installs a permissive verifier even when cipher suites are set");
        assertEquals((String[]) args.get(3), new String[]{"TLS_AES_256_GCM_SHA384"},
                "the configured cipher suites must still be forwarded when SNI is also set");
    }

    /**
     * Boundary case: an empty cipher-suite list must be treated as "no restriction" (JVM defaults), exactly
     * like an unset value - it must NOT be turned into an empty cipher array, which would enable zero suites
     * and make every handshake fail. STRICT with no SNI therefore keeps using the plain factory.
     */
    @Test
    public void testCreateHttpClientEmptyCipherSuitesTreatedAsNoRestriction() {
        Map<String, Object> config = new HashMap<>();
        config.put(ClientConfigProperties.SSL_CIPHER_SUITES.getKey(), Collections.emptyList());

        List<List<?>> calls = captureCustomFactoryConstruction(config);

        assertEquals(calls.size(), 0, "an empty cipher-suite list must be treated as no restriction (JVM "
                + "defaults), so the plain SSLConnectionSocketFactory is used");
    }

    /**
     * Builds an {@link HttpAPIClientHelper} and invokes {@link HttpAPIClientHelper#createHttpClient} with SSL
     * enabled while intercepting every {@link CustomSSLConnectionFactory} construction, returning the
     * constructor arguments of each construction (empty when the plain factory branch is taken instead).
     */
    private static List<List<?>> captureCustomFactoryConstruction(Map<String, Object> sslConfig) {
        HttpAPIClientHelper helper = new HttpAPIClientHelper(new HashMap<>(), null, false,
                LZ4Factory.fastestJavaInstance());
        List<List<?>> constructorArgs = new ArrayList<>();
        try (MockedConstruction<CustomSSLConnectionFactory> mocked = mockConstruction(
                CustomSSLConnectionFactory.class,
                (mock, context) -> constructorArgs.add(context.arguments()))) {
            helper.createHttpClient(true, sslConfig);
        }
        return constructorArgs;
    }

    private static String[] baseSupportedCipherSuites(CustomSSLConnectionFactory factory) throws Exception {
        Field field = SSLConnectionSocketFactory.class.getDeclaredField("supportedCipherSuites");
        field.setAccessible(true);
        return (String[]) field.get(factory);
    }
}
