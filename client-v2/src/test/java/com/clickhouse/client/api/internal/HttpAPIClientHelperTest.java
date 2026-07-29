package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.SSLMode;
import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.client.api.internal.HttpAPIClientHelper.CustomSSLConnectionFactory;
import com.clickhouse.client.api.transport.Endpoint;
import com.clickhouse.client.api.transport.HttpEndpoint;
import com.clickhouse.client.api.transport.internal.TransportRequest;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ConnectException;
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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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
     * restriction, so callers that do not configure cipher suites keep the transport defaults.
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
     * verifier; with no cipher suites configured the factory must keep the transport defaults (no restriction).
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
     * Boundary case: an empty cipher-suite list must be treated as "no restriction" (transport defaults), exactly
     * like an unset value - it must NOT be turned into an empty cipher array, which would enable zero suites
     * and make every handshake fail. STRICT with no SNI therefore keeps using the plain factory.
     */
    @Test
    public void testCreateHttpClientEmptyCipherSuitesTreatedAsNoRestriction() {
        Map<String, Object> config = new HashMap<>();
        config.put(ClientConfigProperties.SSL_CIPHER_SUITES.getKey(), Collections.emptyList());

        List<List<?>> calls = captureCustomFactoryConstruction(config);

        assertEquals(calls.size(), 0, "an empty cipher-suite list must be treated as no restriction "
                + "(transport defaults), so the plain SSLConnectionSocketFactory is used");
    }

    @Test
    public void testExecuteRequestThrowsConnectExceptionOn502() throws Exception {
        Map<String, Object> configuration = new HashMap<>();
        HttpAPIClientHelper helper = new HttpAPIClientHelper(configuration, null, false, LZ4Factory.fastestInstance());

        CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
        Field httpClientField = HttpAPIClientHelper.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(helper, mockHttpClient);

        ClassicHttpResponse mockResponse = mock(ClassicHttpResponse.class);
        when(mockResponse.getCode()).thenReturn(502);
        HttpEntity mockEntity = mock(HttpEntity.class);
        when(mockResponse.getEntity()).thenReturn(mockEntity);

        when(mockHttpClient.executeOpen(any(), any(), any())).thenReturn(mockResponse);

        Endpoint endpoint = new HttpEndpoint("localhost", 8123, false, "/");
        try {
            TransportRequest req = helper.createRequest(endpoint, new HashMap<>(), "SELECT 1");
            helper.executeRequest(req).close();
            Assert.fail("Expected ConnectException to be thrown");
        } catch (ConnectException e) {
            // expected
        } catch (Exception e) {
            Assert.fail("Expected ConnectException to be thrown, but got: " + e.getClass().getName(), e);
        }
    }

    @Test
    public void testExecuteRequestThrowsConnectExceptionOn503() throws Exception {
        Map<String, Object> configuration = new HashMap<>();
        HttpAPIClientHelper helper = new HttpAPIClientHelper(configuration, null, false, LZ4Factory.fastestInstance());

        CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
        Field httpClientField = HttpAPIClientHelper.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(helper, mockHttpClient);

        ClassicHttpResponse mockResponse = mock(ClassicHttpResponse.class);
        when(mockResponse.getCode()).thenReturn(503);
        HttpEntity mockEntity = mock(HttpEntity.class);
        when(mockResponse.getEntity()).thenReturn(mockEntity);

        when(mockHttpClient.executeOpen(any(), any(), any())).thenReturn(mockResponse);

        Endpoint endpoint = new HttpEndpoint("localhost", 8123, false, "/");
        try {
            TransportRequest req = helper.createRequest(endpoint, new HashMap<>(), "SELECT 1");
            helper.executeRequest(req).close();
            Assert.fail("Expected ConnectException to be thrown");
        } catch (ConnectException e) {
            // expected
        } catch (Exception e) {
            Assert.fail("Expected ConnectException to be thrown, but got: " + e.getClass().getName(), e);
        }
    }

    @DataProvider(name = "serverExceptionRetryCases")
    public static Object[][] serverExceptionRetryCases() {
        // Server code 159 (TIMEOUT_EXCEEDED) is retryable; code 60 (TABLE_NOT_FOUND) is not.
        ServerException retryable = new ServerException(159, "TIMEOUT_EXCEEDED", 500, "q1");
        ServerException nonRetryable = new ServerException(60, "TABLE_NOT_FOUND", 404, "q2");
        return new Object[][]{
                // ServerException thrown directly (behaviour that already worked; pinned as contrast).
                {retryable, true},
                {nonRetryable, false},
                // ServerException wrapped as the cause of another exception: the branch matches on the
                // cause, so the decision must come from the cause and must not throw ClassCastException.
                {new RuntimeException("transport failure", retryable), true},
                {new RuntimeException("transport failure", nonRetryable), false},
        };
    }

    /**
     * {@link HttpAPIClientHelper#shouldRetry} enters its ServerException branch when either the thrown
     * exception or its cause is a {@link ServerException}, so it must derive the retry decision from
     * whichever one is the {@code ServerException}. Casting the wrapper unconditionally throws
     * {@link ClassCastException} when only the cause is a {@code ServerException}.
     */
    @Test(dataProvider = "serverExceptionRetryCases")
    public void testShouldRetryUsesServerExceptionFromCause(Throwable ex, boolean expectedRetry) {
        HttpAPIClientHelper helper = new HttpAPIClientHelper(new HashMap<>(), null, false, LZ4Factory.fastestInstance());
        // Empty request settings -> default client_retry_on_failures, which includes ServerRetryable.
        assertEquals(helper.shouldRetry(ex, new HashMap<>()), expectedRetry);
    }

    /**
     * A server error is logged at WARN only for an unknown status code (the switch's default branch). Known
     * error paths emit no server-error WARN: readError surfaces an exception-code error, a mapped code (502)
     * throws a descriptive exception, and 200 is not an error.
     */
    @DataProvider(name = "serverErrorLogging")
    public static Object[][] serverErrorLogging() {
        return new Object[][] {
                // statusCode, exceptionCodeHeader (null => header absent), expectServerErrorWarn
                {480, null, true},    // unknown status code -> context-free ClientException -> logged
                {400, "62", false},   // known ClickHouse error (exception-code header) -> readError surfaces it
                {502, null, false},   // mapped status code -> descriptive ConnectException
                {200, null, false},   // success -> not an error
        };
    }

    @Test(dataProvider = "serverErrorLogging")
    public void testServerErrorLoggedOnlyForUnknownStatus(int statusCode, String exceptionCode,
                                                          boolean expectServerErrorWarn) throws Exception {
        HttpAPIClientHelper helper = new HttpAPIClientHelper(new HashMap<>(), null, false, LZ4Factory.fastestInstance());
        injectMockHttpClient(helper, mockResponse(statusCode, exceptionCode));

        Map<String, Object> reqConfig = new HashMap<>();
        reqConfig.put(ClientConfigProperties.QUERY_ID.getKey(), "qid-log-test");
        Endpoint endpoint = new HttpEndpoint("localhost", 8123, false, "/");

        String logged = captureStdErr(() -> {
            try {
                helper.executeRequest(helper.createRequest(endpoint, reqConfig, "SELECT 1")).close();
            } catch (Exception expected) {
                // error status codes are rethrown to the caller; we assert only on what was logged
            }
        });

        if (expectServerErrorWarn) {
            assertTrue(logged.contains("Server returned error response"),
                    "an unknown status code must be logged at WARN: " + logged);
            assertTrue(logged.contains(String.valueOf(statusCode)), "the HTTP status code must be logged: " + logged);
            assertTrue(logged.contains("qid-log-test"), "the query id must be logged: " + logged);
        } else {
            assertFalse(logged.contains("Server returned error response"),
                    "status " + statusCode + " must not emit a server-error WARN: " + logged);
        }
    }

    /**
     * The server-error logger runs on the failure path, so it must be null-safe - never throw and mask the
     * real error - and it must surface the ClickHouse exception-code header value when the response carries one.
     */
    @Test
    public void testLogServerErrorResponseIsNullSafeAndLogsExceptionCode() throws Exception {
        HttpAPIClientHelper helper = new HttpAPIClientHelper(new HashMap<>(), null, false, LZ4Factory.fastestInstance());
        Method log = HttpAPIClientHelper.class.getDeclaredMethod(
                "logServerErrorResponse", HttpPost.class, ClassicHttpResponse.class);
        log.setAccessible(true);
        HttpPost req = new HttpPost("http://localhost:8123/");
        ClassicHttpResponse responseWithCode = mockResponse(480, "241");

        // A null request or a null response must be a silent no-op: nothing logged, nothing thrown.
        assertFalse(captureStdErr(() -> invokeQuietly(log, helper, null, responseWithCode))
                .contains("Server returned error response"), "a null request must not be logged");
        assertFalse(captureStdErr(() -> invokeQuietly(log, helper, req, null))
                .contains("Server returned error response"), "a null response must not be logged");

        // A present exception-code header must be logged by value, not as the "<none>" placeholder.
        String logged = captureStdErr(() -> invokeQuietly(log, helper, req, responseWithCode));
        assertTrue(logged.contains("Server returned error response"), "the server error must be logged: " + logged);
        assertTrue(logged.contains("241"), "the exception-code header value must be logged: " + logged);
        assertFalse(logged.contains("<none>"), "a present exception-code header must not log the placeholder: " + logged);
    }

    private static void invokeQuietly(Method method, Object target, Object... args) {
        try {
            method.invoke(target, args);
        } catch (Exception e) {
            throw new AssertionError("logServerErrorResponse must not throw on the error path", e);
        }
    }

    private static ClassicHttpResponse mockResponse(int statusCode, String serverExceptionCode) {
        ClassicHttpResponse response = mock(ClassicHttpResponse.class);
        when(response.getCode()).thenReturn(statusCode);
        boolean hasExceptionHeader = serverExceptionCode != null;
        when(response.containsHeader(ClickHouseHttpProto.HEADER_EXCEPTION_CODE)).thenReturn(hasExceptionHeader);
        when(response.getFirstHeader(ClickHouseHttpProto.HEADER_EXCEPTION_CODE))
                .thenReturn(hasExceptionHeader
                        ? new BasicHeader(ClickHouseHttpProto.HEADER_EXCEPTION_CODE, serverExceptionCode) : null);
        when(response.getEntity()).thenReturn(mock(HttpEntity.class));
        return response;
    }

    private static void injectMockHttpClient(HttpAPIClientHelper helper, ClassicHttpResponse response) throws Exception {
        CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
        when(mockHttpClient.executeOpen(any(), any(), any())).thenReturn(response);
        Field httpClientField = HttpAPIClientHelper.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(helper, mockHttpClient);
    }

    /**
     * Captures everything written to {@code System.err} (the slf4j-simple target) while {@code action}
     * runs. slf4j-simple resolves {@code System.err} dynamically per log call, so the temporary swap
     * reliably captures WARN/INFO output emitted during the action.
     */
    private static String captureStdErr(Runnable action) {
        PrintStream original = System.err;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintStream capture;
        try {
            capture = new PrintStream(buf, true, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        System.setErr(capture);
        try {
            action.run();
        } finally {
            capture.flush();
            System.setErr(original);
        }
        try {
            return buf.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
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
