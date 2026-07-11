package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.internal.HttpAPIClientHelper.CustomSSLConnectionFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import java.lang.reflect.Field;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
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

    private static String[] baseSupportedCipherSuites(CustomSSLConnectionFactory factory) throws Exception {
        Field field = SSLConnectionSocketFactory.class.getDeclaredField("supportedCipherSuites");
        field.setAccessible(true);
        return (String[]) field.get(factory);
    }
}
