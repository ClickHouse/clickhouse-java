package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.transport.Endpoint;
import com.clickhouse.client.api.transport.HttpEndpoint;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpAPIClientHelperTest {

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
            helper.executeRequest(endpoint, new HashMap<>(), "SELECT 1");
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
            helper.executeRequest(endpoint, new HashMap<>(), "SELECT 1");
            Assert.fail("Expected ConnectException to be thrown");
        } catch (ConnectException e) {
            // expected
        } catch (Exception e) {
            Assert.fail("Expected ConnectException to be thrown, but got: " + e.getClass().getName(), e);
        }
    }
}