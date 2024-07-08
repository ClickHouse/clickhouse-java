package com.clickhouse.client.api.internal;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ServerException;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.concurrent.DefaultThreadFactory;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.ProtocolException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HttpAPIClientHelper {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private CloseableHttpClient httpClient;

    private Map<String, String> chConfiguration;

    private ExecutorService executorService;

    private RequestConfig baseRequestConfig;

    public HttpAPIClientHelper(Map<String, String> configuration) {
        this.chConfiguration = configuration;
        this.httpClient = createHttpClient(configuration, null);
        this.baseRequestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(1000, TimeUnit.MILLISECONDS)
                .build();
        this.executorService = Executors.newCachedThreadPool(new DefaultThreadFactory("clickhouse-client"));
    }

    public CloseableHttpClient createHttpClient(Map<String, String> chConfig, Map<String, Serializable> requestConfig) {
        final CloseableHttpClient httpclient = HttpClientBuilder.create()

                .build();


        return httpclient;
    }

    /**
     * Reads status line and if error tries to parse response body to get server error message.
     *
     * @param httpResponse - HTTP response
     * @return
     */
    public Exception readError(ClassicHttpResponse httpResponse) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(8192)) {
            httpResponse.getEntity().writeTo(out);
            String message = new String(out.toByteArray(), StandardCharsets.UTF_8);
            int serverCode = httpResponse.getFirstHeader("X-ClickHouse-Exception-Code") != null
                    ? Integer.parseInt(httpResponse.getFirstHeader("X-ClickHouse-Exception-Code").getValue())
                    : 0;
            return new ServerException(serverCode, message);
        } catch (IOException e) {
            throw new ClientException("Failed to read response body", e);
        }
    }

    public CompletableFuture<ClassicHttpResponse> executeRequest(ClickHouseNode server,
                                                                 String sql) {
        return executeRequest(server, new StringEntity(sql), null);
    }

    public CompletableFuture<ClassicHttpResponse> executeRequest(ClickHouseNode server,
                                                                 HttpEntity httpEntity, Map<String, Serializable> requestConfig) {

        CompletableFuture<ClassicHttpResponse> responseFuture = CompletableFuture.supplyAsync(() -> {


            HttpHost target = new HttpHost(server.getHost(), server.getPort());
            HttpPost req = new HttpPost(server.getBaseUri());
            RequestConfig httpReqConfig = RequestConfig.copy(baseRequestConfig)
                    .build();
            req.setConfig(httpReqConfig);
            req.setEntity(httpEntity);

            HttpClientContext context = HttpClientContext.create();

            try {
                ClassicHttpResponse httpResponse = httpClient.executeOpen(target, req, context);
                if (httpResponse.getCode() >= 400 && httpResponse.getCode() < 500) {
                    try {
                        throw readError(httpResponse);
                    } finally {
                        httpResponse.close();
                    }
                } else if (httpResponse.getCode() >= 500) {
                    httpResponse.close();
                    return httpResponse;
                }
                return httpResponse;

            } catch (UnknownHostException e) {
                LOG.warn("Host '{}' unknown", target);
            } catch (ConnectException | NoRouteToHostException e) {
                LOG.warn("Failed to connect to '{}': {}", target, e.getMessage());
            } catch (ServerException e) {
                throw e;
            } catch (Exception e) {
                throw new ClientException("Failed to execute request", e);
            }

            return null;
        }, executorService);

        return responseFuture;
    }

    private SimpleRequestBuilder addHeaders(SimpleRequestBuilder requestBuilder, Map<String, String> chConfig, Map<String, Serializable> requestConfig) {
        requestBuilder.addHeader("Content-Type", "text/plain");
        requestBuilder.addHeader("Accept", "text/plain");
        return requestBuilder;
    }

    private SimpleRequestBuilder addQueryParams(SimpleRequestBuilder requestBuilder, Map<String, String> chConfig, Map<String, Serializable> requestConfig) {
        return requestBuilder;
    }
}
