package com.clickhouse.client.api.internal;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ConnectionInitiationException;
import org.apache.hc.client5.http.ContextBuilder;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.async.methods.SimpleRequestProducer;
import org.apache.hc.client5.http.async.methods.SimpleResponseConsumer;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.classic.AbstractHttpClientResponseHandler;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.concurrent.DefaultThreadFactory;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ConnectionClosedException;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.util.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.NoRouteToHostException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
                .setRetryStrategy(new RetryStrategy(3, TimeValue.ofSeconds(1)))
                .build();


        return httpclient;
    }


    private HttpRequest createRequest(String method, String uri, Map<String, String> headers) {

        return null;
    }

    /**
     * Reads status line and if error tries to parse response body to get server error message.
     *
     * @param statusLine
     * @return
     */
    public String readError(HttpResponse httpResponse, StatusLine statusLine) {
        if (statusLine.isError()) {
            return "Failed to get table schema: " + statusLine;
        }
        return null;
    }

    public CompletableFuture<ClassicHttpResponse> executeRequest(List<ClickHouseNode> servers,
                                                                 String sql, Map<String, Serializable> requestConfig) {
        return executeRequest(servers, new StringEntity(sql), requestConfig);
    }

    public CompletableFuture<ClassicHttpResponse> executeRequest(List<ClickHouseNode> servers,
                                                                 HttpEntity httpEntity, Map<String, Serializable> requestConfig) {

        CompletableFuture<ClassicHttpResponse> responseFuture = CompletableFuture.supplyAsync(() -> {

            for (ClickHouseNode server : servers) {

                HttpHost target = new HttpHost(server.getHost(), server.getPort());
                HttpPost req = new HttpPost(server.getBaseUri());
                RequestConfig httpReqConfig = RequestConfig.copy(baseRequestConfig)
                        .build();
                req.setConfig(httpReqConfig);
                req.setEntity(httpEntity);

                HttpClientContext context = HttpClientContext.create();

                try {
                    ClassicHttpResponse httpResponse = httpClient.execute(target, req, context, new ResponseHandler());
                    if (httpResponse.getCode() == 200) {
                        return httpResponse;
                    } else if (httpResponse.getCode() >= 400 && httpResponse.getCode() < 500) {
                        throw new ClientException("Client error: " + httpResponse.getCode() + " " + httpResponse.getReasonPhrase());
                    }
                    return httpResponse;

                } catch (UnknownHostException e) {
                    LOG.warn("Host '{}' unknown", target);
                } catch (ConnectException | NoRouteToHostException e) {
                    LOG.warn("Failed to connect to '{}': {}", target, e.getMessage());
                } catch (Exception e) {
                    throw new ClientException("Failed to execute request", e);
                }
            }
            return null;
        }, executorService);

        return responseFuture;
    }

    private static class ResponseHandler implements HttpClientResponseHandler<ClassicHttpResponse> {
        @Override
        public ClassicHttpResponse handleResponse(ClassicHttpResponse response) throws HttpException, IOException {
            return response;
        }
    }

    private static class RetryStrategy extends DefaultHttpRequestRetryStrategy {
        RetryStrategy(final int maxRetries,
                      final TimeValue interval) {
            super(maxRetries, interval, Arrays.asList(
                            InterruptedIOException.class,
                            ConnectException.class,
                            SSLException.class),
                    Arrays.asList(
                            HttpStatus.SC_BAD_GATEWAY,
                            HttpStatus.SC_SERVICE_UNAVAILABLE));
        }
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
