package com.clickhouse.client.api.internal;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import org.apache.hc.client5.http.ContextBuilder;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.async.methods.SimpleRequestProducer;
import org.apache.hc.client5.http.async.methods.SimpleResponseConsumer;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.message.StatusLine;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class HttpAPIClientHelper {

    private CloseableHttpAsyncClient httpClient;

    private Map<String, String> chConfiguration;


    public HttpAPIClientHelper(Map<String, String> configuration) {
        this.chConfiguration = configuration;
        this.httpClient = createHttpClient(configuration, null);


    }

    public CloseableHttpAsyncClient createHttpClient(Map<String, String> chConfig, Map<String, Serializable> requestConfig) {
        final CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();
        httpclient.start();

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

    public CompletableFuture<SimpleHttpResponse> executeRequest(ClickHouseNode server,
                                                                Map<String, Serializable> requestConfig) {

//        HttpHost httpHost = HttpHost.create(server.toUri());
        SimpleRequestBuilder reqBuilder = SimpleRequestBuilder.post(server.toUri()) // host to determine if secure or not
                .setPath("/");
        addHeaders(reqBuilder, chConfiguration, requestConfig);
        addQueryParams(reqBuilder, chConfiguration, requestConfig);

        final HttpClientContext localContext = ContextBuilder.create()
                .useCredentialsProvider((authScope, context) -> {
                    ClickHouseCredentials credentials = server.getCredentials().orElse(null);
                    return credentials == null ? null :
                        new UsernamePasswordCredentials(credentials.getUserName(), credentials.getPassword().toCharArray());
                })
                .build();

        CompletableFuture<SimpleHttpResponse> responseFuture = new CompletableFuture<>();
        httpClient.execute(
                SimpleRequestProducer.create(reqBuilder.build()),
                SimpleResponseConsumer.create(),
                localContext,
                new FutureCallback<SimpleHttpResponse>() {

                    @Override
                    public void completed(final SimpleHttpResponse response) {
                        responseFuture.complete(response);
                    }

                    @Override
                    public void failed(final Exception ex) {
                        responseFuture.completeExceptionally(ex);
                    }

                    @Override
                    public void cancelled() {
                        responseFuture.cancel(true);
                    }
                }
        );

        return responseFuture;
    }

    private SimpleRequestBuilder addHeaders(SimpleRequestBuilder requestBuilder, Map<String, String> chConfig, Map<String, Serializable> requestConfig) {
        requestBuilder.addHeader("Content-Type", "text/plain");
        return requestBuilder;
    }

    private SimpleRequestBuilder addQueryParams(SimpleRequestBuilder requestBuilder, Map<String, String> chConfig, Map<String, Serializable> requestConfig) {
        return requestBuilder;
    }
}
