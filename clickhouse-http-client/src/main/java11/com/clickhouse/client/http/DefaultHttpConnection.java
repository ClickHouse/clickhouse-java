package com.clickhouse.client.http;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.data.ClickHousePipedStream;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.SSLContext;

public class DefaultHttpConnection extends ClickHouseHttpConnection {
    private static final Logger log = LoggerFactory.getLogger(DefaultHttpConnection.class);

    private final HttpClient httpClient;

    private ClickHouseHttpResponse buildResponse(HttpResponse<InputStream> r) throws IOException {
        HttpHeaders headers = r.headers();
        String displayName = headers.firstValue("X-ClickHouse-Server-Display-Name").orElse(server.getHost());
        String queryId = headers.firstValue("X-ClickHouse-Query-Id").orElse("");
        String summary = headers.firstValue("X-ClickHouse-Summary").orElse("{}");

        ClickHouseFormat format = config.getFormat();
        TimeZone timeZone = config.getServerTimeZone();
        // queryId, format and timeZone are only available for queries
        if (!ClickHouseChecker.isNullOrEmpty(queryId)) {
            String value = headers.firstValue("X-ClickHouse-Format").orElse("");
            format = !ClickHouseChecker.isNullOrEmpty(value) ? ClickHouseFormat.valueOf(value)
                    : format;
            value = headers.firstValue("X-ClickHouse-Timezone").orElse("");
            timeZone = !ClickHouseChecker.isNullOrEmpty(value) ? TimeZone.getTimeZone(value)
                    : timeZone;
        }

        return new ClickHouseHttpResponse(this, getResponseInputStream(checkResponse(r).body()),
                displayName, queryId, summary, format, timeZone);
    }

    private HttpResponse<InputStream> checkResponse(HttpResponse<InputStream> r) throws IOException {
        if (r.statusCode() != HttpURLConnection.HTTP_OK) {
            // TODO get exception from response header, for example:
            // X-ClickHouse-Exception-Code: 47
            StringBuilder builder = new StringBuilder();
            try (Reader reader = new BufferedReader(
                    new InputStreamReader(getResponseInputStream(r.body()), StandardCharsets.UTF_8))) {
                int c = 0;
                while ((c = reader.read()) != -1) {
                    builder.append((char) c);
                }
            } catch (IOException e) {
                log.warn("Error while reading error message", e);
            }

            throw new IOException(builder.toString());
        }

        return r;
    }

    private HttpRequest newRequest(String url) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(config.getSocketTimeout())).build();
    }

    protected DefaultHttpConnection(ClickHouseNode server, ClickHouseRequest<?> request) throws IOException {
        super(server, request);

        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(config.getConnectionTimeout()))
                .followRedirects(Redirect.ALWAYS)
                .version(Version.HTTP_1_1);
        if (config.isAsync()) {
            builder.executor(ClickHouseClient.getExecutorService());
        }
        if (config.isSsl()) {
            builder.sslContext(ClickHouseSslContextProvider.getProvider().getSslContext(SSLContext.class, config)
                    .orElse(null));
        }

        httpClient = builder.build();
    }

    @Override
    protected boolean isReusable() {
        return true;
    }

    private ClickHouseHttpResponse postStream(HttpRequest.Builder reqBuilder, String boundary, String sql,
            InputStream data, List<ClickHouseExternalTable> tables) throws IOException {
        ClickHousePipedStream stream = new ClickHousePipedStream(config.getMaxBufferSize(),
                config.getMaxQueuedBuffers(), config.getSocketTimeout());
        reqBuilder.POST(HttpRequest.BodyPublishers.ofInputStream(stream::getInput));
        // running in async is necessary to avoid deadlock of the piped stream
        CompletableFuture<HttpResponse<InputStream>> f = httpClient.sendAsync(reqBuilder.build(),
                responseInfo -> new ClickHouseResponseHandler(config.getMaxBufferSize(),
                        config.getSocketTimeout()));
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8))) {
            if (boundary != null) {
                String line = "\r\n--" + boundary + "\r\n";
                writer.write(line);
                writer.write("Content-Disposition: form-data; name=\"query\"\r\n\r\n");
                writer.write(sql);

                for (ClickHouseExternalTable t : tables) {
                    String tableName = t.getName();
                    StringBuilder builder = new StringBuilder();
                    builder.append(line).append("Content-Disposition: form-data; name=\"").append(tableName)
                            .append("_format\"\r\n\r\n").append(t.getFormat().name());
                    builder.append(line).append("Content-Disposition: form-data; name=\"").append(tableName)
                            .append("_structure\"\r\n\r\n").append(t.getStructure());
                    builder.append(line).append("Content-Disposition: form-data; name=\"").append(tableName)
                            .append("\"; filename=\"").append(tableName).append("\"\r\n")
                            .append("Content-Type: application/octet-stream\r\n")
                            .append("Content-Transfer-Encoding: binary\r\n\r\n");
                    writer.write(builder.toString());
                    writer.flush();

                    pipe(t.getContent(), stream, DEFAULT_BUFFER_SIZE);
                }

                writer.write("\r\n--" + boundary + "--\r\n");
                writer.flush();
            } else {
                writer.write(sql);
                writer.flush();

                if (data.available() > 0) {
                    // append \n
                    if (sql.charAt(sql.length() - 1) != '\n') {
                        stream.write(10);
                    }

                    pipe(data, stream, DEFAULT_BUFFER_SIZE);
                }
            }
        }

        HttpResponse<InputStream> r;
        try {
            r = f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread was interrupted when posting request or receiving response", e);
        } catch (ExecutionException e) {
            throw new IOException("Failed to post request", e);
        }

        return buildResponse(r);
    }

    private ClickHouseHttpResponse postString(HttpRequest.Builder reqBuilder, String sql) throws IOException {
        reqBuilder.POST(HttpRequest.BodyPublishers.ofString(sql));
        HttpResponse<InputStream> r;
        try {
            CompletableFuture<HttpResponse<InputStream>> f = httpClient.sendAsync(reqBuilder.build(),
                    responseInfo -> new ClickHouseResponseHandler(config.getMaxBufferSize(),
                            config.getSocketTimeout()));
            r = f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread was interrupted when posting request or receiving response", e);
        } catch (ExecutionException e) {
            throw new IOException("Failed to post query", e);
        }
        return buildResponse(r);
    }

    @Override
    protected ClickHouseHttpResponse post(String sql, InputStream data, List<ClickHouseExternalTable> tables,
            Map<String, String> headers) throws IOException {
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(config.getSocketTimeout()));
        String boundary = null;
        if (tables != null && !tables.isEmpty()) {
            boundary = UUID.randomUUID().toString();
            reqBuilder.setHeader("Content-Type", "multipart/form-data; boundary=" + boundary);
        } else {
            reqBuilder.setHeader("Content-Type", "text/plain; charset=UTF-8");
        }

        headers = mergeHeaders(headers);
        if (headers != null && !headers.isEmpty()) {
            for (Entry<String, String> header : headers.entrySet()) {
                reqBuilder.setHeader(header.getKey(), header.getValue());
            }
        }

        return boundary != null || data != null ? postStream(reqBuilder, boundary, sql, data, tables)
                : postString(reqBuilder, sql);
    }

    @Override
    public boolean ping(int timeout) {
        String response = (String) config.getOption(ClickHouseHttpOption.DEFAULT_RESPONSE);
        try {
            HttpResponse<String> r = httpClient.send(newRequest(getBaseUrl() + "ping"),
                    HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() != HttpURLConnection.HTTP_OK) {
                throw new IOException(r.body());
            }

            return response.equals(r.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.debug("Failed to ping server: ", e.getMessage());
        }

        return false;
    }

    @Override
    public void close() {
    }
}
