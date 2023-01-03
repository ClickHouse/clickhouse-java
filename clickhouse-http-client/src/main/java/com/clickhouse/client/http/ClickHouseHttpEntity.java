package com.clickhouse.client.http;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseOutputStream;
import org.apache.hc.core5.http.io.entity.AbstractHttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Used to encapsulate post request.
 */
public class ClickHouseHttpEntity extends AbstractHttpEntity {
    /**
     * Data to send
     */
    private final ClickHouseInputStream in;
    private final ClickHouseConfig config;
    /**
     * Indicate that there is extra data which comes from file.
     */
    private final boolean hasFile;
    /**
     * Indicate that there is extra data which comes from external tables.
     */
    private final boolean hasInput;

    public ClickHouseHttpEntity(ClickHouseInputStream in, ClickHouseConfig config, String contentType,
            String contentEncoding, boolean hasFile, boolean hasInput) {
        super(contentType, contentEncoding, hasInput);
        this.in = in;
        this.config = config;
        this.hasFile = hasFile;
        this.hasInput = hasInput;
    }

    @Override
    public boolean isRepeatable() {
        return false;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return in;
    }

    @Override
    public void writeTo(OutputStream outStream) throws IOException {
        Objects.requireNonNull(outStream, "outStream");
        try {
            ClickHouseOutputStream wrappedOut = hasFile
                    ? ClickHouseOutputStream.of(outStream, config.getWriteBufferSize())
                    : (hasInput
                            ? ClickHouseClient.getAsyncRequestOutputStream(config, outStream, null)
                            : ClickHouseClient.getRequestOutputStream(config, outStream, null));
            in.pipe(wrappedOut);
            wrappedOut.flush();
        } finally {
            in.close();
        }
    }

    @Override
    public boolean isStreaming() {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}
