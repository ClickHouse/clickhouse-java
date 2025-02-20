package com.clickhouse.client.http;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseExternalTable;

import org.apache.hc.core5.http.io.entity.AbstractHttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Used to encapsulate post request.
 */
@Deprecated
public class ClickHouseHttpEntity extends AbstractHttpEntity {
    private final ClickHouseConfig config;
    private final byte[] boundary;
    private final String sql;
    private final ClickHouseInputStream data;
    private final List<ClickHouseExternalTable> tables;

    protected ClickHouseHttpEntity(ClickHouseConfig config, String contentType, String contentEncoding, byte[] boundary,
            String sql, ClickHouseInputStream data, List<ClickHouseExternalTable> tables) {
        super(contentType, contentEncoding, data != null || boundary != null);

        this.config = config;
        this.boundary = boundary;
        this.sql = sql;
        this.data = data;
        this.tables = tables;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return ClickHouseInputStream.empty();
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public boolean isRepeatable() {
        return false;
    }

    @Override
    public boolean isStreaming() {
        return false;
    }

    @Override
    public void writeTo(OutputStream outStream) throws IOException {
        ClickHouseHttpConnection.postData(config, boundary, sql, data, tables, outStream);
    }

    @Override
    public void close() throws IOException {
        // nothing to do
    }
}
