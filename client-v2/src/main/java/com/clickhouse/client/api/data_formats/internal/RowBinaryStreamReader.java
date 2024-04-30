package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.query.QuerySettings;

import java.io.InputStream;

public class RowBinaryStreamReader extends AbstractRowBinaryReader {
    public RowBinaryStreamReader(InputStream inputStream, QuerySettings querySettings) {
        super(inputStream, querySettings);
    }
}
