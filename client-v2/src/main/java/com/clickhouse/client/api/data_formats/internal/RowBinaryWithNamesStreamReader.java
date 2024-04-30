package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.query.QuerySettings;

import java.io.InputStream;

public class RowBinaryWithNamesStreamReader extends AbstractRowBinaryReader {

    public RowBinaryWithNamesStreamReader(InputStream inputStream, QuerySettings querySettings) {
        super(inputStream, querySettings);
    }
}
