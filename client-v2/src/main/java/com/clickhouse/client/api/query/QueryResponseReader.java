package com.clickhouse.client.api.query;

import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;

/** Class to read query response. */
public class QueryResponseReader {
    final QueryResponse queryResponse;
    final RowBinaryWithNamesAndTypesFormatReader rowBinaryWithNamesAndTypesFormatReader;
    public QueryResponseReader(QueryResponse queryResponse, RowBinaryWithNamesAndTypesFormatReader rowBinaryWithNamesAndTypesFormatReader) {
        this.queryResponse = queryResponse;
        this.rowBinaryWithNamesAndTypesFormatReader = rowBinaryWithNamesAndTypesFormatReader;
    }

    public ClickHouseBinaryFormatReader getReader() {
        return rowBinaryWithNamesAndTypesFormatReader;
    }

    public QueryResponse getQueryResponse() {
        return queryResponse;
    }
}
