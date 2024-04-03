package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.query.QueryResponse;

public class RowBinary extends DataFormat {
    public RowBinary() {
        super();

    }

    public RecordReader createReader(QueryResponse<RowBinary> response) {
        return new RowBinaryReader(response.getInputStream());
    }
}
