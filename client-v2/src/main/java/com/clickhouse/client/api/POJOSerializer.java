package com.clickhouse.client.api;

import com.clickhouse.data.ClickHouseColumn;

import java.io.OutputStream;
import java.util.List;

public interface POJOSerializer {
    void serialize(Object obj, OutputStream outputStream, List<ClickHouseColumn> columns);
}
