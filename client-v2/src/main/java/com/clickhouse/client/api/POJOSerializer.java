package com.clickhouse.client.api;

import com.clickhouse.data.ClickHouseColumn;

import java.io.InputStream;
import java.util.List;

public interface POJOSerializer {
    InputStream serialize(Object obj, List<ClickHouseColumn> columns);
}
