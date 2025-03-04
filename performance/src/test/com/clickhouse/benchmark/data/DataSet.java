package com.clickhouse.benchmark.data;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface DataSet {
    public String getName();
    public String getTableName();
    public int getSize();

    public String getCreateTableString();

    public TableSchema getSchema();

    public List<ClickHouseFormat> supportedFormats();

    public InputStream getInputStream(ClickHouseFormat format);
    public InputStream getInputStream(int rowId, ClickHouseFormat format);
    public List<byte[]> getBytesList(ClickHouseFormat format);
    public List<Map<String, Object>> getRows();
}
