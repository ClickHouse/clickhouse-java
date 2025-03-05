package com.clickhouse.benchmark.data;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface DataSet {
    String getName();
    String getTableName();
    int getSize();

    String getCreateTableString();
    String getTrucateTableString();
    ClickHouseFormat getFormat();

    TableSchema getSchema();

    List<ClickHouseFormat> supportedFormats();

    default InputStream getInputStream(ClickHouseFormat format) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (byte[] bytes : getBytesList(format)) {
                bos.write(bytes);
            }
            return new ByteArrayInputStream(bos.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    default InputStream getInputStream(int rowId, ClickHouseFormat format) {
        return new ByteArrayInputStream(getBytesList(format).get(rowId));
    }

    List<byte[]> getBytesList(ClickHouseFormat format);

    List<Map<String, Object>> getRows();
}
