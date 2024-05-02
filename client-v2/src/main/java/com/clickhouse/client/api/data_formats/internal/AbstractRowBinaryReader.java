package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractRowBinaryReader implements ClickHouseBinaryStreamReader {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractRowBinaryReader.class);

    protected InputStream inputStream;

    protected ClickHouseInputStream chInputStream;

    protected Map<String, Object> settings;

    protected BinaryStreamReader binaryStreamReader;

    private List<Object> record;

    private TableSchema schema;

    protected volatile boolean hasNext = true;

    protected AbstractRowBinaryReader(InputStream inputStream, QuerySettings querySettings, TableSchema schema) {
        this.inputStream = inputStream;
        this.chInputStream = inputStream instanceof ClickHouseInputStream ?
                (ClickHouseInputStream) inputStream : ClickHouseInputStream.of(inputStream);
        this.settings = new HashMap<>(querySettings.getAllSettings());
        this.binaryStreamReader = new BinaryStreamReader(chInputStream, true, LOG);
        setSchema(schema);
    }

    protected Map<String, Object> currentRecord = new ConcurrentHashMap<>();

    @Override
    public <T> T readValue(int colIndex) throws IOException {
        if (colIndex < 1 || colIndex > getSchema().getColumns().size()) {
            throw new ClientException("Column index out of bounds: " + colIndex);
        }
        colIndex = colIndex - 1;
        return (T) currentRecord.get(getSchema().indexToName(colIndex));
    }

    @Override
    public <T> T readValue(String colName) throws IOException {
        return (T) currentRecord.get(colName);
    }

    @Override
    public boolean hasNext() {
        if (hasNext) {
            try {
                return chInputStream.available() > 0;
            } catch (IOException e) {
                hasNext = false;
                LOG.error("Failed to check if there is more data available", e);
                return false;
            }
        }
        return false;
    }

    protected void setSchema(TableSchema schema) {
        this.schema = schema;
    }

    public TableSchema getSchema() {
        return schema;
    }
}
