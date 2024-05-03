package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractBinaryFormatReader implements ClickHouseBinaryFormatReader {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBinaryFormatReader.class);

    protected InputStream inputStream;

    protected ClickHouseInputStream chInputStream;

    protected Map<String, Object> settings;

    protected BinaryStreamReader binaryStreamReader;

    private List<Object> record;

    private TableSchema schema;

    protected volatile boolean hasNext = true;

    protected AbstractBinaryFormatReader(InputStream inputStream, QuerySettings querySettings, TableSchema schema) {
        this.inputStream = inputStream;
        this.chInputStream = inputStream instanceof ClickHouseInputStream ?
                (ClickHouseInputStream) inputStream : ClickHouseInputStream.of(inputStream);
        this.settings = new HashMap<>(querySettings.getAllSettings());
        this.binaryStreamReader = new BinaryStreamReader(chInputStream, LOG);
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
    public <T> T readValue(String colName) {
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

    @Override
    public String asString(String colName) {
        return readValue(colName);
    }

    @Override
    public Byte asByte(String colName) {
        return readValue(colName);
    }

    @Override
    public Short asShort(String colName) {
        return readValue(colName);
    }

    @Override
    public Integer asInteger(String colName) {
        return readValue(colName);
    }

    @Override
    public Long asLong(String colName) {
        return readValue(colName);
    }

    @Override
    public Float asFloat(String colName) {
        return readValue(colName);
    }

    @Override
    public Double asDouble(String colName) {
        return readValue(colName);
    }

    @Override
    public Boolean asBoolean(String colName) {
        return readValue(colName);
    }

    @Override
    public BigInteger asBigInteger(String colName) {
        return readValue(colName);
    }

    @Override
    public BigDecimal asBigDecimal(String colName) {
        return readValue(colName);
    }

    @Override
    public Instant asInstant(String colName) {
        int colIndex = schema.nameToIndex(colName);
        ClickHouseColumn column = schema.getColumns().get(colIndex);
        switch (column.getDataType()) {
            case Date:
            case Date32:
                LocalDate data = readValue(colName);
                return data.atStartOfDay().toInstant(ZoneOffset.UTC);
            case DateTime:
            case DateTime64:
                LocalDateTime dateTime = readValue(colName);
                return dateTime.toInstant(ZoneOffset.UTC);

        }
        throw new ClientException("Column of type " + column.getDataType() + " cannot be converted to Instant");
    }

    @Override
    public ZonedDateTime asZonedDateTime(String colName) {
        return null;
    }

}
