package com.clickhouse.benchmark.data;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class SimpleDataSet implements DataSet {
    private static final DateTimeFormatter DATETIME_FORMATTER = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter();
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleDataSet.class);
    private final String name = "simple";
    private final String tableName;
    private final int size;
    private final List<Map<String, Object>> data;
    private final List<byte[]> jsonBytes;

    public SimpleDataSet() {
        tableName = name + "_dataset_" + UUID.randomUUID().toString().replaceAll("-", "");
        size = 100000;

        data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Map<String, Object> row = new HashMap<>(20);
            row.put("trip_id", i);
            row.put("pickup_datetime", ZonedDateTime.now().format(DATETIME_FORMATTER));
            row.put("dropoff_datetime", ZonedDateTime.now().format(DATETIME_FORMATTER));
            row.put("pickup_longitude", ThreadLocalRandom.current().nextDouble(-180, 180));
            row.put("pickup_latitude", ThreadLocalRandom.current().nextDouble(-90, 90));
            row.put("dropoff_longitude", ThreadLocalRandom.current().nextDouble(-180, 180));
            row.put("dropoff_latitude", ThreadLocalRandom.current().nextDouble(-90, 90));
            row.put("passenger_count", ThreadLocalRandom.current().nextInt(1, 5));
            row.put("trip_distance", ThreadLocalRandom.current().nextDouble(0, 100));
            row.put("fare_amount", ThreadLocalRandom.current().nextDouble(0, 100));
            row.put("extra", ThreadLocalRandom.current().nextDouble(0, 100));
            row.put("tip_amount", ThreadLocalRandom.current().nextDouble(0, 100));
            row.put("tolls_amount", ThreadLocalRandom.current().nextDouble(0, 100));
            row.put("total_amount", ThreadLocalRandom.current().nextDouble(0, 100));
            row.put("payment_type", "CSH");
            row.put("pickup_ntaname", "NTA1");
            row.put("dropoff_ntaname", "NTA2");
            data.add(row);
        }

        jsonBytes = DataSets.convert(data, ClickHouseFormat.JSONEachRow);
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public String getCreateTableString(String tableName) {
        return "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" +
                "    trip_id             UInt32,\n" +
                "    pickup_datetime     DateTime DEFAULT now(),\n" +
                "    dropoff_datetime    DateTime,\n" +
                "    pickup_longitude    Nullable(Float64),\n" +
                "    pickup_latitude     Nullable(Float64),\n" +
                "    dropoff_longitude   Nullable(Float64),\n" +
                "    dropoff_latitude    Nullable(Float64),\n" +
                "    passenger_count     UInt8,\n" +
                "    trip_distance       Float32,\n" +
                "    fare_amount         Float32,\n" +
                "    extra               Float32,\n" +
                "    tip_amount          Float32,\n" +
                "    tolls_amount        Float32,\n" +
                "    total_amount        Float32,\n" +
                "    payment_type        Enum('CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4, 'UNK' = 5),\n" +
                "    pickup_ntaname      LowCardinality(String),\n" +
                "    dropoff_ntaname     LowCardinality(String)\n" +
                ")\n" +
                "ENGINE = MergeTree\n" +
                "PRIMARY KEY (pickup_datetime, dropoff_datetime);";
    }

    @Override
    public ClickHouseFormat getFormat() {
        return ClickHouseFormat.JSONEachRow;
    }

    @Override
    public TableSchema getSchema() {
        TableSchema schema = new TableSchema(tableName, "", "", Arrays.asList(
        ClickHouseColumn.of("trip_id", "UInt32"),
        ClickHouseColumn.of("pickup_datetime", "DateTime DEFAULT now()"),
        ClickHouseColumn.of("dropoff_datetime", "DateTime"),
        ClickHouseColumn.of("pickup_longitude", "Nullable(Float64)"),
        ClickHouseColumn.of("pickup_latitude", "Nullable(Float64)"),
        ClickHouseColumn.of("dropoff_longitude", "Nullable(Float64)"),
        ClickHouseColumn.of("dropoff_latitude", "Nullable(Float64)"),
        ClickHouseColumn.of("passenger_count", "UInt8"),
        ClickHouseColumn.of("trip_distance", "Float32"),
        ClickHouseColumn.of("fare_amount", "Float32"),
        ClickHouseColumn.of("extra", "Float32"),
        ClickHouseColumn.of("tip_amount", "Float32"),
        ClickHouseColumn.of("tolls_amount", "Float32"),
        ClickHouseColumn.of("total_amount", "Float32"),
        ClickHouseColumn.of("payment_type", "Enum('CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4, 'UNK' = 5)"),
        ClickHouseColumn.of("pickup_ntaname", "LowCardinality(String)"),
        ClickHouseColumn.of("dropoff_ntaname", "LowCardinality(String)")));
        return schema;
    }

    @Override
    public List<ClickHouseFormat> supportedFormats() {
        return List.of(ClickHouseFormat.JSONEachRow);
    }


    @Override
    public List<byte[]> getBytesList(ClickHouseFormat format) {
        switch (format) {
            case JSONEachRow:
                return jsonBytes;
            default:
                LOGGER.error("Unsupported format: " + format);
                throw new IllegalArgumentException("Unsupported format: " + format);
        }
    }

    @Override
    public List<Map<String, Object>> getRows() {
        return data;
    }


    private List<ClickHouseRecord> clickHouseRecords;

    @Override
    public List<ClickHouseRecord> getClickHouseRecords() {
        return clickHouseRecords;
    }
    @Override
    public List<Map<String, Object>> getRowsLimit(int numRows) {
        return data.subList(0, numRows);
    }
    @Override
    public List<List<Object>> getRowsOrdered() {
        return null;
    }
    @Override
    public List<ClickHouseRecord> getClickHouseRecordsLimit(int numRows) {
        return clickHouseRecords.subList(0, numRows);
    }
    @Override
    public void setClickHouseRecords(List<ClickHouseRecord> records) {
        clickHouseRecords = records;
    }


    private ClickHouseDataProcessor dataProcessor;

    @Override
    public ClickHouseDataProcessor getClickHouseDataProcessor() {
        return dataProcessor;
    }

    @Override
    public void setClickHouseDataProcessor(ClickHouseDataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }
}
