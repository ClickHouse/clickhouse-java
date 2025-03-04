package com.clickhouse.benchmark.data;

import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
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
        LOGGER.info("Created {} rows of data", data.size());

        jsonBytes = DataSets.convert(data, ClickHouseFormat.JSONEachRow);
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public String getCreateTableString() {
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
    public TableSchema getSchema() {
        TableSchema schema = new TableSchema();
        schema.setTableName(tableName);
        schema.addColumn("trip_id", "UInt32");
        schema.addColumn("pickup_datetime", "DateTime DEFAULT now()");
        schema.addColumn("dropoff_datetime", "DateTime");
        schema.addColumn("pickup_longitude", "Nullable(Float64)");
        schema.addColumn("pickup_latitude", "Nullable(Float64)");
        schema.addColumn("dropoff_longitude", "Nullable(Float64)");
        schema.addColumn("dropoff_latitude", "Nullable(Float64)");
        schema.addColumn("passenger_count", "UInt8");
        schema.addColumn("trip_distance", "Float32");
        schema.addColumn("fare_amount", "Float32");
        schema.addColumn("extra", "Float32");
        schema.addColumn("tip_amount", "Float32");
        schema.addColumn("tolls_amount", "Float32");
        schema.addColumn("total_amount", "Float32");
        schema.addColumn("payment_type", "Enum('CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4, 'UNK' = 5)");
        schema.addColumn("pickup_ntaname", "LowCardinality(String)");
        schema.addColumn("dropoff_ntaname", "LowCardinality(String)");
        return schema;
    }

    @Override
    public List<ClickHouseFormat> supportedFormats() {
        return List.of(ClickHouseFormat.JSONEachRow);
    }

    @Override
    public InputStream getInputStream(ClickHouseFormat format) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (byte[] bytes : getBytesList(format)) {
                bos.write(bytes);
            }
            return new ByteArrayInputStream(bos.toByteArray());
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
            throw new RuntimeException(e);
        }
    }


    @Override
    public InputStream getInputStream(int rowId, ClickHouseFormat format) {
        switch (format) {
            case JSONEachRow:
                return new ByteArrayInputStream(jsonBytes.get(rowId));
            default:
                LOGGER.error("Unsupported format: " + format);
                throw new IllegalArgumentException("Unsupported format: " + format);
        }
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
}
