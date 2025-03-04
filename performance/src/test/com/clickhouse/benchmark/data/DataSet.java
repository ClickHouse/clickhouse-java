package com.clickhouse.benchmark.data;

import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
public class DataSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSet.class);
    public final String name = "sample_data_set";
    public final String tableName = name + "_" + UUID.randomUUID().toString().replaceAll("-", "");
    @Param({"10000"})
    public int size;
    private byte[] data;
    private TableSchema schema;
    private int rowCounter;

    @Setup(Level.Trial)
    public void setup() {
        // Setup method has to do a few things:
        // 1. Create a table
        // 2. Generate data for the table
        // 3. Insert data into the table (for select)
        // 4. Return generated data (for insert)
        try {
            BenchmarkRunner.createTable(getCreateTableString());// Create table
            schema = BenchmarkRunner.describeTable(tableName);
            rowCounter = 0;
            data = generateData(size);
            BenchmarkRunner.insertData(tableName, new ByteArrayInputStream(generateData(size)), getFormat());
        } catch (Exception e) {
            LOGGER.error("Error while creating table or inserting data.", e);
            throw new RuntimeException("Error while creating table or inserting data.", e);
        }
    }
    private byte[] generateData(int size) {
        //Generate JSON sample data
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < size; i++) {
            builder.append("{\"trip_id\":").append(rowCounter)
                    .append(",\"dropoff_datetime\":\"").append(BenchmarkRunner.DATETIME_FORMATTER.format(ZonedDateTime.now())).append("\"")
                    .append(",\"pickup_longitude\":").append(ThreadLocalRandom.current().nextDouble(-180, 180))
                    .append(",\"pickup_latitude\":").append(ThreadLocalRandom.current().nextDouble(-90, 90))
                    .append(",\"dropoff_longitude\":").append(ThreadLocalRandom.current().nextDouble(-180, 180))
                    .append(",\"dropoff_latitude\":").append(ThreadLocalRandom.current().nextDouble(-90, 90))
                    .append(",\"passenger_count\":").append(ThreadLocalRandom.current().nextInt(1, 5))
                    .append(",\"trip_distance\":").append(ThreadLocalRandom.current().nextDouble(0, 100))
                    .append(",\"fare_amount\":").append(ThreadLocalRandom.current().nextDouble(0, 100))
                    .append(",\"extra\":").append(ThreadLocalRandom.current().nextDouble(0, 100))
                    .append(",\"tip_amount\":").append(ThreadLocalRandom.current().nextDouble(0, 100))
                    .append(",\"tolls_amount\":").append(ThreadLocalRandom.current().nextDouble(0, 100))
                    .append(",\"total_amount\":").append(ThreadLocalRandom.current().nextDouble(0, 100))
                    .append(",\"payment_type\":\"CSH\",\"pickup_ntaname\":\"NTA1\",\"dropoff_ntaname\":\"NTA2\"}");
            if (i < size - 1) {
                builder.append("\n");
            }
            rowCounter++;
        }
        return builder.toString().getBytes();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        if (data != null) {
            data = null;
        }

        if (schema != null) {
            schema = null;
        }

        try {
            rowCounter = 0;
            BenchmarkRunner.dropTable(tableName);
        } catch (Exception e) {
            throw new RuntimeException("Error dropping table", e);
        }
    }


    public String getCreateTableString() {
        return "CREATE TABLE " + tableName + " (\n" +
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

    public TableSchema getSchema() {
        return schema;
    }

    public ClickHouseFormat getFormat() {
        return ClickHouseFormat.JSONEachRow;
    }
    public InputStream getInputStream() {
        return new ByteArrayInputStream(data);
    }
}
