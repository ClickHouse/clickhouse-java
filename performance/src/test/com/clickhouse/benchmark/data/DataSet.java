package com.clickhouse.benchmark.data;

import com.clickhouse.benchmark.BenchmarkRunner;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseFormat;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
public class DataSet {
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
            throw new RuntimeException("Error generating data", e);
        }
    }
    private byte[] generateData(int size) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        RowBinaryFormatWriter writer = new RowBinaryFormatWriter(outputStream, getSchema(), getFormat());
        for (int i = 0; i < size; i++) {
            writer.setValue("id", rowCounter++);
            writer.setValue("sample_int", ThreadLocalRandom.current().nextInt());
            writer.commitRow();
        }
        return outputStream.toByteArray();
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
                "    id             UInt32,\n" +
                "    sample_int    Int32\n" +
                ")\n" +
                "ENGINE = Memory;";
    }

    public TableSchema getSchema() {
        return schema;
    }

    public ClickHouseFormat getFormat() {
        return ClickHouseFormat.RowBinaryWithNamesAndTypes;
    }
    public InputStream getInputStream() {
        return new ByteArrayInputStream(data);
    }
}
