package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.internal.ClickHouseLZ4OutputStream;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import com.clickhouse.data.stream.Lz4OutputStream;
import net.jpountz.lz4.LZ4Factory;
import org.openjdk.jmh.annotations.Benchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class Components extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(Components.class);

    @Benchmark
    public void CompressingOutputStreamV1(DataState dataState) {
        DataSet dataSet = dataState.dataSet;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ClickHouseOutputStream out =
                new Lz4OutputStream(baos, 8196, null)) {
            for (byte[] bytes : dataSet.getBytesList(dataSet.getFormat())) {
                out.write(bytes);
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    private static final LZ4Factory factory = LZ4Factory.fastestInstance();

    @Benchmark
    public void CompressingOutputStreamV2(DataState dataState) {
        DataSet dataSet = dataState.dataSet;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(baos,
                factory.fastCompressor(),  8196)) {
            for (byte[] bytes : dataSet.getBytesList(dataSet.getFormat())) {
                out.write(bytes);
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    private OutputStream createEmptyOutputStream() {
        return new OutputStream() {
            private long count = 0;

            @Override
            public void write(int b) {
                count++;
            }

            @Override
            public void write(byte[] b) {
                count += b.length;
            }

            @Override
            public void write(byte[] b, int off, int len) {
                count += len;
            }

            @Override
            public void flush() {

            }

            @Override
            public void close() {
            }
        };
    }
    @Benchmark
    public void SerializerOutputStreamV1(DataState dataState) {
        OutputStream empty = createEmptyOutputStream();

        try {
            ClickHouseOutputStream chos = ClickHouseOutputStream.of(empty);
            ClickHouseDataProcessor p = dataState.dataSet.getClickHouseDataProcessor();
            ClickHouseSerializer[] serializers = p.getSerializers(getClientV1().getConfig(), p.getColumns());
            for (ClickHouseRecord record : dataState.dataSet.getClickHouseRecords()) {
                for (int i = 0; i < serializers.length; i++) {
                    serializers[i].serialize(record.getValue(i), chos);
                }
            }
            chos.flush();
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void SerializerOutputStreamV2(DataState dataState) {
        OutputStream empty = createEmptyOutputStream();
        try {
            RowBinaryFormatWriter w = new RowBinaryFormatWriter(empty, dataState.dataSet.getSchema(), ClickHouseFormat.RowBinary);
            List<ClickHouseColumn> columns = dataState.dataSet.getSchema().getColumns();
            for (Map<String, Object> row : dataState.dataSet.getRows()) {
                for (ClickHouseColumn column : columns) {
                    w.setValue(column.getColumnName(),row.get(column.getColumnName()));
                }
                w.commitRow();
            }
            empty.flush();
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
}
