package com.clickhouse.benchmark.clients;

import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.List;

public class Serializers extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(Serializers.class);

    @Benchmark
    public void SerializerOutputStreamV1(DataState dataState, Blackhole blackhole) {
        OutputStream empty = new BlackholeOutputStream(blackhole);
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
    public void SerializerOutputStreamV2(DataState dataState, Blackhole blackhole) {
        OutputStream empty = new BlackholeOutputStream(blackhole);
        try {
            RowBinaryFormatWriter w = new RowBinaryFormatWriter(empty, dataState.dataSet.getSchema(), ClickHouseFormat.RowBinary);
            for (List<Object> row : dataState.dataSet.getRowsOrdered()) {
                int index = 1;
                for (Object value : row) {
                    w.setValue(index, value);
                    index++;
                }
                w.commitRow();
            }
            empty.flush();
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    private static class BlackholeOutputStream extends OutputStream {

        private final Blackhole blackhole;
        public long count = 0;

        public BlackholeOutputStream(Blackhole blackhole) {
            this.blackhole = blackhole;
        }

        @Override
        public void write(int b) {
            blackhole.consume(b);
            count++;
        }

        @Override
        public void write(byte[] b) {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            blackhole.consume(b);
            count += len;
        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {
        }
    }
}
