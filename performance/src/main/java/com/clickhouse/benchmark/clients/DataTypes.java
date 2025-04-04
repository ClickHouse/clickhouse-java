package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.data.SyntheticDataSet;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.format.BinaryDataProcessor;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.data.value.ClickHouseDateTimeValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.TimeZone;

public class DataTypes extends BenchmarkBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataTypes.class);

    @Setup(Level.Iteration)
    public void setUpIteration(DataState dataState) {
        super.setUpIteration();

        try (Client c = getClientV2(); QueryResponse r = c.query("SELECT * FROM " + dataState.tableNameFilled, new QuerySettings()
                .setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes)).get()) {
            dataState.datasetAsRowBinaryWithNamesAndTypes = ByteBuffer.wrap(r.getInputStream().readAllBytes());
            LOGGER.info("Loaded {} from dataset", dataState.datasetAsRowBinaryWithNamesAndTypes.capacity());
        } catch (Exception e) {
            LOGGER.error("Failed to init data for components benchmark", e);
        }

        if (dataState.syntheticDataSet != null) {
            dataState.syntheticDataSet = new SyntheticDataSet(dataState.limit);
        }
    }

    @Benchmark
    public void readDateTimeV1(DataState dataState, Blackhole blackhole) {
        ClickHouseInputStream input = ClickHouseInputStream.of(dataState.syntheticDataSet.getDateTimeValuesRowBinaryStream());
        BinaryDataProcessor.DateTime64SerDe serDe = new BinaryDataProcessor.DateTime64SerDe(3, TimeZone.getTimeZone("UTC"));

        ClickHouseDateTimeValue valueHolder = ClickHouseDateTimeValue.ofNull(3, TimeZone.getTimeZone("UTC"));

        int valueCount = 0;
        while (valueCount <= dataState.limit) {
            try {
                serDe.deserialize(valueHolder, input);
                blackhole.consume(valueHolder);
                valueCount++;
            } catch (IOException ex) {
                if (valueCount < dataState.limit) {
                    throw new RuntimeException("Failed to read all values", ex);
                }
                break;
            }
        }
    }

    @Benchmark
    public void readDateTimeV2(DataState dataState, Blackhole blackhole) {
        ClickHouseInputStream input = ClickHouseInputStream.of(dataState.syntheticDataSet.getDateTimeValuesRowBinaryStream());

        byte[] buffer = new byte[8];
        TimeZone zoneId = TimeZone.getTimeZone("UTC");

        int valueCount = 0;
        while (valueCount <= dataState.limit) {
            try {
                blackhole.consume(BinaryStreamReader.readDateTime64(input, buffer, 3, zoneId));
                valueCount++;
            } catch (EOFException ex) {
                if (valueCount < dataState.limit) {
                    throw new RuntimeException("Failed to read all values", ex);
                }
                break;
            } catch (IOException ex) {
                throw new RuntimeException("Failed to read all values", ex);
            }
        }
    }


    @Benchmark
    public void DateTimeSerializerV1(DataState dataState, Blackhole blackhole) {
        OutputStream empty = new BlackholeOutputStream(blackhole);
        BinaryDataProcessor.DateTime64SerDe serDe =
                new BinaryDataProcessor.DateTime64SerDe(3, TimeZone.getTimeZone("UTC"));

        ClickHouseOutputStream chos = ClickHouseOutputStream.of(empty);
        TimeZone tz = TimeZone.getTimeZone("UTC");

        for (LocalDateTime dateTime : dataState.syntheticDataSet.getDateTimeValues()) {
            try {
                BinaryStreamUtils.writeDateTime64(chos, dateTime, 3, tz);
                // App should wrap a value with a value object if it wants to use a data processor
                //  serDe.serialize(ClickHouseDateTimeValue.of(dateTime, 3, tz) , chos);
            } catch (Exception e) {
                LOGGER.error("Error: ", e);
            }
        }
        try {
            chos.flush();
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void DateTimeSerializerV2(DataState dataState, Blackhole blackhole) {
        OutputStream empty = new BlackholeOutputStream(blackhole);
        ClickHouseColumn column = ClickHouseColumn.of("a", "DateTime64(3, 'UTC')");

        for (LocalDateTime dateTime : dataState.syntheticDataSet.getDateTimeValues()) {
            try {
                SerializerUtils.serializeData(empty, dateTime, column);
            } catch (Exception e) {
                LOGGER.error("Error: ", e);
            }
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