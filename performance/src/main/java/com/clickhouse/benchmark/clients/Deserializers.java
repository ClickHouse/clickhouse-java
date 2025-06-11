package com.clickhouse.benchmark.clients;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.format.ClickHouseRowBinaryProcessor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class Deserializers extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(Deserializers.class);

    @Setup(Level.Iteration)
    public void setUpIteration(DataState dataState) {
        super.setUpIteration();

        try (Client c = getClientV2(); QueryResponse r = c.query("SELECT * FROM " + dataState.tableNameFilled, new QuerySettings()
                .setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes)).get()){
            dataState.datasetAsRowBinaryWithNamesAndTypes = ByteBuffer.wrap(r.getInputStream().readAllBytes());
            LOGGER.info("Loaded {} from dataset", dataState.datasetAsRowBinaryWithNamesAndTypes.capacity());
        } catch (Exception e ) {
            LOGGER.error("Failed to init data for components com.clickhouse.benchmark", e);
        }
    }

    @Benchmark
    public void DeserializerOutputStreamV1(DataState dataState, Blackhole blackhole) {
        InputStream input = new ByteArrayInputStream(dataState.datasetAsRowBinaryWithNamesAndTypes.array());
        try {
            ClickHouseConfig config = new ClickHouseConfig(Collections.singletonMap(ClickHouseClientOption.FORMAT, ClickHouseFormat.RowBinaryWithNamesAndTypes));
            ClickHouseDataProcessor p = new ClickHouseRowBinaryProcessor(config,
                    ClickHouseInputStream.of(input), null, null, Collections.emptyMap());
            List<ClickHouseColumn> columns = p.getColumns();
            for (ClickHouseRecord record : p.records()) {
                for (int i = 0; i < columns.size(); i++) {
                    blackhole.consume(record.getValue(i).asObject());
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void DeserializerOutputStreamV2(DataState dataState, Blackhole blackhole) {
        InputStream input = new ByteArrayInputStream(dataState.datasetAsRowBinaryWithNamesAndTypes.array());
        try {
            RowBinaryWithNamesAndTypesFormatReader r = new RowBinaryWithNamesAndTypesFormatReader(input,
                    new QuerySettings()
                            .setUseTimeZone("UTC")
                            .setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes), new BinaryStreamReader.DefaultByteBufferAllocator());

            final int columnCount = dataState.dataSet.getSchema().getColumns().size();
            while (r.next() != null) {
                for (int i = 1; i <= columnCount; i++) {
                    blackhole.consume(r.readValue(i));
                }
            }

        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
}
