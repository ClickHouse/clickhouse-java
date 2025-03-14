package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.internal.ClickHouseLZ4OutputStream;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDeserializer;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import com.clickhouse.data.format.ClickHouseRowBinaryProcessor;
import com.clickhouse.data.stream.Lz4OutputStream;
import net.jpountz.lz4.LZ4Factory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Components extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(Components.class);

    @Setup(Level.Iteration)
    public void setUpIteration(DataState dataState) {
        super.setUpIteration();

        try (Client c = getClientV2(); QueryResponse r = c.query("SELECT * FROM " + dataState.tableNameFilled, new QuerySettings()
                .setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes)).get()){
            dataState.datasetAsRowBinaryWithNamesAndTypes = ByteBuffer.wrap(r.getInputStream().readAllBytes());
            LOGGER.info("Loaded {} from dataset", dataState.datasetAsRowBinaryWithNamesAndTypes.capacity());
        } catch (Exception e ) {
            LOGGER.error("Failed to init data for components benchmark", e);
        }
    }


    //    @Benchmark
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

    //    @Benchmark
    public void CompressingOutputStreamV2(DataState dataState) {
        DataSet dataSet = dataState.dataSet;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(baos,
                     factory.fastCompressor(), 8196)) {
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

    //    @Benchmark
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

    //    @Benchmark
    public void SerializerOutputStreamV2(DataState dataState) {
        OutputStream empty = createEmptyOutputStream();
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

            Map<String, Object> row;
            while ((row = r.next()) != null) {
                for (String column : row.keySet()) {
                    blackhole.consume(row.get(column));
                }
            }

        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
}
