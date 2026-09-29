package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ClickHouseLZ4OutputStream;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.internal.net.jpountz.lz4.LZ4Factory;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.stream.Lz4OutputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import static com.clickhouse.benchmark.TestEnvironment.getServer;

/**
 * Compares compression setups of client V1 and V2 for queries (server compresses the response) and inserts
 * (client compresses the request).
 *
 * <p>Matrix benchmarks are named {@code <query|insert><V1|V2><Native|Http><Algorithm>}. {@code Native} is
 * ClickHouse block compression ({@code compress}/{@code decompress} parameters), {@code Http} is compression
 * negotiated with {@code Content-Encoding}/{@code Accept-Encoding}. Only combinations supported by a client
 * have a method: V1 always sends LZ4 natively and any other algorithm over HTTP, V2 native mode is LZ4 only
 * and V2 cannot compress requests with brotli. Snappy does not work with either client.</p>
 *
 * <p>Payloads are raw bytes of the tested format, so the results reflect transport and compression cost
 * rather than (de)serialization.</p>
 */
public class Compression extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(Compression.class);

    static final int COMPRESS_BUFFER_SIZE = 64 * 1024; // 64K
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();

    // Newer servers default to ZSTD for native compression, while both clients decode only LZ4 blocks.
    private static final String NATIVE_METHOD_SETTING = "network_compression_method";

    @Setup(Level.Trial)
    public void setup() {
        LOGGER.info("Compressor type {}", factory.fastCompressor());
    }

    @Benchmark
    public void CompressingOutputStreamV1(DataState dataState) {
        DataSet dataSet = dataState.dataSet;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ClickHouseOutputStream out =
                new Lz4OutputStream(baos, COMPRESS_BUFFER_SIZE, null)) {
            for (byte[] bytes : dataSet.getBytesList(dataSet.getFormat())) {
                out.write(bytes);
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }



    @Benchmark
    public void CompressingOutputStreamV2(DataState dataState) {
        DataSet dataSet = dataState.dataSet;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(baos,
                     factory.fastCompressor(), COMPRESS_BUFFER_SIZE)) {
            for (byte[] bytes : dataSet.getBytesList(dataSet.getFormat())) {
                out.write(bytes);
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @State(Scope.Benchmark)
    public static class CompressionState {
        @Param({"RowBinaryWithNamesAndTypes", "JSONEachRow"})
        String format;

        ClickHouseFormat clickHouseFormat;
        byte[] payload;
        ClickHouseClient clientV1;
        Client clientV2Native;
        Client clientV2Http;

        // Iteration level so it runs after the environment and tables are created at trial level.
        @Setup(Level.Iteration)
        public void setup(DataState dataState) throws Exception {
            if (payload != null) {
                return;
            }
            clickHouseFormat = ClickHouseFormat.valueOf(format);
            try (Client client = getClientV2IncludeDb(false);
                 QueryResponse response = client.query(getSelectQuery(dataState.tableNameFilled),
                         new QuerySettings().setFormat(clickHouseFormat)).get()) {
                payload = response.getInputStream().readAllBytes();
            }
            LOGGER.info("Payload in {}: {} bytes", format, payload.length);

            clientV1 = getClientV1(false);
            clientV2Native = getClientV2Builder(true, true)
                    .useHttpCompression(false)
                    .serverSetting(NATIVE_METHOD_SETTING, "lz4")
                    .build();
            clientV2Http = getClientV2Builder(true, true)
                    .useHttpCompression(true)
                    .build();
        }

        @TearDown(Level.Iteration)
        public void truncate(DataState dataState) {
            truncateTable(dataState.tableNameEmpty);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (clientV1 != null) {
                clientV1.close();
            }
            if (clientV2Native != null) {
                clientV2Native.close();
            }
            if (clientV2Http != null) {
                clientV2Http.close();
            }
        }
    }

    @Benchmark
    public void queryV1NativeLz4(DataState dataState, CompressionState state, Blackhole blackhole) throws Exception {
        queryV1(dataState, state, ClickHouseCompression.LZ4, blackhole);
    }

    @Benchmark
    public void queryV1HttpZstd(DataState dataState, CompressionState state, Blackhole blackhole) throws Exception {
        queryV1(dataState, state, ClickHouseCompression.ZSTD, blackhole);
    }

    @Benchmark
    public void queryV1HttpBrotli(DataState dataState, CompressionState state, Blackhole blackhole) throws Exception {
        queryV1(dataState, state, ClickHouseCompression.BROTLI, blackhole);
    }

    @Benchmark
    public void insertV1NativeLz4(DataState dataState, CompressionState state) throws Exception {
        insertV1(dataState, state, ClickHouseCompression.LZ4);
    }

    @Benchmark
    public void insertV1HttpZstd(DataState dataState, CompressionState state) throws Exception {
        insertV1(dataState, state, ClickHouseCompression.ZSTD);
    }

    @Benchmark
    public void insertV1HttpBrotli(DataState dataState, CompressionState state) throws Exception {
        insertV1(dataState, state, ClickHouseCompression.BROTLI);
    }

    @Benchmark
    public void queryV2NativeLz4(DataState dataState, CompressionState state, Blackhole blackhole) throws Exception {
        queryV2(dataState, state, state.clientV2Native, null, blackhole);
    }

    @Benchmark
    public void queryV2HttpLz4(DataState dataState, CompressionState state, Blackhole blackhole) throws Exception {
        queryV2(dataState, state, state.clientV2Http, ClickHouseCompression.LZ4, blackhole);
    }

    @Benchmark
    public void queryV2HttpZstd(DataState dataState, CompressionState state, Blackhole blackhole) throws Exception {
        queryV2(dataState, state, state.clientV2Http, ClickHouseCompression.ZSTD, blackhole);
    }

    @Benchmark
    public void queryV2HttpBrotli(DataState dataState, CompressionState state, Blackhole blackhole) throws Exception {
        queryV2(dataState, state, state.clientV2Http, ClickHouseCompression.BROTLI, blackhole);
    }

    @Benchmark
    public void insertV2NativeLz4(DataState dataState, CompressionState state) throws Exception {
        insertV2(dataState, state, state.clientV2Native, null);
    }

    @Benchmark
    public void insertV2HttpLz4(DataState dataState, CompressionState state) throws Exception {
        insertV2(dataState, state, state.clientV2Http, ClickHouseCompression.LZ4);
    }

    @Benchmark
    public void insertV2HttpZstd(DataState dataState, CompressionState state) throws Exception {
        insertV2(dataState, state, state.clientV2Http, ClickHouseCompression.ZSTD);
    }

    private static void queryV1(DataState dataState, CompressionState state, ClickHouseCompression algorithm,
                                Blackhole blackhole) throws Exception {
        try (ClickHouseResponse response = state.clientV1.read(getServer())
                .query(getSelectQuery(dataState.tableNameFilled))
                .format(state.clickHouseFormat)
                .option(ClickHouseClientOption.ASYNC, false)
                .option(ClickHouseClientOption.COMPRESS, true)
                .option(ClickHouseClientOption.COMPRESS_ALGORITHM, algorithm)
                .set(NATIVE_METHOD_SETTING, "lz4")
                .executeAndWait()) {
            blackhole.consume(drain(response.getInputStream()));
        }
    }

    private static void insertV1(DataState dataState, CompressionState state, ClickHouseCompression algorithm)
            throws Exception {
        try (ClickHouseResponse response = state.clientV1.read(getServer())
                .write()
                .query(getInsertQuery(dataState.tableNameEmpty))
                .format(state.clickHouseFormat)
                .option(ClickHouseClientOption.ASYNC, false)
                .option(ClickHouseClientOption.DECOMPRESS, true)
                .option(ClickHouseClientOption.DECOMPRESS_ALGORITHM, algorithm)
                .data(new ByteArrayInputStream(state.payload))
                .executeAndWait()) {
            response.getSummary();
        }
    }

    /**
     * @param httpAlgorithm encoding to request with {@code Accept-Encoding}, {@code null} for native compression
     */
    private static void queryV2(DataState dataState, CompressionState state, Client client,
                                ClickHouseCompression httpAlgorithm, Blackhole blackhole) throws Exception {
        QuerySettings settings = new QuerySettings().setFormat(state.clickHouseFormat);
        if (httpAlgorithm != null) {
            settings.httpHeader("Accept-Encoding", httpAlgorithm.encoding());
        }
        try (QueryResponse response = client.query(getSelectQuery(dataState.tableNameFilled), settings).get()) {
            blackhole.consume(drain(response.getInputStream()));
        }
    }

    /**
     * @param httpAlgorithm encoding to send with {@code Content-Encoding}, {@code null} for native compression
     */
    private static void insertV2(DataState dataState, CompressionState state, Client client,
                                 ClickHouseCompression httpAlgorithm) throws Exception {
        InsertSettings settings = new InsertSettings().compressClientRequest(true);
        if (httpAlgorithm != null) {
            settings.httpHeader("Content-Encoding", httpAlgorithm.encoding());
        }
        try (InsertResponse response = client.insert(dataState.tableNameEmpty,
                new ByteArrayInputStream(state.payload), state.clickHouseFormat, settings).get()) {
            response.getWrittenRows();
        }
    }

    private static long drain(InputStream in) throws Exception {
        byte[] buffer = new byte[COMPRESS_BUFFER_SIZE];
        long total = 0;
        int read;
        while ((read = in.read(buffer)) != -1) {
            total += read;
        }
        return total;
    }
}
