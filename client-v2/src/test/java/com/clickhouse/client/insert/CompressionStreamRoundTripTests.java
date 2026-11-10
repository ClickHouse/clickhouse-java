package com.clickhouse.client.insert;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.CompressionMethod;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Streams a few hundred thousand mixed-type rows through native block compression and reads them back.
 * The client network buffer is the compression block size, so the payload crosses many blocks.
 */
@Slf4j
public class CompressionStreamRoundTripTests extends BaseIntegrationTest {

    private static final int ROW_COUNT = 300_000;

    private static final long SEED =  ThreadLocalRandom.current().nextLong();

    /** One native compression block. 300k rows then span many blocks. */
    private static final int COMPRESSION_BLOCK_SIZE = 64 * 1024;

    private static final int DATE_SPAN_DAYS = 50 * 365;

    private static final long MIN_UNCOMPRESSED_BYTES = 8_000_000L;

    private static final long MIN_INSERT_BLOCKS = 16L;

    private static final String[] LABELS = {
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
            "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa"
    };

    @DataProvider(name = "compressionMethods")
    public static Object[][] compressionMethods() {
        return new Object[][] {
                { CompressionMethod.LZ4, "lz4" },
                { CompressionMethod.ZSTD, "zstd" },
        };
    }


    @Test(groups = { "integration" }, dataProvider = "compressionMethods")
    public void testContinuousCompressedRoundTrip(CompressionMethod method, String serverMethod) throws Exception {
        log.info("Random seed: {}", SEED);
        String table = "compression_stream_" + serverMethod;
        try (Client client = newCompressedClient(method, serverMethod)) {
            try {
                execute(client, "DROP TABLE IF EXISTS " + table);
                execute(client, "CREATE TABLE " + table + " ("
                        + "id UInt64, qty Int32, label String, event_date Date, score Float64"
                        + ") ORDER BY id");

                String insertQueryId = UUID.randomUUID().toString();
                TableSchema schema = client.getTableSchema(table);
                Random writeRandom = new Random(SEED);
                InsertSettings insertSettings = new InsertSettings()
                        .setQueryId(insertQueryId)
                        .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                        .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1");
                try (InsertResponse response = client.insert(table, out -> {
                    RowBinaryFormatWriter writer = new RowBinaryFormatWriter(out, schema, ClickHouseFormat.RowBinary);
                    for (long id = 0; id < ROW_COUNT; id++) {
                        Row row = Row.next(writeRandom);
                        writer.setValue("id", id);
                        writer.setValue("qty", row.qty);
                        writer.setValue("label", row.label);
                        writer.setValue("event_date", row.eventDate);
                        writer.setValue("score", row.score);
                        writer.commitRow();
                    }
                }, ClickHouseFormat.RowBinary, insertSettings).get(120, TimeUnit.SECONDS)) {
                    assertEquals(response.getWrittenRows(), (long) ROW_COUNT);
                }

                String selectQueryId = UUID.randomUUID().toString();
                Random readRandom = new Random(SEED);
                try (QueryResponse response = client.query(
                        "SELECT id, qty, label, event_date, score FROM " + table + " ORDER BY id",
                        new QuerySettings().setQueryId(selectQueryId)).get(120, TimeUnit.SECONDS)) {
                    ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
                    long rows = 0;
                    while (reader.hasNext()) {
                        reader.next();
                        Row expected = Row.next(readRandom);
                        assertEquals(reader.getLong("id"), rows, "id at row " + rows);
                        assertEquals(reader.getInteger("qty"), expected.qty, "qty at row " + rows);
                        assertEquals(reader.getString("label"), expected.label, "label at row " + rows);
                        assertEquals(reader.getLocalDate("event_date"), expected.eventDate, "event_date at row " + rows);
                        assertEquals(reader.getDouble("score"), expected.score, 0.0, "score at row " + rows);
                        rows++;
                    }
                    assertEquals(rows, (long) ROW_COUNT, "rows read back");
                }

                if (!isCloud()) {
                    assertInsertWasCompressed(client, insertQueryId, serverMethod);
                    assertSelectWasCompressed(client, selectQueryId, serverMethod);
                }
            } finally {
                execute(client, "DROP TABLE IF EXISTS " + table);
            }
        }
    }

    private Client newCompressedClient(CompressionMethod method, String serverMethod) {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean secure = isCloud();
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), secure)
                .setUsername(ClickHouseServerForTest.getUsername())
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .compressClientRequest(true)
                .compressServerResponse(true)
                .useHttpCompression(false)
                .compressionMethod(method)
                .setClientNetworkBufferSize(COMPRESSION_BLOCK_SIZE)
                .serverSetting("network_compression_method", serverMethod)
                .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1")
                .build();
    }

    private static void execute(Client client, String sql) throws Exception {
        client.execute(sql).get(30, TimeUnit.SECONDS).close();
    }

    private static void assertInsertWasCompressed(Client client, String queryId, String serverMethod) throws Exception {
        GenericRecord stats = queryFinishStats(client, queryId);
        long blocks = stats.getLong("read_blocks");
        long rawBytes = stats.getLong("read_raw_bytes");
        long compressedBytes = stats.getLong("read_compressed_bytes");
        assertTrue(blocks >= MIN_INSERT_BLOCKS,
                serverMethod + " insert compressed blocks " + blocks);
        assertTrue(rawBytes >= MIN_UNCOMPRESSED_BYTES,
                serverMethod + " insert uncompressed bytes " + rawBytes);
        assertTrue(compressedBytes > 0, serverMethod + " insert compressed bytes " + compressedBytes);
        assertTrue(rawBytes > compressedBytes,
                serverMethod + " insert raw " + rawBytes + " compressed " + compressedBytes);
    }

    private static void assertSelectWasCompressed(Client client, String queryId, String serverMethod) throws Exception {
        GenericRecord stats = queryFinishStats(client, queryId);
        long resultRows = stats.getLong("result_rows");
        long resultBytes = stats.getLong("result_bytes");
        long sentBytes = stats.getLong("http_sent_bytes");
        if (sentBytes == 0) {
            sentBytes = stats.getLong("sent_bytes");
        }
        assertEquals(resultRows, (long) ROW_COUNT, serverMethod + " select result rows");
        assertTrue(resultBytes >= MIN_UNCOMPRESSED_BYTES,
                serverMethod + " select result bytes " + resultBytes);
        assertTrue(sentBytes > 0, serverMethod + " select sent bytes " + sentBytes);
        assertTrue(resultBytes > sentBytes,
                serverMethod + " select raw " + resultBytes + " sent " + sentBytes);
    }

    private static GenericRecord queryFinishStats(Client client, String queryId) throws Exception {
        client.execute("SYSTEM FLUSH LOGS" + onCluster()).get(30, TimeUnit.SECONDS).close();
        String sql = "SELECT count() AS entries, "
                + "sum(ProfileEvents['CompressedReadBufferBlocks']) AS read_blocks, "
                + "sum(ProfileEvents['CompressedReadBufferBytes']) AS read_raw_bytes, "
                + "sum(ProfileEvents['ReadCompressedBytes']) AS read_compressed_bytes, "
                + "sum(result_rows) AS result_rows, "
                + "sum(result_bytes) AS result_bytes, "
                + "sum(ProfileEvents['NetworkSendBytes']) AS sent_bytes, "
                + "sum(ProfileEvents['InterfaceHTTPSendBytes']) AS http_sent_bytes "
                + "FROM " + queryLogTable() + " "
                + "WHERE query_id = '" + queryId + "' AND type = 'QueryFinish'";
        for (int attempt = 0; attempt < 10; attempt++) {
            List<GenericRecord> records = client.queryAll(sql);
            if (!records.isEmpty() && records.get(0).getLong("entries") > 0) {
                return records.get(0);
            }
            Thread.sleep(500);
        }
        throw new AssertionError("No QueryFinish row for query_id " + queryId);
    }

    private static String queryLogTable() {
        if (isCluster()) {
            return "clusterAllReplicas('test_cluster', system.query_log)";
        }
        return "system.query_log";
    }

    private static final class Row {
        final int qty;
        final String label;
        final LocalDate eventDate;
        final double score;

        private Row(int qty, String label, LocalDate eventDate, double score) {
            this.qty = qty;
            this.label = label;
            this.eventDate = eventDate;
            this.score = score;
        }

        static Row next(Random random) {
            String word = LABELS[random.nextInt(LABELS.length)];
            int repeats = 4 + random.nextInt(4);
            StringBuilder label = new StringBuilder(word.length() * repeats + 8);
            for (int i = 0; i < repeats; i++) {
                if (i > 0) {
                    label.append(' ');
                }
                label.append(word);
            }
            label.append('-').append(random.nextInt(10_000));
            return new Row(
                    random.nextInt(),
                    label.toString(),
                    LocalDate.ofEpochDay(random.nextInt(DATE_SPAN_DAYS)),
                    random.nextDouble());
        }
    }
}
