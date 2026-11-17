package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.format.BinaryStreamUtils;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link NativeFormatReader}.
 * <p>
 * The QBit tests are driven by Native-format blocks captured from ClickHouse 26.5.1 and by minimal hand-built
 * block headers. These run without a live server, so the block-level routing (decode a plain float QBit, reject
 * every other shape) stays covered on the coverage build, whose older server skips the QBit integration tests.
 */
public class NativeFormatReaderTest extends BaseIntegrationTest {

    @Test(groups = {"integration"})
    public void testReadsEveryRowAcrossBlocks() throws Exception {
        // Small blocks put several block boundaries in a short result. The row count was right even when the
        // rows around each boundary were lost, so every value is checked.
        String sql = "SELECT toInt32(number) AS n FROM numbers(25) SETTINGS max_block_size = 10";
        List<Object> expected = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            expected.add(i);
        }

        try (Client client = newClient().build();
             QueryResponse response = client.query(sql, new QuerySettings().setFormat(ClickHouseFormat.Native))
                     .get(30, TimeUnit.SECONDS)) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            List<Object> values = new ArrayList<>();
            List<Object> fromGetters = new ArrayList<>();
            Map<String, Object> row;
            while ((row = reader.next()) != null) {
                values.add(row.get("n"));
                fromGetters.add(reader.getInteger("n"));
            }
            Assert.assertEquals(values, expected);
            Assert.assertEquals(fromGetters, expected);
            Assert.assertFalse(reader.hasNext());
        }
    }

    // 0 rows: the server sends an empty stream. 25 rows with max_block_size = 10: three blocks.
    @DataProvider(name = "rowCounts")
    public static Object[][] rowCounts() {
        return new Object[][] {{0}, {1}, {25}};
    }

    @Test(groups = {"integration"}, dataProvider = "rowCounts")
    public void testHasNextIsIdempotent(int rows) throws Exception {
        try (Client client = newClient().build();
             QueryResponse response = queryNative(client, rows)) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            List<Object> values = new ArrayList<>();
            List<Object> fromGetters = new ArrayList<>();
            while (reader.hasNext()) {
                Assert.assertTrue(reader.hasNext());
                Map<String, Object> row = reader.next();
                // After the last row of a block, this reads the next block.
                reader.hasNext();
                values.add(row.get("n"));
                fromGetters.add(reader.getInteger("n"));
            }
            Assert.assertEquals(values, sequence(rows));
            Assert.assertEquals(fromGetters, sequence(rows));
            Assert.assertFalse(reader.hasNext());
            Assert.assertNull(reader.next());
        }
    }

    @Test(groups = {"integration"}, dataProvider = "rowCounts")
    public void testReadRecordReadsEveryRow(int rows) throws Exception {
        try (Client client = newClient().build();
             QueryResponse response = queryNative(client, rows)) {
            NativeFormatReader reader = (NativeFormatReader) client.newBinaryFormatReader(response);
            List<Object> values = new ArrayList<>();
            Map<String, Object> record = new HashMap<>();
            while (reader.readRecord(record)) {
                values.add(record.get("n"));
            }
            Assert.assertEquals(values, sequence(rows));
            Assert.assertFalse(reader.readRecord(record));
        }
    }

    private static QueryResponse queryNative(Client client, int rows) throws Exception {
        return client.query("SELECT toInt32(number) AS n FROM numbers(" + rows + ") SETTINGS max_block_size = 10",
                new QuerySettings().setFormat(ClickHouseFormat.Native)).get(30, TimeUnit.SECONDS);
    }

    private static List<Object> sequence(int rows) {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            values.add(i);
        }
        return values;
    }

    // Row count of each block in the stream. No blocks: an empty stream.
    @DataProvider(name = "blockRowCounts")
    public static Object[][] blockRowCounts() {
        return new Object[][] {{new int[0]}, {new int[] {0}}, {new int[] {0, 2}}, {new int[] {2, 0, 1}},
                {new int[] {2, 0}}};
    }

    @Test(dataProvider = "blockRowCounts")
    public void testSkipsBlocksWithoutRows(int[] blockRows) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        List<Object> expected = new ArrayList<>();
        for (int rows : blockRows) {
            stream.write(nativeBlock(Arrays.asList("n", "Int32"), rows));
            expected.addAll(sequence(rows));
        }

        NativeFormatReader reader = nativeReader(stream.toByteArray());
        List<Object> fromNext = new ArrayList<>();
        Map<String, Object> row;
        while ((row = reader.next()) != null) {
            fromNext.add(row.get("n"));
        }
        Assert.assertEquals(fromNext, expected);

        reader = nativeReader(stream.toByteArray());
        List<Object> fromReadRecord = new ArrayList<>();
        Map<String, Object> record = new HashMap<>();
        while (reader.readRecord(record)) {
            fromReadRecord.add(record.get("n"));
        }
        Assert.assertEquals(fromReadRecord, expected);
    }

    @Test
    public void testReadsBlocksWithManyColumns() throws IOException {
        List<String> columns = new ArrayList<>();
        for (int i = 0; i < 130; i++) {
            columns.add("c" + i);
            columns.add("Int32");
        }
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(nativeBlock(columns, 1));
        stream.write(nativeBlock(columns, 2));
        NativeFormatReader reader = nativeReader(stream.toByteArray());

        Assert.assertEquals(reader.getSchema().getColumns().size(), 130);
        List<Object> values = new ArrayList<>();
        Map<String, Object> row;
        while ((row = reader.next()) != null) {
            values.add(row.get("c129"));
        }
        Assert.assertEquals(values, Arrays.asList(0, 0, 1));
    }

    // A later block with other columns than the first block cannot come from the server: the reader is not at a
    // block boundary. A null cause marks these cases.
    @DataProvider(name = "brokenSecondBlocks")
    public static Object[][] brokenSecondBlocks() throws IOException {
        byte[] block = nativeBlock(Arrays.asList("n", "Int32"), 1);
        return new Object[][] {
                {"no columns", nativeBlock(Collections.emptyList(), 3), null},
                {"other column count", nativeBlock(Arrays.asList("n", "Int32", "m", "Int32"), 1), null},
                {"other name", nativeBlock(Arrays.asList("m", "Int32"), 1), null},
                {"other type", nativeBlock(Arrays.asList("n", "UInt32"), 1), null},
                {"end after the column count", new byte[] {1}, EOFException.class},
                {"end inside the values", Arrays.copyOf(block, block.length - 1), EOFException.class},
        };
    }

    @Test(dataProvider = "brokenSecondBlocks")
    public void testFailsOnBrokenBlock(String label, byte[] secondBlock, Class<?> cause) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(nativeBlock(Arrays.asList("n", "Int32"), 2));
        stream.write(secondBlock);
        NativeFormatReader reader = nativeReader(stream.toByteArray());

        Assert.assertEquals(reader.next().get("n"), 0, label);
        Assert.assertEquals(reader.next().get("n"), 1, label);
        ClientException ex = Assert.expectThrows(ClientException.class, reader::hasNext);
        Assert.assertEquals(ex.getCause() == null ? null : ex.getCause().getClass(), cause, label);
        Assert.assertTrue(ex.getMessage().startsWith("Failed to read block")
                && (cause != null || ex.getMessage().endsWith("not at a block boundary, see the Javadoc of "
                        + NativeFormatReader.class.getName())), label + ": " + ex.getMessage());
        Assert.assertFalse(reader.hasNext(), label);
        Assert.assertNull(reader.next(), label);
    }

    private Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase());
    }

    // Full single-row Native blocks (SELECT CAST(<vec> AS QBit(<type>)) FORMAT Native), one per decodable
    // element type, with the vector each encodes.
    @DataProvider(name = "decodableQBitBlocks")
    public static Object[][] decodableQBitBlocks() {
        return new Object[][] {
                {"Float32",
                        "010103766563105142697428466c6f617433322c2033290206010101010101010404000000000000000000000000000000000000000000",
                        false, new float[] {1f, -2f, 3.5f}, null},
                {"Float64",
                        "010103766563105142697428466c6f617436342c20332902060101010101010101010104040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                        true, null, new double[] {1d, -2d, 3.5d}},
                {"BFloat16",
                        "01010376656311514269742842466c6f617431362c20382900fe01010101e1995500000000000000",
                        false, new float[] {1f, 2f, 4f, 8f, 16f, 32f, 64f, 128f}, null},
        };
    }

    @Test(dataProvider = "decodableQBitBlocks")
    public void testDecodesPlainQBitColumn(String label, String blockHex, boolean isDouble,
            float[] expectedFloat, double[] expectedDouble) {
        NativeFormatReader reader = nativeReader(fromHex(blockHex));

        Assert.assertNotNull(reader.next(), label);
        if (isDouble) {
            Assert.assertEquals(reader.getDoubleArray("vec"), expectedDouble, label);
        } else {
            Assert.assertEquals(reader.getFloatArray("vec"), expectedFloat, label);
        }
        Assert.assertNull(reader.next(), label);
    }

    // QBit shapes the Native reader cannot decode: nested inside a container, strided, wrapped in Nullable,
    // or a non-float element type. Each must be rejected up front instead of misreading the block.
    @DataProvider(name = "undecodableQBitTypes")
    public static Object[][] undecodableQBitTypes() {
        return new Object[][] {
                {"m", "Map(String, QBit(Float32, 3))"},
                {"vec", "QBit(Float32, 4, 2)"},
                {"vec", "Nullable(QBit(Float32, 3))"},
                {"vec", "QBit(Int8, 3)"},
        };
    }

    @Test(dataProvider = "undecodableQBitTypes")
    public void testRejectsUndecodableQBitColumn(String colName, String colType) throws IOException {
        // Only the block header (column name + type) is needed: readBlock rejects the column before reading
        // any payload.
        byte[] block = nativeBlockHeader(colName, colType);
        ClientException ex = Assert.expectThrows(ClientException.class, () -> nativeReader(block));
        Assert.assertTrue(ex.getMessage().contains("QBit") && ex.getMessage().contains("Native"),
                colType + ": " + ex.getMessage());
    }

    private static NativeFormatReader nativeReader(byte[] block) {
        return new NativeFormatReader(new ByteArrayInputStream(block),
                new QuerySettings().setUseTimeZone("UTC"),
                new BinaryStreamReader.CachingByteBufferAllocator());
    }

    private static byte[] nativeBlockHeader(String columnName, String columnType) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(out, 1); // one column
        BinaryStreamUtils.writeVarInt(out, 1); // one row
        BinaryStreamUtils.writeString(out, columnName);
        BinaryStreamUtils.writeString(out, columnType);
        return out.toByteArray();
    }

    /**
     * Builds a Native block with {@code Int32} values 0, 1, ... in each column.
     *
     * @param columns column names and types, in pairs
     */
    private static byte[] nativeBlock(List<String> columns, int rows) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(out, columns.size() / 2);
        BinaryStreamUtils.writeVarInt(out, rows);
        for (int i = 0; i < columns.size(); i += 2) {
            BinaryStreamUtils.writeString(out, columns.get(i));
            BinaryStreamUtils.writeString(out, columns.get(i + 1));
            for (int row = 0; row < rows; row++) {
                BinaryStreamUtils.writeInt32(out, row);
            }
        }
        return out.toByteArray();
    }

    private static byte[] fromHex(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }
}
