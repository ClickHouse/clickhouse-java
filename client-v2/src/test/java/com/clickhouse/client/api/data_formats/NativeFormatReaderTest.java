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
import java.io.IOException;
import java.util.ArrayList;
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

    private static byte[] fromHex(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }
}
