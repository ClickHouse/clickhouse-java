package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.format.BinaryStreamUtils;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * QBit handling in {@link NativeFormatReader}, driven by Native-format blocks captured from ClickHouse
 * 26.5.1 and by minimal hand-built block headers. These run without a live server, so the block-level
 * routing (decode a plain float QBit, reject every other shape) stays covered on the coverage build,
 * whose older server skips the QBit integration tests.
 */
public class NativeFormatReaderQBitTest {

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
