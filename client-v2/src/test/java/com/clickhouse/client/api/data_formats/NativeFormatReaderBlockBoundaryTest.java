package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.format.BinaryStreamUtils;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Rows around a Native block boundary. The reader reads one row ahead and the next block re-publishes the
 * schema, so these check every value, not only the row count (which was right even when rows were lost).
 */
public class NativeFormatReaderBlockBoundaryTest {

    @Test
    public void testNextReturnsEveryRowAcrossBlocks() throws IOException {
        // 25 rows split like SELECT toInt32(number) FROM numbers(25) SETTINGS max_block_size = 10
        NativeFormatReader reader = nativeReader(blocks(25, 10));

        List<Object> values = new ArrayList<>();
        Map<String, Object> row;
        while ((row = reader.next()) != null) {
            values.add(row.get("n"));
        }

        Assert.assertEquals(values, expected(25));
    }

    @Test
    public void testGettersReturnEveryRowAcrossBlocks() throws IOException {
        NativeFormatReader reader = nativeReader(blocks(25, 10));

        List<Object> values = new ArrayList<>();
        while (reader.hasNext()) {
            reader.next();
            values.add(reader.getInteger("n"));
        }

        Assert.assertEquals(values, expected(25));
    }

    @Test
    public void testSingleRowBlocks() throws IOException {
        // Every row is both the last row of one block and followed by a new block.
        NativeFormatReader reader = nativeReader(blocks(5, 1));

        List<Object> values = new ArrayList<>();
        Map<String, Object> row;
        while ((row = reader.next()) != null) {
            values.add(row.get("n"));
        }

        Assert.assertEquals(values, expected(5));
    }

    private static List<Object> expected(int rows) {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            values.add(i);
        }
        return values;
    }

    // Native blocks with a single Int32 column "n" holding 0..rows-1, at most blockSize rows per block.
    private static byte[] blocks(int rows, int blockSize) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int start = 0; start < rows; start += blockSize) {
            int n = Math.min(blockSize, rows - start);
            BinaryStreamUtils.writeVarInt(out, 1); // one column
            BinaryStreamUtils.writeVarInt(out, n);
            BinaryStreamUtils.writeString(out, "n");
            BinaryStreamUtils.writeString(out, "Int32");
            for (int i = start; i < start + n; i++) {
                BinaryStreamUtils.writeInt32(out, i);
            }
        }
        return out.toByteArray();
    }

    private static NativeFormatReader nativeReader(byte[] data) {
        return new NativeFormatReader(new ByteArrayInputStream(data),
                new QuerySettings().setUseTimeZone("UTC"),
                new BinaryStreamReader.CachingByteBufferAllocator());
    }
}
