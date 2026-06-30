package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.TimeZone;

/**
 * Unit coverage for the {@code writeString} helpers on {@link RowBinaryFormatSerializer} and the
 * {@code setString(byte[])} overloads on {@link RowBinaryFormatWriter}, which the integration tests do
 * not exercise in isolation.
 */
public class RowBinaryFormatStringWriteTest {

    private static final ClickHouseColumn STRING_COLUMN = ClickHouseColumn.of("s", "String");

    private static BinaryStreamReader reader(byte[] data) {
        return new BinaryStreamReader(new ByteArrayInputStream(data), TimeZone.getTimeZone("UTC"), null,
                new BinaryStreamReader.DefaultByteBufferAllocator(), false, null, true);
    }

    @Test
    public void testSerializerWriteString() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new RowBinaryFormatSerializer(out).writeString("hello world");

        StringValue read = reader(out.toByteArray()).readValue(STRING_COLUMN);
        Assert.assertEquals(read.asString(), "hello world");
    }

    @Test
    public void testSerializerWriteStringUnicode() throws IOException {
        String value = "Привет 你好 🚀";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new RowBinaryFormatSerializer(out).writeString(value);

        StringValue read = reader(out.toByteArray()).readValue(STRING_COLUMN);
        Assert.assertEquals(read.asString(), value);
    }

    @Test
    public void testSerializerWriteStringFromBytesPreservesBinary() throws IOException {
        byte[] binary = new byte[]{(byte) 0x00, (byte) 0xFF, (byte) 0x80, (byte) 0x7F, (byte) 0xAB};
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new RowBinaryFormatSerializer(out).writeString(binary);

        StringValue read = reader(out.toByteArray()).readValue(STRING_COLUMN);
        Assert.assertEquals(read.toByteArray(), binary);
    }

    @Test
    public void testWriterSetStringBytesByName() throws IOException {
        byte[] binary = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0x00, (byte) 0xBE, (byte) 0xEF};
        ByteArrayOutputStream out = writeSingleStringRow(w -> w.setString("s", binary));

        StringValue read = reader(out.toByteArray()).readValue(STRING_COLUMN);
        Assert.assertEquals(read.toByteArray(), binary);
    }

    @Test
    public void testWriterSetStringBytesByIndex() throws IOException {
        byte[] binary = new byte[]{(byte) 0x01, (byte) 0x02, (byte) 0xFE, (byte) 0xFF, (byte) 0x00};
        ByteArrayOutputStream out = writeSingleStringRow(w -> w.setString(1, binary));

        StringValue read = reader(out.toByteArray()).readValue(STRING_COLUMN);
        Assert.assertEquals(read.toByteArray(), binary);
    }

    @Test
    public void testWriterSetStringBytesUtf8Content() throws IOException {
        String text = "ascii and Юникод";
        byte[] binary = text.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = writeSingleStringRow(w -> w.setString("s", binary));

        StringValue read = reader(out.toByteArray()).readValue(STRING_COLUMN);
        Assert.assertEquals(read.asString(), text);
    }

    private interface RowWriter {
        void write(RowBinaryFormatWriter writer);
    }

    private static ByteArrayOutputStream writeSingleStringRow(RowWriter rowWriter) throws IOException {
        TableSchema schema = new TableSchema(Collections.singletonList(STRING_COLUMN));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatWriter writer = new RowBinaryFormatWriter(out, schema, ClickHouseFormat.RowBinary);
        rowWriter.write(writer);
        writer.commitRow();
        return out;
    }
}
