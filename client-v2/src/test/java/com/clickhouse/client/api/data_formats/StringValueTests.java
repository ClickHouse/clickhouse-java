package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;

public class StringValueTests {

    private static final Map<ClickHouseDataType, Class<?>> STRING_AS_BINARY =
            Collections.singletonMap(ClickHouseDataType.String, (Class<?>) StringValue.class);

    private static BinaryStreamReader reader(byte[] input, Map<ClickHouseDataType, Class<?>> hints) {
        return new BinaryStreamReader(new ByteArrayInputStream(input), TimeZone.getTimeZone("UTC"), null,
                new BinaryStreamReader.DefaultByteBufferAllocator(), false, hints);
    }

    // ---- StringValue API ----

    @Test
    public void testStringValueApiBasics() {
        byte[] bytes = "hello world".getBytes(StandardCharsets.UTF_8);
        StringValue sv = new StringValue(bytes);

        Assert.assertEquals(sv.size(), bytes.length);
        Assert.assertFalse(sv.isEmpty());
        Assert.assertEquals(sv.asString(), "hello world");
        Assert.assertEquals(sv.toString(), "hello world");
        Assert.assertEquals(sv.toByteArray(), bytes);
    }

    @Test
    public void testToByteArrayReturnsIndependentCopy() {
        byte[] bytes = {1, 2, 3, 4};
        StringValue sv = new StringValue(bytes);
        byte[] copy = sv.toByteArray();
        copy[0] = 42;
        Assert.assertEquals(sv.toByteArray()[0], 1, "Mutating the returned array must not affect the value");
    }

    @Test
    public void testAsByteBufferIsReadOnly() {
        StringValue sv = new StringValue(new byte[]{1, 2, 3});
        ByteBuffer buffer = sv.asByteBuffer();
        Assert.assertTrue(buffer.isReadOnly());
        Assert.assertEquals(buffer.remaining(), 3);
    }

    @Test
    public void testAsStringIsCached() {
        StringValue sv = new StringValue("cached".getBytes(StandardCharsets.UTF_8));
        String first = sv.asString();
        String second = sv.asString();
        Assert.assertSame(first, second, "asString() should cache and return the same instance");
    }

    @Test
    public void testAsStringWithCharset() {
        String original = "Привет, мир";
        StringValue sv = new StringValue(original.getBytes(StandardCharsets.UTF_16));
        Assert.assertEquals(sv.asString(StandardCharsets.UTF_16), original);
    }

    @Test
    public void testAsInputStream() throws IOException {
        byte[] bytes = {(byte) 0x00, (byte) 0xFF, (byte) 0x10, (byte) 0x7F};
        StringValue sv = new StringValue(bytes);
        try (InputStream is = sv.asInputStream()) {
            byte[] read = new byte[bytes.length];
            int n = is.read(read);
            Assert.assertEquals(n, bytes.length);
            Assert.assertEquals(read, bytes);
        }
    }

    @Test
    public void testEqualsAndHashCode() {
        StringValue a = StringValue.of("abc");
        StringValue b = new StringValue("abc".getBytes(StandardCharsets.UTF_8));
        StringValue c = StringValue.of("abd");
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
        Assert.assertNotEquals(a, c);
    }

    @Test
    public void testOfStringCachesValue() {
        StringValue sv = StringValue.of("preset");
        Assert.assertSame(sv.asString(), sv.asString());
        Assert.assertEquals(sv.asString(), "preset");
    }

    @Test
    public void testEmptyValue() {
        StringValue sv = new StringValue(new byte[0]);
        Assert.assertTrue(sv.isEmpty());
        Assert.assertEquals(sv.size(), 0);
        Assert.assertEquals(sv.asString(), "");
        Assert.assertEquals(sv.toByteArray().length, 0);
    }

    // ---- Reading String columns as StringValue ----

    @DataProvider(name = "charsetStrings")
    private Object[][] charsetStrings() {
        return new Object[][]{
                {"plain ascii", StandardCharsets.UTF_8},
                {"unicode: Привет 你好 🚀", StandardCharsets.UTF_8},
                {"latin1 café", StandardCharsets.ISO_8859_1},
                {"utf16 текст", StandardCharsets.UTF_16},
                {"   leading and trailing   ", StandardCharsets.UTF_8},
                {"", StandardCharsets.UTF_8},
        };
    }

    @Test(dataProvider = "charsetStrings")
    public void testReadStringAsStringValuePreservesBytes(String value, Charset charset) throws IOException {
        byte[] encoded = value.getBytes(charset);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeString(baos, encoded); // binary string write (raw bytes)

        ClickHouseColumn column = ClickHouseColumn.of("s", "String");
        Object read = reader(baos.toByteArray(), STRING_AS_BINARY).readValue(column);

        Assert.assertTrue(read instanceof StringValue, "Expected StringValue but got " + read.getClass());
        StringValue sv = (StringValue) read;
        Assert.assertEquals(sv.toByteArray(), encoded, "Raw bytes must be preserved");
        Assert.assertEquals(sv.asString(charset), value, "Decoding with the source charset must round-trip");
    }

    @Test
    public void testReadBinaryNonUtf8IsPreserved() throws IOException {
        // Bytes that are not valid UTF-8 (e.g. a binary hash). Decoding as UTF-8 would be lossy.
        byte[] binary = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF,
                (byte) 0xFF, (byte) 0x00, (byte) 0x80, (byte) 0xC0, (byte) 0xFE};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeString(baos, binary);

        ClickHouseColumn column = ClickHouseColumn.of("s", "String");
        StringValue sv = reader(baos.toByteArray(), STRING_AS_BINARY).readValue(column);

        Assert.assertEquals(sv.toByteArray(), binary, "Binary content must be preserved exactly");
        Assert.assertEquals(AbstractBinaryFormatReader.stringLikeToBytes(sv), binary,
                "Shared string->bytes conversion must preserve binary content");
    }

    @Test
    public void testFixedStringAsStringValue() throws IOException {
        byte[] binary = new byte[]{(byte) 0x01, (byte) 0xFF, (byte) 0x00, (byte) 0x10, (byte) 0x80};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(binary); // FixedString(5) is written as exactly 5 raw bytes

        ClickHouseColumn column = ClickHouseColumn.of("s", "FixedString(5)");
        Object read = reader(baos.toByteArray(), STRING_AS_BINARY).readValue(column);

        Assert.assertTrue(read instanceof StringValue);
        Assert.assertEquals(((StringValue) read).toByteArray(), binary);
    }

    @Test
    public void testDefaultBehaviorReturnsString() throws IOException {
        byte[] encoded = "still a string".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeString(baos, encoded);

        ClickHouseColumn column = ClickHouseColumn.of("s", "String");
        Object read = reader(baos.toByteArray(), AbstractBinaryFormatReader.NO_TYPE_HINT_MAPPING).readValue(column);

        Assert.assertTrue(read instanceof String, "Without a type hint Strings must still be returned as String");
        Assert.assertEquals(read, "still a string");
    }

    // ---- Writing binary String values ----

    @Test
    public void testWriteByteArrayToStringRoundTrip() throws IOException {
        byte[] binary = new byte[]{(byte) 0x00, (byte) 0xFF, (byte) 0xAB, (byte) 0xCD, (byte) 0x7F};
        ClickHouseColumn column = ClickHouseColumn.of("s", "String");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.serializeData(baos, binary, column);
        StringValue read = reader(baos.toByteArray(), STRING_AS_BINARY).readValue(column);
        Assert.assertEquals(read.toByteArray(), binary);
    }

    @Test
    public void testWriteStringValueToStringRoundTrip() throws IOException {
        byte[] binary = new byte[]{(byte) 0x10, (byte) 0x20, (byte) 0xFE, (byte) 0xFF, (byte) 0x00};
        StringValue value = new StringValue(binary);
        ClickHouseColumn column = ClickHouseColumn.of("s", "String");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.serializeData(baos, value, column);
        StringValue read = reader(baos.toByteArray(), STRING_AS_BINARY).readValue(column);
        Assert.assertEquals(read.toByteArray(), binary);
    }

    @Test
    public void testWriteByteArrayToFixedStringRoundTrip() throws IOException {
        byte[] binary = new byte[]{(byte) 0xAA, (byte) 0xBB, (byte) 0xCC};
        ClickHouseColumn column = ClickHouseColumn.of("s", "FixedString(3)");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.serializeData(baos, binary, column);
        StringValue read = reader(baos.toByteArray(), STRING_AS_BINARY).readValue(column);
        Assert.assertEquals(read.toByteArray(), binary);
    }
}
