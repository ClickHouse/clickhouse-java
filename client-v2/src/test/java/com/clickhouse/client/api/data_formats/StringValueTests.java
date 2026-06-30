package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TimeZone;

public class StringValueTests {

    private static BinaryStreamReader reader(byte[] input, boolean binaryStringSupport) {
        return new BinaryStreamReader(new ByteArrayInputStream(input), TimeZone.getTimeZone("UTC"), null,
                new BinaryStreamReader.DefaultByteBufferAllocator(), false, null, binaryStringSupport);
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
    public void testToByteArrayReturnsBackingArrayReference() {
        byte[] bytes = {1, 2, 3, 4};
        StringValue sv = new StringValue(bytes);
        byte[] backing = sv.toByteArray();
        // No copy is made: the returned array is the live backing storage and mutating it mutates the value.
        Assert.assertSame(backing, bytes, "toByteArray() must return the backing array without copying");
        backing[0] = 42;
        Assert.assertEquals(sv.toByteArray()[0], 42, "Mutating the returned array mutates the value (no copy)");
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
    public void testEqualsAndHashCode() {
        StringValue a = new StringValue("abc".getBytes(StandardCharsets.UTF_8));
        StringValue b = new StringValue("abc".getBytes(StandardCharsets.UTF_8));
        StringValue c = new StringValue("abd".getBytes(StandardCharsets.UTF_8));

        // Reflexive
        Assert.assertEquals(a, a);
        // Equal content -> equal value and equal hash code
        Assert.assertEquals(a, b);
        Assert.assertEquals(b, a, "equals must be symmetric");
        Assert.assertEquals(a.hashCode(), b.hashCode());
        // Different content -> not equal
        Assert.assertNotEquals(a, c);
    }

    @Test
    public void testEqualsRejectsNullAndOtherTypes() {
        StringValue a = new StringValue("abc".getBytes(StandardCharsets.UTF_8));
        Assert.assertFalse(a.equals(null), "A value must never equal null");
        Assert.assertFalse(a.equals("abc"), "A value must not equal a raw String of the same text");
        Assert.assertNotEquals(a, new Object());
    }

    @Test
    public void testEqualsIgnoresDefaultCharset() {
        // equals/hashCode are defined on the raw bytes, so the default charset must not affect them.
        byte[] bytes = "abc".getBytes(StandardCharsets.UTF_8);
        StringValue utf8 = new StringValue(bytes, StandardCharsets.UTF_8);
        StringValue latin1 = new StringValue("abc".getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
        Assert.assertEquals(utf8, latin1, "Values with identical bytes must be equal regardless of default charset");
        Assert.assertEquals(utf8.hashCode(), latin1.hashCode());
    }

    @Test
    public void testEqualsDistinguishesByContentAndLength() {
        StringValue ab = new StringValue(new byte[]{1, 2});
        StringValue abc = new StringValue(new byte[]{1, 2, 3});
        StringValue empty = new StringValue(new byte[0]);

        // Same prefix but different length must not be equal.
        Assert.assertNotEquals(ab, abc);
        Assert.assertNotEquals(abc, ab);
        // Empty values are only equal to other empty values.
        Assert.assertEquals(empty, new StringValue(new byte[0]));
        Assert.assertNotEquals(empty, ab);
    }

    @Test
    public void testEqualsIsConsistentWithBinaryReads() throws IOException {
        // Two independently read StringValues over the same bytes must compare equal.
        byte[] binary = new byte[]{(byte) 0x00, (byte) 0xFF, (byte) 0x80, (byte) 0x7F};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeString(baos, binary);
        byte[] wire = baos.toByteArray();

        ClickHouseColumn column = ClickHouseColumn.of("s", "String");
        StringValue first = reader(wire, true).readValue(column);
        StringValue second = reader(wire, true).readValue(column);

        Assert.assertEquals(first, second);
        Assert.assertEquals(first.hashCode(), second.hashCode());
        Assert.assertEquals(first, new StringValue(binary));
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
        Object read = reader(baos.toByteArray(), true).readValue(column);

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
        StringValue sv = reader(baos.toByteArray(), true).readValue(column);

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
        Object read = reader(baos.toByteArray(), true).readValue(column);

        Assert.assertTrue(read instanceof StringValue);
        Assert.assertEquals(((StringValue) read).toByteArray(), binary);
    }

    @Test
    public void testReadStringArrayKeepsStringWhenBinarySupportEnabled() throws IOException {
        // Even with the binary-string feature enabled, nested Array(String) elements are read as String:
        // nested types are not expected to carry large/binary strings.
        String[] elements = {"plain", "Привет", "中文", ""};

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(baos, elements.length);
        for (String element : elements) {
            BinaryStreamUtils.writeString(baos, element.getBytes(StandardCharsets.UTF_8));
        }

        ClickHouseColumn column = ClickHouseColumn.of("a", "Array(String)");
        Object read = reader(baos.toByteArray(), true).readValue(column);

        Assert.assertTrue(read instanceof BinaryStreamReader.ArrayValue,
                "Expected ArrayValue but got " + read.getClass());
        BinaryStreamReader.ArrayValue array = (BinaryStreamReader.ArrayValue) read;
        Assert.assertEquals(array.length(), elements.length);

        Object raw = array.getArray();
        Assert.assertTrue(raw instanceof String[], "Nested array items must be String, got " + raw.getClass());
        String[] values = (String[]) raw;
        for (int i = 0; i < elements.length; i++) {
            Assert.assertEquals(values[i], elements[i], "Element " + i + " must round-trip as String");
        }
    }

    @Test
    public void testReadStringMapKeepsStringWhenBinarySupportEnabled() throws IOException {
        // Even with the binary-string feature enabled, nested Map(String, String) keys and values are read as String.
        String[] keys = {"k1", "ключ"};
        String[] vals = {"v1", "значение"};

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(baos, keys.length);
        for (int i = 0; i < keys.length; i++) {
            BinaryStreamUtils.writeString(baos, keys[i].getBytes(StandardCharsets.UTF_8));
            BinaryStreamUtils.writeString(baos, vals[i].getBytes(StandardCharsets.UTF_8));
        }

        ClickHouseColumn column = ClickHouseColumn.of("m", "Map(String, String)");
        Object read = reader(baos.toByteArray(), true).readValue(column);

        Assert.assertTrue(read instanceof Map, "Expected Map but got " + read.getClass());
        Map<?, ?> map = (Map<?, ?>) read;
        Assert.assertEquals(map.size(), keys.length);

        int i = 0;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Assert.assertTrue(entry.getKey() instanceof String, "Nested map key must be a String");
            Assert.assertTrue(entry.getValue() instanceof String, "Nested map value must be a String");
            Assert.assertEquals(entry.getKey(), keys[i], "Key " + i);
            Assert.assertEquals(entry.getValue(), vals[i], "Value " + i);
            i++;
        }

        Assert.assertEquals(map.get(keys[0]), vals[0]);
    }

    @Test
    public void testDefaultBehaviorReturnsString() throws IOException {
        byte[] encoded = "still a string".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeString(baos, encoded);

        ClickHouseColumn column = ClickHouseColumn.of("s", "String");
        Object read = reader(baos.toByteArray(), false).readValue(column);

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
        StringValue read = reader(baos.toByteArray(), true).readValue(column);
        Assert.assertEquals(read.toByteArray(), binary);
    }

    @Test
    public void testWriteStringValueToStringRoundTrip() throws IOException {
        byte[] binary = new byte[]{(byte) 0x10, (byte) 0x20, (byte) 0xFE, (byte) 0xFF, (byte) 0x00};
        StringValue value = new StringValue(binary);
        ClickHouseColumn column = ClickHouseColumn.of("s", "String");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.serializeData(baos, value, column);
        StringValue read = reader(baos.toByteArray(), true).readValue(column);
        Assert.assertEquals(read.toByteArray(), binary);
    }

    @Test
    public void testWriteByteArrayToFixedStringRoundTrip() throws IOException {
        byte[] binary = new byte[]{(byte) 0xAA, (byte) 0xBB, (byte) 0xCC};
        ClickHouseColumn column = ClickHouseColumn.of("s", "FixedString(3)");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.serializeData(baos, binary, column);
        StringValue read = reader(baos.toByteArray(), true).readValue(column);
        Assert.assertEquals(read.toByteArray(), binary);
    }
}
