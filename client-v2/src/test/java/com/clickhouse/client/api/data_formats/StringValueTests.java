package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.MapBackedRecord;
import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.serde.POJOFieldDeserializer;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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

    // ---- POJO binding (queryAll/readToPOJO) over String columns with the feature enabled ----

    /**
     * Minimal POJO with the only two field representations supported for top-level String/FixedString
     * columns: {@link String} and {@code byte[]}. {@link StringValue} is a read-time holder and is not a
     * supported POJO field type.
     */
    public static class StringPojo {
        private String asString;
        private byte[] asBytes;

        public void setAsString(String asString) { this.asString = asString; }
        public void setAsBytes(byte[] asBytes) { this.asBytes = asBytes; }
    }

    private static byte[] stringWire(byte[] value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryStreamUtils.writeString(baos, value);
        return baos.toByteArray();
    }

    private static POJOFieldDeserializer setterFor(String name, ClickHouseColumn column) throws Exception {
        Method setter = StringPojo.class.getMethod(name,
                name.equals("setAsString") ? String.class : byte[].class);
        return SerializerUtils.compilePOJOSetter(setter, column);
    }

    @Test
    public void testPojoSetterStringFieldDecodesWhenFeatureEnabled() throws Exception {
        // Regression: with the feature enabled the reader produces StringValue, but a String setter must
        // still receive a decoded String (the compiled setter casts the readValue result to String).
        ClickHouseColumn column = ClickHouseColumn.of("s", "String");
        byte[] wire = stringWire("hello".getBytes(StandardCharsets.UTF_8));

        StringPojo pojo = new StringPojo();
        setterFor("setAsString", column).setValue(pojo, reader(wire, true), column);
        Assert.assertEquals(pojo.asString, "hello");
    }

    @Test
    public void testPojoSetterByteArrayFieldReceivesRawBytesWhenFeatureEnabled() throws Exception {
        // A byte[] setter must receive the raw bytes (preserving non-UTF-8 content) instead of a StringValue.
        ClickHouseColumn column = ClickHouseColumn.of("s", "String");
        byte[] binary = {(byte) 0xDE, (byte) 0xAD, (byte) 0x00, (byte) 0xBE, (byte) 0xEF};
        byte[] wire = stringWire(binary);

        StringPojo pojo = new StringPojo();
        setterFor("setAsBytes", column).setValue(pojo, reader(wire, true), column);
        Assert.assertEquals(pojo.asBytes, binary);
    }

    @Test
    public void testPojoSetterFixedStringByteArrayFieldWhenFeatureEnabled() throws Exception {
        // A byte[] setter over a FixedString column must receive the raw bytes, preserving binary content.
        ClickHouseColumn column = ClickHouseColumn.of("s", "FixedString(3)");
        byte[] binary = {(byte) 0xAA, (byte) 0xBB, (byte) 0xCC};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(binary); // FixedString(3) is exactly 3 raw bytes on the wire

        StringPojo pojo = new StringPojo();
        setterFor("setAsBytes", column).setValue(pojo, reader(baos.toByteArray(), true), column);
        Assert.assertEquals(pojo.asBytes, binary);
    }

    @Test
    public void testPojoSetterFixedStringStringFieldWhenFeatureEnabled() throws Exception {
        // A String setter over a FixedString column must receive the decoded String.
        ClickHouseColumn column = ClickHouseColumn.of("s", "FixedString(3)");
        byte[] bytes = "abc".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(bytes);

        StringPojo pojo = new StringPojo();
        setterFor("setAsString", column).setValue(pojo, reader(baos.toByteArray(), true), column);
        Assert.assertEquals(pojo.asString, "abc");
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

    @Test
    public void testWriteStringValueToFixedStringRoundTrip() throws IOException {
        // FixedString serialization must accept a StringValue and write its raw bytes (padded to the
        // column width), mirroring the byte[] branch.
        byte[] binary = new byte[]{(byte) 0x10, (byte) 0x20, (byte) 0x00};
        StringValue value = new StringValue(binary);
        ClickHouseColumn column = ClickHouseColumn.of("s", "FixedString(3)");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.serializeData(baos, value, column);
        StringValue read = reader(baos.toByteArray(), true).readValue(column);
        Assert.assertEquals(read.toByteArray(), binary);
    }

    @Test
    public void testWriteStringValueToFixedStringPadsShorterValue() throws IOException {
        // A StringValue shorter than the column width is right-padded with zero bytes.
        byte[] binary = new byte[]{(byte) 0xAB, (byte) 0xCD};
        ClickHouseColumn column = ClickHouseColumn.of("s", "FixedString(5)");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializerUtils.serializeData(baos, new StringValue(binary), column);
        StringValue read = reader(baos.toByteArray(), true).readValue(column);
        Assert.assertEquals(read.toByteArray(), new byte[]{(byte) 0xAB, (byte) 0xCD, 0, 0, 0});
    }

    // ---- MapBackedRecord (queryAll materialization) ----

    /**
     * Materializes a single-row {@link MapBackedRecord} the same way {@code Client.queryAll(...)} does:
     * a {@code RowBinaryWithNamesAndTypes} stream is read into a map and wrapped together with the
     * reader's converters and schema. {@code binaryStringSupport} mirrors the client property.
     */
    private static MapBackedRecord materializeRow(String[] names, String[] types, Object[] values,
                                                  boolean binaryStringSupport) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }
        for (int i = 0; i < names.length; i++) {
            SerializerUtils.serializeData(out, values[i], ClickHouseColumn.of(names[i], types[i]));
        }

        RowBinaryWithNamesAndTypesFormatReader reader = new RowBinaryWithNamesAndTypesFormatReader(
                new ByteArrayInputStream(out.toByteArray()),
                new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId()),
                new BinaryStreamReader.CachingByteBufferAllocator(), null, binaryStringSupport);

        Map<String, Object> record = new LinkedHashMap<>();
        Assert.assertTrue(reader.readRecord(record), "Expected a row to be read");
        return new MapBackedRecord(record, reader.getConvertions(), reader.getSchema());
    }

    @Test
    public void testMapBackedRecordReadsStringAsStringValueWhenEnabled() throws IOException {
        byte[] binary = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0x00, (byte) 0xBE, (byte) 0xEF};
        MapBackedRecord record = materializeRow(new String[]{"s"}, new String[]{"String"},
                new Object[]{binary}, true);

        // getObject exposes the raw holder
        Object value = record.getObject("s");
        Assert.assertTrue(value instanceof StringValue, "Expected StringValue but got " + value.getClass());
        Assert.assertEquals(((StringValue) value).toByteArray(), binary);

        // getByteArray must preserve the raw bytes through both the label and index overloads
        Assert.assertEquals(record.getByteArray("s"), binary, "getByteArray(name) must preserve raw bytes");
        Assert.assertEquals(record.getByteArray(1), binary, "getByteArray(index) must preserve raw bytes");
    }

    @Test
    public void testMapBackedRecordGetByteArrayIndexAndLabelParity() throws IOException {
        // The index and label overloads of getByteArray must behave identically for a String column.
        byte[] binary = new byte[]{(byte) 0x01, (byte) 0x80, (byte) 0xFF, (byte) 0x00};
        MapBackedRecord record = materializeRow(new String[]{"s"}, new String[]{"String"},
                new Object[]{binary}, true);
        Assert.assertEquals(record.getByteArray(1), record.getByteArray("s"));
        Assert.assertEquals(record.getByteArray(1), binary);
    }

    @Test
    public void testMapBackedRecordFixedStringByteArray() throws IOException {
        byte[] binary = new byte[]{(byte) 0xAA, (byte) 0xBB, (byte) 0xCC};
        MapBackedRecord record = materializeRow(new String[]{"s"}, new String[]{"FixedString(3)"},
                new Object[]{binary}, true);

        Assert.assertTrue(record.getObject("s") instanceof StringValue);
        Assert.assertEquals(record.getByteArray("s"), binary);
        Assert.assertEquals(record.getByteArray(1), binary);
    }

    @Test
    public void testMapBackedRecordGetStringDecodesStringValue() throws IOException {
        String text = "Привет, мир";
        MapBackedRecord record = materializeRow(new String[]{"s"}, new String[]{"String"},
                new Object[]{text}, true);
        Assert.assertEquals(record.getString("s"), text);
        Assert.assertEquals(record.getString(1), text);
    }

    @Test
    public void testMapBackedRecordNestedArrayStaysString() throws IOException {
        // Even with the feature enabled, nested Array(String) elements stay String; getStringArray must
        // work through both overloads and getList must expose String elements.
        String[] elements = {"plain", "Привет", ""};
        MapBackedRecord record = materializeRow(new String[]{"a"}, new String[]{"Array(String)"},
                new Object[]{elements}, true);

        Assert.assertEquals(record.getStringArray("a"), elements);
        Assert.assertEquals(record.getStringArray(1), elements);

        List<String> list = record.getList("a");
        Assert.assertEquals(list.size(), elements.length);
        for (Object element : list) {
            Assert.assertTrue(element instanceof String, "Nested array element must be a String");
        }
    }

    @Test
    public void testMapBackedRecordDefaultReturnsString() throws IOException {
        // Default behavior (feature off): top-level Strings are plain String and byte access re-encodes as UTF-8.
        String text = "ascii text";
        MapBackedRecord record = materializeRow(new String[]{"s"}, new String[]{"String"},
                new Object[]{text}, false);

        Assert.assertTrue(record.getObject("s") instanceof String);
        Assert.assertEquals(record.getString("s"), text);
        Assert.assertEquals(record.getByteArray("s"), text.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(record.getByteArray(1), text.getBytes(StandardCharsets.UTF_8));
    }

    // ---- getStringArray over arrays whose items are StringValue ----

    private static BinaryStreamReader.ArrayValue stringValueArray() {
        // Build an array whose item type is StringValue (the branch that is not reachable through the
        // wire, where nested array elements are always decoded as String). A null element exercises the
        // null-guard inside the StringValue mapping.
        BinaryStreamReader.ArrayValue array = new BinaryStreamReader.ArrayValue(StringValue.class, 3);
        array.set(0, new StringValue("plain".getBytes(StandardCharsets.UTF_8)));
        array.set(1, new StringValue("Привет".getBytes(StandardCharsets.UTF_8)));
        array.set(2, null);
        return array;
    }

    @Test
    public void testAbstractReaderGetStringArrayFromStringValueItems() {
        TableSchema schema = new TableSchema(
                Collections.singletonList(ClickHouseColumn.of("a", "Array(String)")));
        InjectableReader reader = new InjectableReader(schema);
        reader.setCurrentRecord(new Object[]{stringValueArray()});

        String[] expected = {"plain", "Привет", null};
        Assert.assertEquals(reader.getStringArray(1), expected);
        Assert.assertEquals(reader.getStringArray("a"), expected);
    }

    @Test
    public void testMapBackedRecordGetStringArrayFromStringValueItems() {
        MapBackedRecord record = singleColumnRecord("a", "Array(String)", stringValueArray());

        String[] expected = {"plain", "Привет", null};
        Assert.assertEquals(record.getStringArray("a"), expected);
        Assert.assertEquals(record.getStringArray(1), expected);
    }

    // ---- MapBackedRecord.getByteArray edge cases ----

    @Test
    public void testMapBackedRecordGetByteArrayReturnsNullForNullValue() {
        // A null value must short-circuit before any string-like or primitive-array handling.
        MapBackedRecord record = singleColumnRecord("s", "String", null);
        Assert.assertNull(record.getByteArray("s"));
        Assert.assertNull(record.getByteArray(1));
    }

    @Test
    public void testMapBackedRecordGetByteArrayFromPrimitiveArray() {
        // A non-string, primitive Array(Int8) value is not string-like, so getByteArray falls through to
        // getPrimitiveArray and returns the backing byte[] (the branch the string-based tests never reach).
        BinaryStreamReader.ArrayValue array = new BinaryStreamReader.ArrayValue(byte.class, 3);
        array.set(0, (byte) 1);
        array.set(1, (byte) -2);
        array.set(2, (byte) 127);
        MapBackedRecord record = singleColumnRecord("b", "Array(Int8)", array);

        byte[] expected = {(byte) 1, (byte) -2, (byte) 127};
        Assert.assertEquals(record.getByteArray("b"), expected);
        Assert.assertEquals(record.getByteArray(1), expected);
    }

    // ---- Nested structures round-tripped through the real read path ----

    @Test
    public void testAbstractReaderArrayOfArrayOfString() throws IOException {
        List<List<String>> value = Arrays.asList(
                Arrays.asList("a", "b"),
                Collections.singletonList("c"),
                Collections.emptyList());
        RowBinaryWithNamesAndTypesFormatReader reader = readerForRow(
                new String[]{"a"}, new String[]{"Array(Array(String))"}, new Object[]{value}, true);

        Assert.assertEquals(reader.getList("a"), value);

        Object[] outer = reader.getObjectArray("a");
        Assert.assertEquals(outer.length, 3);
        Assert.assertEquals((Object[]) outer[0], new Object[]{"a", "b"});
        Assert.assertEquals((Object[]) outer[1], new Object[]{"c"});
        Assert.assertEquals((Object[]) outer[2], new Object[0]);
    }

    @Test
    public void testMapBackedRecordArrayOfArrayOfString() throws IOException {
        List<List<String>> value = Arrays.asList(
                Arrays.asList("a", "b"),
                Collections.singletonList("c"),
                Collections.emptyList());
        MapBackedRecord record = materializeRow(
                new String[]{"a"}, new String[]{"Array(Array(String))"}, new Object[]{value}, true);

        Assert.assertEquals(record.getList("a"), value);

        Object[] outer = record.getObjectArray("a");
        Assert.assertEquals(outer.length, 3);
        Assert.assertEquals((Object[]) outer[0], new Object[]{"a", "b"});
        Assert.assertEquals((Object[]) outer[1], new Object[]{"c"});
        Assert.assertEquals((Object[]) outer[2], new Object[0]);
    }

    @Test
    public void testAbstractReaderTupleStringIntString() throws IOException {
        List<Object> value = Arrays.asList("first", 7, "third");
        RowBinaryWithNamesAndTypesFormatReader reader = readerForRow(
                new String[]{"t"}, new String[]{"Tuple(String, Int32, String)"}, new Object[]{value}, true);

        Assert.assertEquals(reader.getTuple("t"), new Object[]{"first", 7, "third"});
        Assert.assertEquals(reader.getTuple(1), new Object[]{"first", 7, "third"});
    }

    @Test
    public void testMapBackedRecordTupleStringIntString() throws IOException {
        List<Object> value = Arrays.asList("first", 7, "third");
        MapBackedRecord record = materializeRow(
                new String[]{"t"}, new String[]{"Tuple(String, Int32, String)"}, new Object[]{value}, true);

        Assert.assertEquals(record.getTuple("t"), new Object[]{"first", 7, "third"});
        Assert.assertEquals(record.getTuple(1), new Object[]{"first", 7, "third"});
    }

    /**
     * Serializes a single row and returns a positioned {@link RowBinaryWithNamesAndTypesFormatReader}
     * (which is an {@link AbstractBinaryFormatReader}) so its accessors can be exercised over real data.
     */
    private static RowBinaryWithNamesAndTypesFormatReader readerForRow(String[] names, String[] types,
                                                                       Object[] values, boolean binaryStringSupport)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }
        for (int i = 0; i < names.length; i++) {
            SerializerUtils.serializeData(out, values[i], ClickHouseColumn.of(names[i], types[i]));
        }

        RowBinaryWithNamesAndTypesFormatReader reader = new RowBinaryWithNamesAndTypesFormatReader(
                new ByteArrayInputStream(out.toByteArray()),
                new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId()),
                new BinaryStreamReader.CachingByteBufferAllocator(), null, binaryStringSupport);
        Assert.assertNotNull(reader.next(), "Expected a row to be read");
        return reader;
    }

    private static MapBackedRecord singleColumnRecord(String name, String type, Object value) {
        TableSchema schema = new TableSchema(Collections.singletonList(ClickHouseColumn.of(name, type)));
        Map<String, Object> record = new HashMap<>();
        record.put(name, value);
        return new MapBackedRecord(record, new Map[]{null}, schema);
    }

    /**
     * Minimal concrete {@link AbstractBinaryFormatReader} that lets a test inject a materialized record
     * directly, so accessor branches can be exercised without a live binary stream.
     */
    private static final class InjectableReader extends AbstractBinaryFormatReader {
        InjectableReader(TableSchema schema) {
            super(new ByteArrayInputStream(new byte[0]),
                    new QuerySettings().setUseTimeZone("UTC"),
                    schema,
                    new BinaryStreamReader.DefaultByteBufferAllocator(),
                    NO_TYPE_HINT_MAPPING);
        }

        void setCurrentRecord(Object[] record) {
            this.currentRecord = record;
        }
    }
}
