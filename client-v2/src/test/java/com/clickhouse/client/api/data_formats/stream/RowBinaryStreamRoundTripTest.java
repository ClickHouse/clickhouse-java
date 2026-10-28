package com.clickhouse.client.api.data_formats.stream;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.StringValue;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.NullValueException;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.data.value.ClickHouseBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class RowBinaryStreamRoundTripTest {

    @Test
    public void scalarsRoundTripInTheMiddleOfTheRow() throws IOException {
        TableSchema schema = schema("id Int32", "name String", "tail Float64");
        byte[] streamed = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(7);
            row.writeString("abc");
            row.writeFloat64(1.5d);
        });
        byte[] buffered = buffered(schema, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 7);
            writer.setValue("name", "abc");
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(streamed, buffered);

        BinaryStreamReader reader = binary(streamed);
        Assert.assertEquals(reader.readValue(column(schema, 0)), Integer.valueOf(7));
        Assert.assertEquals(reader.readValue(column(schema, 1)), "abc");
        Assert.assertEquals(reader.readValue(column(schema, 2)), 1.5d);

        RowBinaryStreamReader streamReader = new RowBinaryStreamReader(
                new ByteArrayInputStream(streamed), schema, ClickHouseFormat.RowBinary);
        Assert.assertTrue(streamReader.hasNext());
        try (ValueReader row = streamReader.beginRow()) {
            Assert.assertEquals(row.readInt32(), 7);
            Assert.assertEquals(row.readString(), "abc");
            Assert.assertEquals(row.readFloat64(), 1.5d);
        }
        Assert.assertFalse(streamReader.hasNext());
    }

    @Test
    public void nullableStringAndNull() throws IOException {
        TableSchema schema = schema("id Int32", "name Nullable(String)", "tail Float64");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryStreamWriter writer = new RowBinaryStreamWriter(out, schema, ClickHouseFormat.RowBinary);
        try (ValueWriter row = writer.beginRow()) {
            row.writeInt32(1);
            row.writeString((String) null);
            row.writeFloat64(2.5d);
        }
        try (ValueWriter row = writer.beginRow()) {
            row.writeInt32(2);
            row.writeString("ok");
            row.writeFloat64(3.5d);
        }
        byte[] streamed = out.toByteArray();
        BinaryStreamReader reader = binary(streamed);
        Assert.assertEquals(reader.readValue(column(schema, 0)), Integer.valueOf(1));
        Assert.assertNull(reader.readValue(column(schema, 1)));
        Assert.assertEquals(reader.readValue(column(schema, 2)), 2.5d);
        Assert.assertEquals(reader.readValue(column(schema, 0)), Integer.valueOf(2));
        Assert.assertEquals(reader.readValue(column(schema, 1)), "ok");
        Assert.assertEquals(reader.readValue(column(schema, 2)), 3.5d);

        RowBinaryStreamReader streamReader = new RowBinaryStreamReader(
                new ByteArrayInputStream(streamed), schema, ClickHouseFormat.RowBinary);
        try (ValueReader row = streamReader.beginRow()) {
            Assert.assertEquals(row.readInt32(), 1);
            Assert.assertTrue(row.isNull());
            Assert.assertEquals(row.readFloat64(), 2.5d);
        }
        try (ValueReader row = streamReader.beginRow()) {
            Assert.assertEquals(row.readInt32(), 2);
            Assert.assertFalse(row.isNull());
            Assert.assertEquals(row.readString(), "ok");
            Assert.assertEquals(row.readFloat64(), 3.5d);
        }
    }

    @Test
    public void arraysKnownAndUnknownSizeMatchBufferedWriter() throws IOException {
        TableSchema schema = schema("id Int32", "items Array(Nullable(String))", "tail Float64");
        List<String> items = Arrays.asList("a", null, "b");
        byte[] unknown = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            try (ValueWriter array = row.beginArray()) {
                array.writeString("a");
                array.writeString((String) null);
                array.writeString("b");
            }
            row.writeFloat64(1.5d);
        });
        byte[] known = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            try (ValueWriter array = row.beginArray(items.size())) {
                for (String item : items) {
                    array.writeString(item);
                }
            }
            row.writeFloat64(1.5d);
        });
        byte[] fromBuffer = buffered(schema, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 1);
            writer.setValue("items", items);
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(unknown, known);
        Assert.assertEquals(known, fromBuffer);

        BinaryStreamReader reader = binary(known);
        Assert.assertEquals(reader.readValue(column(schema, 0)), Integer.valueOf(1));
        Assert.assertEquals(asList(reader.readValue(column(schema, 1))), items);
        Assert.assertEquals(reader.readValue(column(schema, 2)), 1.5d);
    }

    @Test
    public void nestedArraysMatchBufferedWriter() throws IOException {
        TableSchema schema = schema("id Int32", "matrix Array(Array(Nullable(String)))", "tail Float64");
        List<List<String>> matrix = Arrays.asList(
                Arrays.asList("a", null),
                Collections.<String>emptyList());
        byte[] unknown = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(4);
            try (ValueWriter outer = row.beginArray()) {
                try (ValueWriter inner = outer.beginArray()) {
                    inner.writeString("a");
                    inner.writeString((String) null);
                }
                try (ValueWriter inner = outer.beginArray(0)) {
                }
            }
            row.writeFloat64(1.5d);
        });
        byte[] known = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(4);
            try (ValueWriter outer = row.beginArray(2)) {
                try (ValueWriter inner = outer.beginArray(2)) {
                    inner.writeString("a");
                    inner.writeString((String) null);
                }
                try (ValueWriter inner = outer.beginArray()) {
                }
            }
            row.writeFloat64(1.5d);
        });
        byte[] fromBuffer = buffered(schema, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 4);
            writer.setValue("matrix", matrix);
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(unknown, known);
        Assert.assertEquals(known, fromBuffer);
        BinaryStreamReader reader = binary(known);
        Assert.assertEquals(reader.readValue(column(schema, 0)), Integer.valueOf(4));
        Assert.assertEquals(asList(reader.readValue(column(schema, 1))), matrix);
        Assert.assertEquals(reader.readValue(column(schema, 2)), 1.5d);
    }

    @Test
    public void tupleWithArrayInTheMiddle() throws IOException {
        TableSchema schema = schema("id Int32", "t Tuple(Int32, Array(Nullable(String)), Float64)", "tail Float64");
        byte[] streamed = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            try (ValueWriter tuple = row.beginTuple()) {
                tuple.writeInt32(9);
                try (ValueWriter array = tuple.beginArray(2)) {
                    array.writeString("x");
                    array.writeString((String) null);
                }
                tuple.writeFloat64(0.5d);
            }
            row.writeFloat64(1.5d);
        });
        byte[] fromBuffer = buffered(schema, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 1);
            writer.setValue("t", Arrays.asList(9, Arrays.asList("x", null), 0.5d));
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(streamed, fromBuffer);
        BinaryStreamReader reader = binary(streamed);
        Assert.assertEquals(reader.readValue(column(schema, 0)), Integer.valueOf(1));
        Object[] tuple = (Object[]) reader.readValue(column(schema, 1));
        Assert.assertEquals(tuple[0], 9);
        Assert.assertEquals(asList(tuple[1]), Arrays.asList("x", null));
        Assert.assertEquals(tuple[2], 0.5d);
        Assert.assertEquals(reader.readValue(column(schema, 2)), 1.5d);
    }

    @Test
    public void mapWithNestedArray() throws IOException {
        TableSchema schema = schema("id Int32", "m Map(String, Array(Int32))", "tail Float64");
        Map<String, List<Integer>> map = new LinkedHashMap<String, List<Integer>>();
        map.put("a", Arrays.asList(1, 2));
        map.put("b", Collections.<Integer>emptyList());
        byte[] streamed = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            try (ValueWriter values = row.beginMap(2)) {
                values.writeString("a");
                try (ValueWriter array = values.beginArray(2)) {
                    array.writeInt32(1);
                    array.writeInt32(2);
                }
                values.writeString("b");
                try (ValueWriter array = values.beginArray()) {
                }
            }
            row.writeFloat64(1.5d);
        });
        byte[] fromBuffer = buffered(schema, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 1);
            writer.setValue("m", map);
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(streamed, fromBuffer);
    }

    @Test
    public void unflattenedNested() throws IOException {
        TableSchema schema = schema("id Int32", "n Nested(n1 Int32, n2 Nullable(String))", "tail Float64");
        byte[] streamed = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            try (ValueWriter nested = row.beginNested(2)) {
                try (ValueWriter tuple = nested.beginTuple()) {
                    tuple.writeInt32(5);
                    tuple.writeString("a");
                }
                try (ValueWriter tuple = nested.beginTuple()) {
                    tuple.writeInt32(6);
                    tuple.writeString((String) null);
                }
            }
            row.writeFloat64(1.5d);
        });
        byte[] fromBuffer = buffered(schema, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 1);
            writer.setValue("n", Arrays.asList(Arrays.asList(5, "a"), Arrays.asList(6, null)));
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(streamed, fromBuffer);
        BinaryStreamReader reader = binary(streamed);
        Assert.assertEquals(reader.readValue(column(schema, 0)), Integer.valueOf(1));
        List<?> rows = asList(reader.readValue(column(schema, 1)));
        Assert.assertEquals(((Object[]) rows.get(0))[0], 5);
        Assert.assertEquals(((Object[]) rows.get(0))[1], "a");
        Assert.assertEquals(((Object[]) rows.get(1))[0], 6);
        Assert.assertNull(((Object[]) rows.get(1))[1]);
        Assert.assertEquals(reader.readValue(column(schema, 2)), 1.5d);
    }

    @Test
    public void qbitAndBulkArrayUseTheKnownSizePath() throws IOException {
        TableSchema qbit = schema("id Int32", "v QBit(Float64, 4)", "tail Float64");
        double[] vector = new double[] {1.0d, 2.0d, 3.0d, 4.0d};
        byte[] bulk = streamed(qbit, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            row.writeFloat64Array(vector);
            row.writeFloat64(1.5d);
        });
        byte[] cursor = streamed(qbit, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            try (ValueWriter values = row.beginQBit()) {
                for (double value : vector) {
                    values.writeFloat64(value);
                }
            }
            row.writeFloat64(1.5d);
        });
        byte[] fromBuffer = buffered(qbit, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 1);
            writer.setValue("v", vector);
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(bulk, cursor);
        Assert.assertEquals(cursor, fromBuffer);

        TableSchema ints = schema("id Int32", "v Array(Int32)", "tail Float64");
        int[] numbers = new int[] {4, 5, 6};
        byte[] bulkInts = streamed(ints, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            row.writeInt32Array(numbers);
            row.writeFloat64(1.5d);
        });
        byte[] loopInts = streamed(ints, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            try (ValueWriter array = row.beginArray(numbers.length)) {
                for (int number : numbers) {
                    array.writeInt32(number);
                }
            }
            row.writeFloat64(1.5d);
        });
        Assert.assertEquals(bulkInts, loopInts);
    }

    @Test
    public void defaultsPreamble() throws IOException {
        ClickHouseColumn withDefault = ClickHouseColumn.of("d", "Nullable(Int32)");
        withDefault.setHasDefault(true);
        withDefault.setDefaultValue(ClickHouseColumn.DefaultValue.DEFAULT);
        ClickHouseColumn nullable = ClickHouseColumn.of("n", "Nullable(Int32)");
        ClickHouseColumn plain = ClickHouseColumn.of("v", "Int32");
        TableSchema schema = new TableSchema(Arrays.asList(withDefault, nullable, plain));

        byte[] streamed = streamed(schema, ClickHouseFormat.RowBinaryWithDefaults, row -> {
            row.writeDefault();
            row.writeNull();
            row.writeInt32(3);
        });
        byte[] fromBuffer = buffered(schema, ClickHouseFormat.RowBinaryWithDefaults, writer -> {
            writer.setValue("d", null);
            writer.setValue("n", null);
            writer.setValue("v", 3);
        });
        Assert.assertEquals(streamed, fromBuffer);
        Assert.assertEquals(streamed, new byte[] {1, 0, 1, 0, 3, 0, 0, 0});

        byte[] value = streamed(schema, ClickHouseFormat.RowBinaryWithDefaults, row -> {
            row.writeInt32(4);
            row.writeInt32(5);
            row.writeInt32(6);
        });
        Assert.assertEquals(value[0], 0);
        Assert.assertEquals(value[1], 0);
    }

    @Test
    public void rawStringBytesRoundTrip() throws IOException {
        TableSchema schema = schema("id Int32", "raw String", "tail Float64");
        byte[] raw = new byte[] {(byte) 0xFF, 0x00, (byte) 0xFE};
        byte[] streamed = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            row.writeString(raw);
            row.writeFloat64(1.5d);
        });
        BinaryStreamReader reader = new BinaryStreamReader(new ByteArrayInputStream(streamed),
                TimeZone.getTimeZone("UTC"), null, new BinaryStreamReader.CachingByteBufferAllocator(),
                true, null, true);
        Assert.assertEquals(reader.readValue(column(schema, 0)), Integer.valueOf(1));
        Object value = reader.readValue(column(schema, 1));
        Assert.assertTrue(value instanceof StringValue);
        Assert.assertEquals(((StringValue) value).toByteArray(), raw);
        Assert.assertEquals(reader.readValue(column(schema, 2)), 1.5d);

        RowBinaryStreamReader streamReader = new RowBinaryStreamReader(
                new ByteArrayInputStream(streamed), schema, ClickHouseFormat.RowBinary);
        try (ValueReader row = streamReader.beginRow()) {
            Assert.assertEquals(row.readInt32(), 1);
            Assert.assertEquals(row.readStringValue().toByteArray(), raw);
            Assert.assertEquals(row.readFloat64(), 1.5d);
        }
    }

    @Test
    public void variantAndDynamic() throws IOException {
        TableSchema variantSchema = schema("id Int32", "v Variant(String, Int32)", "tail Float64");
        ClickHouseColumn variant = column(variantSchema, 1);
        int stringOrdinal = -1;
        for (int i = 0; i < variant.getNestedColumns().size(); i++) {
            if (variant.getNestedColumns().get(i).getDataType() == ClickHouseDataType.String) {
                stringOrdinal = i;
            }
        }
        Assert.assertTrue(stringOrdinal >= 0);
        int ordinal = stringOrdinal;
        byte[] streamed = streamed(variantSchema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            try (ValueWriter value = row.beginVariant(ordinal)) {
                value.writeString("hi");
            }
            row.writeFloat64(1.5d);
        });
        byte[] fromBuffer = buffered(variantSchema, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 1);
            writer.setValue("v", "hi");
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(streamed, fromBuffer);

        TableSchema dynamicSchema = schema("id Int32", "d Dynamic", "tail Float64");
        byte[] dynamicInt = streamed(dynamicSchema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            try (ValueWriter value = row.beginDynamic(ClickHouseColumn.of("v", "Int32"))) {
                value.writeInt32(7);
            }
            row.writeFloat64(1.5d);
        });
        byte[] dynamicFromBuffer = buffered(dynamicSchema, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 1);
            writer.setValue("d", 7);
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(dynamicInt, dynamicFromBuffer);

        byte[] dynamicArray = streamed(dynamicSchema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            try (ValueWriter value = row.beginDynamic(ClickHouseColumn.of("v", "Array(Nullable(String))"))) {
                try (ValueWriter array = value.beginArray(2)) {
                    array.writeString("a");
                    array.writeString((String) null);
                }
            }
            row.writeFloat64(1.5d);
        });
        byte[] dynamicArrayFromBuffer = buffered(dynamicSchema, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 1);
            writer.setValue("d", Arrays.asList("a", null));
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(dynamicArray, dynamicArrayFromBuffer);

        RowBinaryStreamReader reader = new RowBinaryStreamReader(
                new ByteArrayInputStream(dynamicInt), dynamicSchema, ClickHouseFormat.RowBinary);
        try (ValueReader row = reader.beginRow()) {
            Assert.assertEquals(row.readInt32(), 1);
            try (ValueReader value = row.beginDynamic()) {
                Assert.assertEquals(value.runtimeType().getDataType(), ClickHouseDataType.Int32);
                Assert.assertEquals(value.readInt32(), 7);
            }
            Assert.assertEquals(row.readFloat64(), 1.5d);
        }
    }

    @Test
    public void dynamicNullIsNothingAndIncompleteTypesAreRejected() throws IOException {
        TableSchema schema = schema("d Dynamic");
        byte[] streamed = streamed(schema, ClickHouseFormat.RowBinary, ValueWriter::writeNull);
        byte[] fromBuffer = buffered(schema, ClickHouseFormat.RowBinary, writer -> writer.setValue("d", null));
        Assert.assertEquals(streamed, fromBuffer);

        RowBinaryStreamReader reader = new RowBinaryStreamReader(
                new ByteArrayInputStream(streamed), schema, ClickHouseFormat.RowBinary);
        try (ValueReader row = reader.beginRow()) {
            try (ValueReader value = row.beginDynamic()) {
                Assert.assertEquals(value.runtimeType().getDataType(), ClickHouseDataType.Nothing);
            }
        }

        Assert.expectThrows(ClientException.class, () -> streamed(schema, ClickHouseFormat.RowBinary, row ->
                row.beginDynamic(ClickHouseColumn.of("v", "Tuple(Int32, String)"))));
        Assert.expectThrows(ClientException.class, () -> streamed(schema, ClickHouseFormat.RowBinary, row ->
                row.beginDynamic(ClickHouseColumn.of("v", "Array(Tuple(Int32))"))));
        Assert.expectThrows(ClientException.class, () -> streamed(schema, ClickHouseFormat.RowBinary, row ->
                row.beginDynamic(ClickHouseColumn.of("v", "QBit(Float64, 4)"))));
    }

    @Test
    public void jsonIsAStringAndStructuredWriteIsRejected() throws IOException {
        TableSchema schema = schema("id Int32", "j JSON", "tail Float64");
        byte[] streamed = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            row.writeString("{\"a\":1}");
            row.writeFloat64(1.5d);
        });
        byte[] fromBuffer = buffered(schema, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 1);
            writer.setValue("j", "{\"a\":1}");
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(streamed, fromBuffer);
        Assert.expectThrows(IllegalStateException.class, () -> streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            row.beginArray();
        }));
    }

    @Test
    public void groupBitmapRoundTrip() throws IOException {
        TableSchema schema = schema("id Int32", "b AggregateFunction(groupBitmap, UInt32)", "tail Float64");
        ClickHouseBitmap bitmap = ClickHouseBitmap.wrap(1, 2, 3);
        byte[] streamed = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            row.writeBitmap(bitmap);
            row.writeFloat64(1.5d);
        });
        byte[] fromBuffer = buffered(schema, ClickHouseFormat.RowBinary, writer -> {
            writer.setValue("id", 1);
            writer.setValue("b", bitmap);
            writer.setValue("tail", 1.5d);
        });
        Assert.assertEquals(streamed, fromBuffer);
        BinaryStreamReader reader = binary(streamed);
        Assert.assertEquals(reader.readValue(column(schema, 0)), Integer.valueOf(1));
        Assert.assertEquals(((ClickHouseBitmap) reader.readValue(column(schema, 1))).toBytes(), bitmap.toBytes());
        Assert.assertEquals(reader.readValue(column(schema, 2)), 1.5d);
    }

    @Test
    public void readerSkipsArrayWithoutLosingTheNextColumn() throws IOException {
        TableSchema schema = schema("id Int32", "items Array(Int32)", "tail Float64");
        int[] numbers = new int[32];
        for (int i = 0; i < numbers.length; i++) {
            numbers[i] = i;
        }
        byte[] streamed = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32(1);
            row.writeInt32Array(numbers);
            row.writeFloat64(1.5d);
        });
        RowBinaryStreamReader reader = new RowBinaryStreamReader(
                new ByteArrayInputStream(streamed), schema, ClickHouseFormat.RowBinary);
        try (ValueReader row = reader.beginRow()) {
            Assert.assertEquals(row.readInt32(), 1);
            row.skip();
            Assert.assertEquals(row.readFloat64(), 1.5d);
        }
    }

    @Test
    public void readIntoFillsCallerBuffer() throws IOException {
        TableSchema schema = schema("v Array(Int32)", "tail Float64");
        byte[] streamed = streamed(schema, ClickHouseFormat.RowBinary, row -> {
            row.writeInt32Array(new int[] {8, 9, 10});
            row.writeFloat64(1.5d);
        });
        RowBinaryStreamReader reader = new RowBinaryStreamReader(
                new ByteArrayInputStream(streamed), schema, ClickHouseFormat.RowBinary);
        try (ValueReader row = reader.beginRow()) {
            try (ValueReader array = row.beginArray()) {
                Assert.assertEquals(array.size(), 3);
                int[] dst = new int[3];
                Assert.assertEquals(array.readInto(dst, 0, dst.length), 3);
                Assert.assertEquals(dst, new int[] {8, 9, 10});
                Assert.assertFalse(array.hasNext());
            }
            Assert.assertEquals(row.readFloat64(), 1.5d);
        }
    }

    @Test
    public void namesAndTypesHeaderBuildsTheSchema() throws IOException {
        TableSchema schema = schema("id Int32", "name String");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeString(out, "id");
        BinaryStreamUtils.writeString(out, "name");
        BinaryStreamUtils.writeString(out, column(schema, 0).getOriginalTypeName());
        BinaryStreamUtils.writeString(out, column(schema, 1).getOriginalTypeName());
        try (ValueWriter row = new RowBinaryStreamWriter(out, schema, ClickHouseFormat.RowBinary).beginRow()) {
            row.writeInt32(3);
            row.writeString("z");
        }
        RowBinaryStreamReader reader = new RowBinaryStreamReader(
                new ByteArrayInputStream(out.toByteArray()), null, ClickHouseFormat.RowBinaryWithNamesAndTypes);
        Assert.assertEquals(reader.getSchema().getColumns().size(), 2);
        Assert.assertEquals(reader.getSchema().getColumns().get(0).getColumnName(), "id");
        Assert.assertEquals(reader.getSchema().getColumns().get(1).getDataType(), ClickHouseDataType.String);
        try (ValueReader row = reader.beginRow()) {
            Assert.assertEquals(row.readInt32(), 3);
            Assert.assertEquals(row.readString(), "z");
        }
    }

    @Test
    public void negativePaths() throws IOException {
        TableSchema arraySchema = schema("v Array(Int32)");
        Assert.expectThrows(IllegalStateException.class, () -> {
            RowBinaryStreamWriter writer = new RowBinaryStreamWriter(
                    new ByteArrayOutputStream(), arraySchema, ClickHouseFormat.RowBinary);
            ValueWriter row = writer.beginRow();
            ValueWriter array = row.beginArray();
            array.writeInt32(1);
            row.close();
        });
        Assert.expectThrows(IllegalStateException.class, () -> streamed(arraySchema, ClickHouseFormat.RowBinary, row -> {
            try (ValueWriter array = row.beginArray(2)) {
                array.writeInt32(1);
            }
        }));
        Assert.expectThrows(IllegalArgumentException.class, () ->
                streamed(schema("v Int32"), ClickHouseFormat.RowBinary, row -> row.writeInt32(1L << 40)));
        Assert.expectThrows(IllegalArgumentException.class, () ->
                streamed(schema("v Int32"), ClickHouseFormat.RowBinary, row -> row.writeNull()));
        Assert.expectThrows(IllegalStateException.class, () ->
                streamed(schema("v Array(Int32)"), ClickHouseFormat.RowBinaryWithDefaults, row -> {
                    try (ValueWriter array = row.beginArray()) {
                        array.writeDefault();
                    }
                }));
        Assert.expectThrows(IllegalArgumentException.class, () ->
                streamed(schema("v Int32"), ClickHouseFormat.RowBinaryWithDefaults, ValueWriter::writeDefault));
        Assert.expectThrows(IllegalStateException.class, () -> streamed(schema("m Map(String, Int32)"), ClickHouseFormat.RowBinary, row -> {
            try (ValueWriter map = row.beginMap(1)) {
                map.writeString("a");
            }
        }));
        Assert.expectThrows(IllegalArgumentException.class, () ->
                streamed(schema("v QBit(Float64, 4)"), ClickHouseFormat.RowBinary,
                        row -> row.writeFloat64Array(new double[] {1.0d})));
        Assert.expectThrows(NullValueException.class, () -> {
            byte[] bytes = streamed(schema("v Nullable(Int32)"), ClickHouseFormat.RowBinary, ValueWriter::writeNull);
            RowBinaryStreamReader reader = new RowBinaryStreamReader(
                    new ByteArrayInputStream(bytes), schema("v Nullable(Int32)"), ClickHouseFormat.RowBinary);
            try (ValueReader row = reader.beginRow()) {
                row.readInt32();
            }
        });
        Assert.expectThrows(IllegalStateException.class, () ->
                streamed(schema("a Int32", "b Int32"), ClickHouseFormat.RowBinary, row -> row.writeInt32(1)));
    }

    private static TableSchema schema(String... definitions) {
        List<ClickHouseColumn> columns = new ArrayList<ClickHouseColumn>();
        for (String definition : definitions) {
            int space = definition.indexOf(' ');
            columns.add(ClickHouseColumn.of(definition.substring(0, space), definition.substring(space + 1).trim()));
        }
        return new TableSchema(columns);
    }

    private static ClickHouseColumn column(TableSchema schema, int index) {
        return schema.getColumns().get(index);
    }

    private static byte[] streamed(TableSchema schema, ClickHouseFormat format, StreamingRow row) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryStreamWriter writer = new RowBinaryStreamWriter(out, schema, format);
        try (ValueWriter value = writer.beginRow()) {
            row.accept(value);
        }
        return out.toByteArray();
    }

    private static byte[] buffered(TableSchema schema, ClickHouseFormat format, BufferedRow row) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatWriter writer = new RowBinaryFormatWriter(out, schema, format);
        row.accept(writer);
        writer.commitRow();
        return out.toByteArray();
    }

    private static BinaryStreamReader binary(byte[] bytes) {
        return new BinaryStreamReader(new ByteArrayInputStream(bytes), TimeZone.getTimeZone("UTC"), null,
                new BinaryStreamReader.CachingByteBufferAllocator(), true, null, false);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object value) {
        if (value instanceof BinaryStreamReader.ArrayValue) {
            return ((BinaryStreamReader.ArrayValue) value).asList();
        }
        return (List<Object>) value;
    }

    private interface StreamingRow {
        void accept(ValueWriter row) throws IOException;
    }

    private interface BufferedRow {
        void accept(RowBinaryFormatWriter writer) throws IOException;
    }
}
