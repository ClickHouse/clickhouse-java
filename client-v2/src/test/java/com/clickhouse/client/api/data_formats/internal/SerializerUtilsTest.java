package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

@Test(groups = {"unit"})
public class SerializerUtilsTest {
    private BinaryStreamReader newReader(byte[] data) {
        return new BinaryStreamReader(new ByteArrayInputStream(data), TimeZone.getTimeZone("UTC"), null,
                new BinaryStreamReader.DefaultByteBufferAllocator(), false, null);
    }

    @Test
    public void testGeometryRoundTrip() throws Exception {
        ClickHouseColumn geometry = ClickHouseColumn.of("v", "Geometry");
        double[] point = new double[] {1.5D, 2.5D};

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SerializerUtils.serializeData(out, point, geometry);

        Object value = newReader(out.toByteArray()).readValue(geometry);
        Assert.assertEquals((double[]) value, point);
    }

    @Test
    public void testGeometryRoundTripWithBoxedArray() throws Exception {
        ClickHouseColumn geometry = ClickHouseColumn.of("v", "Geometry");
        Double[][] ring = new Double[][] {{1D, 2D}, {3D, 4D}};

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SerializerUtils.serializeData(out, ring, geometry);

        Object value = newReader(out.toByteArray()).readValue(geometry);
        Assert.assertTrue(Arrays.deepEquals((double[][]) value, new double[][] {{1D, 2D}, {3D, 4D}}));
    }

    @Test
    public void testGeometryRoundTripWithPointList() throws Exception {
        ClickHouseColumn geometry = ClickHouseColumn.of("v", "Geometry");
        List<Double> point = Arrays.asList(1.5D, 2.5D);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SerializerUtils.serializeData(out, point, geometry);

        Object value = newReader(out.toByteArray()).readValue(geometry);
        Assert.assertEquals((double[]) value, new double[] {1.5D, 2.5D});
    }

    @Test
    public void testGeometryRoundTripWithPolygonList() throws Exception {
        ClickHouseColumn geometry = ClickHouseColumn.of("v", "Geometry");
        List<List<List<Double>>> polygon = Arrays.asList(
                Arrays.asList(
                        Arrays.asList(1D, 2D),
                        Arrays.asList(3D, 4D),
                        Arrays.asList(1D, 2D)));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SerializerUtils.serializeData(out, polygon, geometry);

        Object value = newReader(out.toByteArray()).readValue(geometry);
        Assert.assertTrue(Arrays.deepEquals((double[][][]) value, new double[][][] {{{1D, 2D}, {3D, 4D}, {1D, 2D}}}));
    }

    @Test
    public void testGeometryRoundTripWithMultiPolygonArray() throws Exception {
        ClickHouseColumn geometry = ClickHouseColumn.of("v", "Geometry");
        double[][][][] multiPolygon = new double[][][][] {{{{1D, 2D}, {3D, 4D}}}};

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SerializerUtils.serializeData(out, multiPolygon, geometry);

        Object value = newReader(out.toByteArray()).readValue(geometry);
        Assert.assertTrue(Arrays.deepEquals((double[][][][]) value, multiPolygon));
    }

    @Test
    public void testGeometryArrayDimensions() {
        Assert.assertEquals(SerializerUtils.getArrayDimensions(new Double[] {1D, 2D}), 1);
        Assert.assertEquals(SerializerUtils.getArrayDimensions(new Double[][] {{1D, 2D}}), 2);
        Assert.assertEquals(SerializerUtils.getArrayDimensions(new Double[][][] {{{1D, 2D}}}), 3);
        Assert.assertEquals(SerializerUtils.getArrayDimensions(new Double[][][][] {{{{1D, 2D}}}}), 4);
        Assert.assertEquals(SerializerUtils.getArrayDimensions(new Object[] {new Double[] {1D, 2D}}), 2);
        Assert.assertEquals(SerializerUtils.getArrayDimensions(new Object[] {null, new Object[] {new Double[] {1D, 2D}}}), 3);
        Assert.assertEquals(SerializerUtils.getArrayDimensions(new Object[] {null, null}), 1);
        Assert.assertEquals(SerializerUtils.getArrayDimensions(Arrays.asList(1D, 2D)), 1);
        Assert.assertEquals(SerializerUtils.getArrayDimensions(Arrays.asList(Arrays.asList(1D, 2D))), 2);
        Assert.assertEquals(SerializerUtils.getArrayDimensions(Arrays.asList(null, Arrays.asList(Arrays.asList(1D, 2D)))), 3);
        Assert.assertEquals(SerializerUtils.getArrayDimensions(Arrays.asList()), 1);
        Assert.assertEquals(SerializerUtils.getArrayDimensions("not an array"), -1);
        Assert.assertEquals(SerializerUtils.getArrayDimensions(null), -1);
    }

    @Test
    public void testDynamicWithGeoCustomTypeRoundTrip() throws Exception {
        ClickHouseColumn dynamic = ClickHouseColumn.of("v", "Dynamic");
        double[][][] polygon = new double[][][] {{{1D, 2D}, {3D, 4D}}};

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SerializerUtils.serializeData(out, ClickHouseGeoPolygonValue.of(polygon), dynamic);

        Object value = newReader(out.toByteArray()).readValue(dynamic);
        Assert.assertTrue(Arrays.deepEquals((double[][][]) value, polygon));
    }

    @Test
    public void testDynamicTypeTagUsesCustomEncodingForGeoTypes() throws Exception {
        assertCustomGeoTypeTag("LineString");
        assertCustomGeoTypeTag("MultiLineString");
        assertCustomGeoTypeTag("Geometry");
    }

    @Test
    public void testGeometrySerializationRejectsUnsupportedValue() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> SerializerUtils.serializeData(new ByteArrayOutputStream(), "not-geometry",
                        ClickHouseColumn.of("v", "Geometry")));
    }

    @Test
    public void testGeometrySerializationRejectsMalformedList() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> SerializerUtils.serializeData(new ByteArrayOutputStream(),
                        Arrays.asList(Arrays.asList(1D, 2D, 3D)),
                        ClickHouseColumn.of("v", "Geometry")));
    }

    @Test(dataProvider = "nonNullableEnumTypes")
    public void testNullIntoNonNullableEnumThrowsIllegalArgument(String typeName) {
        ClickHouseColumn column = ClickHouseColumn.of("bs_flag", typeName);

        IllegalArgumentException ex = Assert.expectThrows(IllegalArgumentException.class,
                () -> SerializerUtils.serializeData(new ByteArrayOutputStream(), null, column));
        String message = ex.getMessage();
        Assert.assertTrue(message.contains("Cannot write NULL into non-nullable Enum column"),
                "Unexpected message: " + message);
        Assert.assertTrue(message.contains("bs_flag"),
                "Message should name the offending column: " + message);
        Assert.assertTrue(message.contains(typeName),
                "Message should include the enum type: " + message);
    }

    @DataProvider(name = "nonNullableEnumTypes")
    private Object[][] nonNullableEnumTypes() {
        return new Object[][] {
                {"Enum8('B' = 1, 'S' = 2)"},
                {"Enum16('B' = 1, 'S' = 2)"},
        };
    }

    @Test
    public void testEnumSerializationUnaffectedByNullGuard() throws Exception {
        // A Nullable(Enum) with null still takes the early null-marker path and never reaches
        // enum serialization, so a single null-marker byte is written.
        ByteArrayOutputStream nullableOut = new ByteArrayOutputStream();
        SerializerUtils.serializeData(nullableOut, null,
                ClickHouseColumn.of("v", "Nullable(Enum8('B' = 1, 'S' = 2))"));
        Assert.assertEquals(nullableOut.toByteArray(), new byte[] {1});

        // A present value in a non-nullable Enum column still serializes to its mapped numeric value.
        ByteArrayOutputStream valueOut = new ByteArrayOutputStream();
        SerializerUtils.serializeData(valueOut, "S", ClickHouseColumn.of("v", "Enum8('B' = 1, 'S' = 2)"));
        Assert.assertEquals(valueOut.toByteArray(), new byte[] {2});
    }

    @Test(dataProvider = "nestedNullableData")
    public void testNestedNullableRoundTrip(String typeName, Object value) throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("v", typeName);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SerializerUtils.serializeData(out, value, column);

        Object actual = newReader(out.toByteArray()).readValue(column);
        Assert.assertEquals(normalize(actual), normalize(value));
    }

    @DataProvider(name = "nestedNullableData")
    private Object[][] nestedNullableData() throws Exception {
        UUID uuid = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        InetAddress ipv4 = InetAddress.getByName("1.2.3.4");
        return new Object[][] {
                // Each present Nullable element sits in the MIDDLE of the schema: a non-nullable
                // leading column, the Nullable, then a trailing non-nullable Float64. If the
                // present-marker byte is dropped, every following byte shifts and the trailing
                // Float64 reads a wrong value, so a faulty serialization is detected positionally
                // rather than only by running out of bytes. The assertion compares the whole row.
                {"Tuple(Int32, Nullable(String), Float64)", Arrays.asList(7, "opt", 9.5d)},
                {"Tuple(Int32, Nullable(FixedString(3)), Float64)", Arrays.asList(7, "abc", 9.5d)},
                {"Tuple(Int32, Nullable(Int8), Float64)", Arrays.asList(7, (byte) -5, 9.5d)},
                {"Tuple(Int32, Nullable(UInt8), Float64)", Arrays.asList(7, (short) 200, 9.5d)},
                {"Tuple(Int32, Nullable(Int16), Float64)", Arrays.asList(7, (short) -1600, 9.5d)},
                {"Tuple(Int32, Nullable(UInt16), Float64)", Arrays.asList(7, 40000, 9.5d)},
                {"Tuple(Int32, Nullable(Int32), Float64)", Arrays.asList(7, 42, 9.5d)},
                {"Tuple(Int32, Nullable(UInt32), Float64)", Arrays.asList(7, 4_000_000_000L, 9.5d)},
                {"Tuple(Int32, Nullable(Int64), Float64)", Arrays.asList(7, -64L, 9.5d)},
                {"Tuple(Int32, Nullable(UInt64), Float64)", Arrays.asList(7, BigInteger.valueOf(64), 9.5d)},
                {"Tuple(Int32, Nullable(Float32), Float64)", Arrays.asList(7, 1.5f, 9.5d)},
                {"Tuple(Int32, Nullable(Float64), Float64)", Arrays.asList(7, 2.5d, 9.5d)},
                {"Tuple(Int32, Nullable(Bool), Float64)", Arrays.asList(7, true, 9.5d)},
                {"Tuple(Int32, Nullable(UUID), Float64)", Arrays.asList(7, uuid, 9.5d)},
                {"Tuple(Int32, Nullable(Date), Float64)", Arrays.asList(7, LocalDate.of(2021, 2, 3), 9.5d)},
                {"Tuple(Int32, Nullable(Decimal64(4)), Float64)", Arrays.asList(7, new BigDecimal("1.2345"), 9.5d)},
                {"Tuple(Int32, Nullable(IPv4), Float64)", Arrays.asList(7, ipv4, 9.5d)},

                // A Tuple value given as a Java array (not a List) takes the other branch of
                // serializeTupleData, which is routed through the same nested-marker path, for
                // both a present value and a null.
                {"Tuple(Int32, Nullable(String), Float64)", new Object[] {7, "opt", 9.5d}},
                {"Tuple(Int32, Nullable(String), Float64)", new Object[] {7, null, 9.5d}},

                // The Map value path: the Nullable map value sits between the key and a trailing
                // Float64, so a dropped value-marker misaligns the float.
                {"Tuple(Int32, Map(String, Nullable(String)), Float64)", Arrays.asList(7, newMap("k", "v"), 9.5d)},
                {"Tuple(Int32, Map(String, Nullable(Int32)), Float64)", Arrays.asList(7, newMap("k", 32), 9.5d)},
                {"Tuple(Int32, Map(String, Nullable(Float64)), Float64)", Arrays.asList(7, newMap("k", 2.5d), 9.5d)},
                {"Tuple(Int32, Map(String, Nullable(UUID)), Float64)", Arrays.asList(7, newMap("k", uuid), 9.5d)},

                // Null elements/values still serialize a single null-marker byte; the trailing
                // Float64 confirms the following data stays aligned.
                {"Tuple(Int32, Nullable(String), Float64)", Arrays.asList(7, null, 9.5d)},
                {"Tuple(Int32, Nullable(Int32), Nullable(String), Float64)", Arrays.asList(7, null, null, 9.5d)},
                {"Tuple(Int32, Map(String, Nullable(String)), Float64)", Arrays.asList(7, newMap("k", null), 9.5d)},

                // Containers compose: marker handling threads through nested Tuple/Map/Array,
                // including Array(Tuple(Nullable)) which is how Nested columns are encoded. A
                // trailing Float64 after each nested container detects misalignment.
                {"Array(Tuple(Int32, Nullable(String), Float64))",
                        Arrays.asList(Arrays.asList(7, "a", 9.5d), Arrays.asList(7, null, 8.5d))},
                {"Tuple(String, Map(String, Nullable(Int32)), Float64)",
                        Arrays.asList("id", newMap("k1", 7, "k2", null), 9.5d)},
                {"Tuple(Array(Nullable(Int32)), Float64)", Arrays.asList(Arrays.asList(1, null, 3), 9.5d)},

                // Contrast: non-nullable nested elements must keep serializing without a marker,
                // so these rows round-trip identically with or without the fix.
                {"Tuple(Int32, String, Float64)", Arrays.asList(7, "tail", 9.5d)},
                {"Tuple(Int32, Map(String, String), Float64)", Arrays.asList(7, newMap("k", "v"), 9.5d)},
        };
    }

    // Normalizes Tuple (Object[]) and Array (ArrayValue / List) results to nested Lists so
    // round-tripped values compare structurally regardless of the container representation the
    // reader returns.
    @SuppressWarnings("unchecked")
    private static Object normalize(Object value) {
        if (value instanceof BinaryStreamReader.ArrayValue) {
            return normalizeList(((BinaryStreamReader.ArrayValue) value).asList());
        } else if (value instanceof Object[]) {
            return normalizeList(Arrays.asList((Object[]) value));
        } else if (value instanceof List) {
            return normalizeList((List<Object>) value);
        } else if (value instanceof Map) {
            Map<Object, Object> result = new LinkedHashMap<>();
            ((Map<Object, Object>) value).forEach((k, v) -> result.put(k, normalize(v)));
            return result;
        }
        return value;
    }

    private static List<Object> normalizeList(List<Object> values) {
        List<Object> result = new ArrayList<>(values.size());
        for (Object v : values) {
            result.add(normalize(v));
        }
        return result;
    }

    private static Map<Object, Object> newMap(Object... kv) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i], kv[i + 1]);
        }
        return map;
    }

    private void assertCustomGeoTypeTag(String typeName) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SerializerUtils.writeDynamicTypeTag(out, ClickHouseColumn.of("v", typeName));

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        Assert.assertEquals(in.read(), ClickHouseDataType.CUSTOM_TYPE_BIN_TAG & 0xFF);
        Assert.assertEquals(BinaryStreamReader.readString(in), typeName);
    }
}
