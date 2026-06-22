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
                // A present Nullable element of each datatype nested in a Tuple, with a trailing
                // non-nullable sibling whose bytes misalign if the present-marker is dropped.
                {"Tuple(Nullable(String), String)", Arrays.asList("opt", "tail")},
                {"Tuple(Nullable(FixedString(3)), String)", Arrays.asList("abc", "tail")},
                {"Tuple(Nullable(Int8), String)", Arrays.asList((byte) -5, "tail")},
                {"Tuple(Nullable(UInt8), String)", Arrays.asList((short) 200, "tail")},
                {"Tuple(Nullable(Int16), String)", Arrays.asList((short) -1600, "tail")},
                {"Tuple(Nullable(UInt16), String)", Arrays.asList(40000, "tail")},
                {"Tuple(Nullable(Int32), String)", Arrays.asList(42, "tail")},
                {"Tuple(Nullable(UInt32), String)", Arrays.asList(4_000_000_000L, "tail")},
                {"Tuple(Nullable(Int64), String)", Arrays.asList(-64L, "tail")},
                {"Tuple(Nullable(UInt64), String)", Arrays.asList(BigInteger.valueOf(64), "tail")},
                {"Tuple(Nullable(Float32), String)", Arrays.asList(1.5f, "tail")},
                {"Tuple(Nullable(Float64), String)", Arrays.asList(2.5d, "tail")},
                {"Tuple(Nullable(Bool), String)", Arrays.asList(true, "tail")},
                {"Tuple(Nullable(UUID), String)", Arrays.asList(uuid, "tail")},
                {"Tuple(Nullable(Date), String)", Arrays.asList(LocalDate.of(2021, 2, 3), "tail")},
                {"Tuple(Nullable(Decimal64(4)), String)", Arrays.asList(new BigDecimal("1.2345"), "tail")},
                {"Tuple(Nullable(IPv4), String)", Arrays.asList(ipv4, "tail")},

                // A Tuple value given as a Java array (not a List) takes the other branch of
                // serializeTupleData, which is routed through the same nested-marker path.
                {"Tuple(Nullable(String), String)", new Object[] {"opt", "tail"}},

                // The same marker handling on the Map value path, across a range of widths.
                {"Map(String, Nullable(String))", newMap("k", "v")},
                {"Map(String, Nullable(Int32))", newMap("k", 32)},
                {"Map(String, Nullable(Float64))", newMap("k", 2.5d)},
                {"Map(String, Nullable(UUID))", newMap("k", uuid)},

                // Null elements/values still serialize a single null-marker byte.
                {"Tuple(Nullable(String), String)", Arrays.asList(null, "tail")},
                {"Tuple(Nullable(Int32), String)", Arrays.asList(null, "tail")},
                {"Tuple(Nullable(Int32), Nullable(String))", Arrays.asList(null, null)},
                {"Map(String, Nullable(String))", newMap("k", null)},

                // Containers compose: marker handling threads through nested Tuple/Map/Array,
                // including Array(Tuple(Nullable)) which is how Nested columns are encoded.
                {"Tuple(String, Map(String, Nullable(String)))", Arrays.asList("id", newMap("k1", "v1", "k2", null))},
                {"Tuple(Nullable(String), Map(String, Nullable(Int32)))", Arrays.asList("opt", newMap("k", 7))},
                {"Array(Tuple(Nullable(String), String))", Arrays.asList(Arrays.asList("a", "b"), Arrays.asList(null, "c"))},
                {"Tuple(Array(Nullable(Int32)), String)", Arrays.asList(Arrays.asList(1, null, 3), "tail")},

                // Contrast: non-nullable nested elements must keep serializing without a marker.
                {"Tuple(Int32, String)", Arrays.asList(7, "tail")},
                {"Map(String, String)", newMap("k", "v")},
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
