package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

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

    @Test
    public void testTupleWithNullableElementsRoundTrip() throws Exception {
        // Regression for #2721: a Nullable element nested inside a Tuple must be prefixed with its
        // null-marker byte (0x00 present, 0x01 null). Before the fix the present-value marker was
        // missing, so the reader mis-parsed the bytes that followed. The non-nullable sibling element
        // must keep being written without a marker.
        ClickHouseColumn column = ClickHouseColumn.of("v", "Tuple(Nullable(String), String)");

        ByteArrayOutputStream present = new ByteArrayOutputStream();
        SerializerUtils.serializeData(present, Arrays.asList("optional-value", "value-2"), column);
        Assert.assertEquals((Object[]) newReader(present.toByteArray()).readValue(column),
                new Object[] {"optional-value", "value-2"});

        ByteArrayOutputStream absent = new ByteArrayOutputStream();
        SerializerUtils.serializeData(absent, Arrays.asList(null, "value-2"), column);
        Assert.assertEquals((Object[]) newReader(absent.toByteArray()).readValue(column),
                new Object[] {null, "value-2"});
    }

    @Test
    public void testMapWithNullableValuesRoundTrip() throws Exception {
        // Regression for #2721: a Nullable Map value must be prefixed with its null-marker byte.
        ClickHouseColumn column = ClickHouseColumn.of("v", "Map(String, Nullable(String))");
        Map<String, String> value = new LinkedHashMap<>();
        value.put("k1", "v1");
        value.put("k2", null);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SerializerUtils.serializeData(out, value, column);

        Map<?, ?> result = newReader(out.toByteArray()).readValue(column);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testTupleWithNullableFixedWidthElementsRoundTrip() throws Exception {
        // #2721 is about the marker byte, not the element type: a fixed-width Nullable element
        // (here Int32) must also get its marker so the following value bytes are not misaligned.
        // Also covers an all-null tuple.
        ClickHouseColumn column = ClickHouseColumn.of("v", "Tuple(Nullable(Int32), Nullable(String))");

        ByteArrayOutputStream present = new ByteArrayOutputStream();
        SerializerUtils.serializeData(present, Arrays.asList(42, "x"), column);
        Assert.assertEquals((Object[]) newReader(present.toByteArray()).readValue(column),
                new Object[] {42, "x"});

        ByteArrayOutputStream allNull = new ByteArrayOutputStream();
        SerializerUtils.serializeData(allNull, Arrays.asList(null, null), column);
        Assert.assertEquals((Object[]) newReader(allNull.toByteArray()).readValue(column),
                new Object[] {null, null});
    }

    @Test
    public void testNestedContainerWithNullableRoundTrip() throws Exception {
        // #2721: the marker handling must compose through nested containers — here a Map carrying a
        // Nullable value sits inside a Tuple, exercising serializeTupleData -> serializeMapData.
        ClickHouseColumn column = ClickHouseColumn.of("v", "Tuple(String, Map(String, Nullable(String)))");
        Map<String, String> inner = new LinkedHashMap<>();
        inner.put("k1", "v1");
        inner.put("k2", null);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SerializerUtils.serializeData(out, Arrays.asList("id", inner), column);

        Assert.assertEquals((Object[]) newReader(out.toByteArray()).readValue(column),
                new Object[] {"id", inner});
    }

    private void assertCustomGeoTypeTag(String typeName) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SerializerUtils.writeDynamicTypeTag(out, ClickHouseColumn.of("v", typeName));

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        Assert.assertEquals(in.read(), ClickHouseDataType.CUSTOM_TYPE_BIN_TAG & 0xFF);
        Assert.assertEquals(BinaryStreamReader.readString(in), typeName);
    }
}
