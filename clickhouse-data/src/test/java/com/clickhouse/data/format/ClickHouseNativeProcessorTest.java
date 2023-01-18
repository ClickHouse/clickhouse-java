package com.clickhouse.data.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.function.Consumer;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseTestDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.value.BaseDataProcessorTest;

public class ClickHouseNativeProcessorTest extends BaseDataProcessorTest {
    private ClickHouseNativeProcessor newProcessor(int... bytes) throws IOException {
        return new ClickHouseNativeProcessor(new ClickHouseTestDataConfig(), BinaryStreamUtilsTest.generateInput(bytes),
                null, null, null);
    }

    private void verifySingleRecord(ClickHouseDataProcessor p, Object expectedValue) {
        int rows = 0;
        for (ClickHouseRecord r : p.records()) {
            Assert.assertEquals(r.getValue(0).asObject(), expectedValue);
            rows++;
        }
        Assert.assertEquals(rows, 1);
    }

    @Override
    protected ClickHouseDataProcessor getDataProcessor(ClickHouseDataConfig config, ClickHouseColumn column,
            ClickHouseInputStream input, ClickHouseOutputStream output) throws IOException {
        return new ClickHouseNativeProcessor(config, input, output, Collections.singletonList(column), null);
    }

    // @Override
    // protected ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseConfig
    // config,
    // ClickHouseColumn column, ClickHouseInputStream input) throws IOException {
    // if (ref == null) {
    // ref = column.newValue(config);
    // }
    // return new ClickHouseRowBinaryProcessor(config, input, null,
    // Collections.singletonList(column), null)
    // .getDeserializer(config, column).deserialize(ref, input);
    // }

    // @Override
    // protected void serialize(ClickHouseValue value, ClickHouseConfig config,
    // ClickHouseColumn column,
    // ClickHouseOutputStream output) throws IOException {
    // ClickHouseNativeProcessor.getMappedFunctions().serialize(value, config,
    // column, output);
    // }

    @Override
    protected byte[] getRawData(String typeName, String key) {
        byte[] data = null;
        byte[] header = new byte[] { 1, 1, 0, 0 };
        // toBytes(new byte[] { 0x01, 0x01, 0x78 }, new byte[] { (byte)
        // typeName.length() },typeName.getBytes());
        boolean isLowCardinality = typeName.startsWith("LowCardinality(");
        if (isLowCardinality) {
            typeName = typeName.replaceFirst("LowCardinality\\(", "").replaceFirst("\\)", "");
        }
        boolean isNull = "null".equals(key);
        boolean isNullable = typeName.startsWith("Nullable(");
        if (isNullable) {
            typeName = typeName.replaceFirst("Nullable\\(", "").replaceFirst("\\)", "");
            if (isNull) {
                if (!isLowCardinality) {
                    return toBytes(header, 1);
                }
            } else {
                if (!isLowCardinality) {
                    header = toBytes(header, 0);
                }
            }
        }
        if (isLowCardinality) {
            header = toBytes(header, 1, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0);
        }
        switch (typeName) {
            case "Array(Bool)":
                if ("0,1".equals(key)) {
                    data = toBytes("[false,true]");
                }
                break;
            case "Array(Int8)":
            case "Array(Int16)":
            case "Array(Int32)":
            case "Array(Int64)":
            case "Array(Float32)":
            case "Array(Float64)":
                if ("0,1,-1".equals(key)) {
                    data = toBytes("0,1,-1");
                }
                break;
            case "Array(UInt8)":
                if ("0,1,-1".equals(key)) {
                    data = toBytes("0,1,255");
                }
                break;
            case "Array(UInt16)":
                if ("0,1,-1".equals(key)) {
                    data = toBytes("0,1,65535");
                }
                break;
            case "Array(UInt32)":
                if ("0,1,-1".equals(key)) {
                    data = toBytes("0,1,4294967295");
                }
                break;
            case "Array(UInt64)":
                if ("0,1,-1".equals(key)) {
                    data = toBytes("0,1,18446744073709551615");
                }
                break;
            case "Array(String)":
                if ("4bcd".equals(key)) {
                    data = toBytes("['','B','BC','BCD']");
                }
                break;
            case "Bool":
            case "Int8":
            case "UInt8":
                if (isLowCardinality) {
                    byte b = isNull ? 0 : Byte.parseByte(key);
                    int base = isNullable ? 1 : 0;
                    data = toBytes(header, toBytes(base + (b == 0 ? 1 : 2), 0, 0, 0, 0, 0, 0, 0, 0),
                            new byte[isNullable ? 1 : 0], b == 0 ? new byte[0] : new byte[] { b },
                            toBytes(1, 0, 0, 0, 0, 0, 0, 0, isNull ? 0 : (base + (b == 0 ? 0 : 1))));
                } else {
                    data = toBytes(header, new byte[] { Byte.parseByte(key) });
                }
                break;
            case "Int16":
            case "UInt16":
                if (isLowCardinality) {
                    short s = isNull ? 0 : Short.parseShort(key);
                    int base = isNullable ? 1 : 0;
                    data = toBytes(header, toBytes(base + (s == 0 ? 1 : 2), 0, 0, 0, 0, 0, 0, 0, 0, 0),
                            new byte[isNullable ? 2 : 0], s == 0 ? new byte[0] : toLEBytes(s),
                            toBytes(1, 0, 0, 0, 0, 0, 0, 0, isNull ? 0 : (base + (s == 0 ? 0 : 1))));
                } else {
                    data = toBytes(header, toLEBytes(Short.parseShort(key)));
                }
                break;
            case "Int32":
            case "UInt32":
                if (isLowCardinality) {
                    int i = isNull ? 0 : Integer.parseInt(key);
                    int base = isNullable ? 1 : 0;
                    data = toBytes(header,
                            toBytes(base + (i == 0 ? 1 : 2), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
                            new byte[isNullable ? 4 : 0], i == 0 ? new byte[0] : toLEBytes(i),
                            toBytes(1, 0, 0, 0, 0, 0, 0, 0, isNull ? 0 : (base + (i == 0 ? 0 : 1))));
                } else {
                    data = toBytes(header, toLEBytes(Integer.parseInt(key)));
                }
                break;
            case "Int64":
            case "UInt64":
                if (isLowCardinality) {
                    long l = isNull ? 0L : Long.parseLong(key);
                    int base = isNullable ? 1 : 0;
                    data = toBytes(header,
                            toBytes(base + (l == 0L ? 1 : 2), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
                            new byte[isNullable ? 8 : 0], l == 0L ? new byte[0] : toLEBytes(l),
                            toBytes(1, 0, 0, 0, 0, 0, 0, 0, isNull ? 0 : (base + (l == 0L ? 0 : 1))));
                } else {
                    data = toBytes(header, toLEBytes(Long.parseLong(key)));
                }
                break;
            case "Float32":
                if (isLowCardinality) {
                    float f = isNull ? 0F : Float.parseFloat(key);
                    int base = isNullable ? 1 : 0;
                    data = toBytes(header,
                            toBytes(base + (f == 0F ? 1 : 2), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
                            new byte[isNullable ? 4 : 0], f == 0F ? new byte[0] : toLEBytes(f),
                            toBytes(1, 0, 0, 0, 0, 0, 0, 0, isNull ? 0 : (base + (f == 0F ? 0 : 1))));
                } else {
                    data = toBytes(header, toLEBytes(Float.parseFloat(key)));
                }
                break;
            case "Float64":
                if (isLowCardinality) {
                    double d = isNull ? 0D : Double.parseDouble(key);
                    int base = isNullable ? 1 : 0;
                    data = toBytes(header,
                            toBytes(base + (d == 0D ? 1 : 2), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
                            new byte[isNullable ? 8 : 0], d == 0D ? new byte[0] : toLEBytes(d),
                            toBytes(1, 0, 0, 0, 0, 0, 0, 0, isNull ? 0 : (base + (d == 0D ? 0 : 1))));
                } else {
                    data = toBytes(header, toLEBytes(Double.parseDouble(key)));
                }
                break;
            case "String":
                if (isLowCardinality) {
                    int index = isNull ? 0 : 1;
                    int base = isNullable ? 1 : 0;
                    data = toBytes(header,
                            toBytes(base + (index == 0 ? 1 : 2), 0, 0, 0, 0, 0, 0, 0, 0),
                            new byte[isNullable ? 1 : 0], index == 0 ? new byte[0] : toLEBytes(key),
                            toBytes(1, 0, 0, 0, 0, 0, 0, 0, isNull ? 0 : (base + (index == 0 ? 0 : 1))));
                } else {
                    data = toBytes(header, toLEBytes(key));
                }
                break;
            default:
                break;
        }

        if (data == null) {
            Assert.fail(String.format("No raw data defined for type=[%s] and key=[%s]", typeName, key));
        }
        return data;
    }

    @Test(dataProvider = "simpleTypesForWrite", groups = { "unit" })
    public void testSerializeSimpleTypes(ClickHouseDataConfig config, String typeName, String dataKey,
            ClickHouseValue... values) throws IOException {
        if (values == null || values.length == 0) {
            Assert.fail("At least one value must be provided for testing");
        }

        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        for (ClickHouseValue value : values) {
            bao.reset();
            try (ClickHouseOutputStream output = ClickHouseOutputStream.of(bao)) {
                serialize(value, config, ClickHouseColumn.of("a", typeName), output);
            }
            Assert.assertEquals(bao.toByteArray(), getRawData(typeName, dataKey));

            bao.reset();
            String lowCardinalityTypeName = "LowCardinality(" + typeName + ")";
            try (ClickHouseOutputStream output = ClickHouseOutputStream.of(bao)) {
                serialize(value, config, ClickHouseColumn.of("a", lowCardinalityTypeName), output);
            }
            Assert.assertEquals(bao.toByteArray(), getRawData(lowCardinalityTypeName, dataKey));
        }
    }

    @Test(dataProvider = "simpleTypesForRead", groups = { "unit" })
    public void testDeserializeSimpleTypes(ClickHouseDataConfig config, String typeName, String dataKey,
            Class<?> valueClass, Object objVal, String strVal, Consumer<ClickHouseValue> customChecks)
            throws IOException {
        try (ClickHouseInputStream in = getInputData(typeName, dataKey)) {
            ClickHouseValue value = deserialize(null, config, ClickHouseColumn.of("a", typeName), in);
            Assert.assertEquals(value.getClass(), valueClass);
            Assert.assertEquals(value.asObject(), objVal);
            Assert.assertEquals(value.asString(), strVal);
            if (customChecks != null) {
                customChecks.accept(value);
            }
        }

        // String lowCardinalityTypeName = "LowCardinality(" + typeName + ")";
        // try (ClickHouseInputStream in = getInputData(lowCardinalityTypeName,
        // dataKey)) {
        // ClickHouseValue value = deserialize(null, config, ClickHouseColumn.of("a",
        // lowCardinalityTypeName), in);
        // Assert.assertEquals(value.getClass(), valueClass);
        // Assert.assertEquals(value.asObject(), objVal);
        // Assert.assertEquals(value.asString(), strVal);
        // if (customChecks != null) {
        // customChecks.accept(value);
        // }
        // }
    }

    @Test(groups = { "unit" })
    public void testDeserializeArray() throws IOException {
        ClickHouseNativeProcessor p = newProcessor(0x02, 0x01, 0x01, 0x62, 0x06, 0x53, 0x74, 0x72, 0x69, 0x6E,
                0x67, 0x04, 0x61, 0x31, 0x32, 0x33, 0x01, 0x63, 0x05, 0x55, 0x49, 0x6E, 0x74, 0x38, 0x01);

        Assert.assertNotNull(p.getColumns());

        // select 1::LowCardinality(Int8) a
        p = newProcessor(0x01, 0x01, 0x01, 0x61, 0x14, 0x4C, 0x6F, 0x77, 0x43, 0x61, 0x72, 0x64, 0x69, 0x6E, 0x61, 0x6C,
                0x69, 0x74, 0x79, 0x28, 0x49, 0x6e, 0x74, 0x38, 0x29, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01);
        Assert.assertNotNull(p.getColumns());

        // select 1::LowCardinality(String) a
        p = newProcessor(0x01, 0x01, 0x01, 0x61, 0x16, 0x4C, 0x6F, 0x77, 0x43, 0x61, 0x72, 0x64, 0x69, 0x6E,
                0x61, 0x6C, 0x69, 0x74, 0x79, 0x28, 0x53, 0x74, 0x72, 0x69, 0x6E, 0x67, 0x29, 0x01, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x01, 0x31, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01);
        Assert.assertNotNull(p.getColumns());
    }

    @Test(groups = { "unit" })
    public void testArray() throws IOException {
        // [1,2,3]::Array(Int8)
        ClickHouseNativeProcessor p = newProcessor(0x01, 0x01, 0x1E, 0x43, 0x41, 0x53, 0x54, 0x28, 0x27, 0x5B, 0x31,
                0x2C, 0x32, 0x2C, 0x33, 0x5D, 0x27, 0x2C, 0x20, 0x27, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x49, 0x6E,
                0x74, 0x38, 0x29, 0x27, 0x29, 0x0B, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x49, 0x6E, 0x74, 0x38, 0x29,
                0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03);
        verifySingleRecord(p, new byte[] { 1, 2, 3 });

        // [1,2,3]::Array(Nullable(Int8))
        p = newProcessor(0x01, 0x01, 0x28, 0x43, 0x41, 0x53, 0x54, 0x28, 0x27, 0x5B, 0x31, 0x2C, 0x32, 0x2C, 0x33, 0x5D,
                0x27, 0x2C, 0x20, 0x27, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x4E, 0x75, 0x6C, 0x6C, 0x61, 0x62, 0x6C,
                0x65, 0x28, 0x49, 0x6E, 0x74, 0x38, 0x29, 0x29, 0x27, 0x29, 0x15, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28,
                0x4E, 0x75, 0x6C, 0x6C, 0x61, 0x62, 0x6C, 0x65, 0x28, 0x49, 0x6E, 0x74, 0x38, 0x29, 0x29, 0x03, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03);
        verifySingleRecord(p, new Byte[] { 1, 2, 3 });

        // [1,null,3]::Array(Nullable(Int8))
        p = newProcessor(0x01, 0x01, 0x2B, 0x43, 0x41, 0x53, 0x54, 0x28, 0x5B, 0x31, 0x2C, 0x20, 0x4E, 0x55, 0x4C, 0x4C,
                0x2C, 0x20, 0x33, 0x5D, 0x2C, 0x20, 0x27, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x4E, 0x75, 0x6C, 0x6C,
                0x61, 0x62, 0x6C, 0x65, 0x28, 0x49, 0x6E, 0x74, 0x38, 0x29, 0x29, 0x27, 0x29, 0x15, 0x41, 0x72, 0x72,
                0x61, 0x79, 0x28, 0x4E, 0x75, 0x6C, 0x6C, 0x61, 0x62, 0x6C, 0x65, 0x28, 0x49, 0x6E, 0x74, 0x38, 0x29,
                0x29, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x03);
        verifySingleRecord(p, new Byte[] { 1, null, 3 });

        // [[1,2],[],[null,4,5]]::Array(Array(Nullable(Int8)))
        p = newProcessor(0x01, 0x01, 0x40, 0x43, 0x41, 0x53, 0x54, 0x28, 0x5B, 0x5B, 0x31, 0x2C, 0x20, 0x32, 0x5D, 0x2C,
                0x20, 0x5B, 0x5D, 0x2C, 0x20, 0x5B, 0x4E, 0x55, 0x4C, 0x4C, 0x2C, 0x20, 0x34, 0x2C, 0x20, 0x35, 0x5D,
                0x5D, 0x2C, 0x20, 0x27, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x4E,
                0x75, 0x6C, 0x6C, 0x61, 0x62, 0x6C, 0x65, 0x28, 0x49, 0x6E, 0x74, 0x38, 0x29, 0x29, 0x29, 0x27, 0x29,
                0x1C, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x4E, 0x75, 0x6C, 0x6C,
                0x61, 0x62, 0x6C, 0x65, 0x28, 0x49, 0x6E, 0x74, 0x38, 0x29, 0x29, 0x29, 0x03, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x02,
                0x00, 0x04, 0x05);
        verifySingleRecord(p, new Byte[][] { { 1, 2 }, {}, { null, 4, 5 } });

        // [[[null],[1,2],[3,4,5]],[[1,2],[],[null,4,5]]]::Array(Array(Array(Nullable(Int8))))
        newProcessor(0x01, 0x01, 0x66, 0x43, 0x41, 0x53, 0x54, 0x28, 0x5B, 0x5B, 0x5B, 0x4E, 0x55, 0x4C, 0x4C, 0x5D,
                0x2C, 0x20, 0x5B, 0x31, 0x2C, 0x20, 0x32, 0x5D, 0x2C, 0x20, 0x5B, 0x33, 0x2C, 0x20, 0x34, 0x2C, 0x20,
                0x35, 0x5D, 0x5D, 0x2C, 0x20, 0x5B, 0x5B, 0x31, 0x2C, 0x20, 0x32, 0x5D, 0x2C, 0x20, 0x5B, 0x5D, 0x2C,
                0x20, 0x5B, 0x4E, 0x55, 0x4C, 0x4C, 0x2C, 0x20, 0x34, 0x2C, 0x20, 0x35, 0x5D, 0x5D, 0x5D, 0x2C, 0x20,
                0x27, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x41, 0x72, 0x72, 0x61,
                0x79, 0x28, 0x4E, 0x75, 0x6C, 0x6C, 0x61, 0x62, 0x6C, 0x65, 0x28, 0x49, 0x6E, 0x74, 0x38, 0x29, 0x29,
                0x29, 0x29, 0x27, 0x29, 0x23, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x41, 0x72, 0x72, 0x61, 0x79, 0x28,
                0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x4E, 0x75, 0x6C, 0x6C, 0x61, 0x62, 0x6C, 0x65, 0x28, 0x49, 0x6E,
                0x74, 0x38, 0x29, 0x29, 0x29, 0x29, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x0B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x01, 0x02, 0x00, 0x04, 0x05);
        verifySingleRecord(p, new Byte[][][] { { { null }, { 1, 2 }, { 3, 4, 5 } }, { { 1, 2 }, {}, { null, 4, 5 } } });
    }
}
