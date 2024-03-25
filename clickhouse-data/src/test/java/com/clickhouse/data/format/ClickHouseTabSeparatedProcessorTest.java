package com.clickhouse.data.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseTestDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.value.BaseDataProcessorTest;
import com.clickhouse.data.value.ClickHouseEmptyValue;
import com.clickhouse.data.value.ClickHouseStringValue;
import com.clickhouse.data.value.UnsignedLong;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseTabSeparatedProcessorTest extends BaseDataProcessorTest {
    @DataProvider(name = "simpleTsvDataProvider")
    private Object[][] getSimpleTsvData() {
        return new Object[][] {
                { UUID.randomUUID().toString(), 0 },
                { UUID.randomUUID().toString(), 1 },
                { UUID.randomUUID().toString(), 2 },
                { UUID.randomUUID().toString(), 3 },
                { UUID.randomUUID().toString(), 256 } };
    }

    @DataProvider(name = "specialCkDoubleDataProvider")
    private Object[][] getSpecialCkDoubleData() {
        return new Object[][] {
                { "inf", Double.POSITIVE_INFINITY },
                { "+inf", Double.POSITIVE_INFINITY },
                { "-inf", Double.NEGATIVE_INFINITY },
                { "nan", Double.NaN } };
    }

    @Override
    protected ClickHouseDataProcessor getDataProcessor(ClickHouseDataConfig config, ClickHouseColumn column,
            ClickHouseInputStream input, ClickHouseOutputStream output) throws IOException {
        return new ClickHouseTabSeparatedProcessor(config, input, output, Collections.singletonList(column), null);
    }

    @Override
    protected byte[] getRawData(String typeName, String key) {
        byte[] data = null;
        while (typeName.indexOf("LowCardinality(") >= 0) {
            typeName = typeName.replaceFirst("LowCardinality\\(", "").replaceFirst("\\)", "");
        }
        if (typeName.startsWith("Nullable(")) {
            if ("null".equals(key)) {
                return toBytes("\\N\n");
            } else {
                return getRawData(typeName.replaceFirst("Nullable\\(", "").replaceFirst("\\)", ""), key);
            }
        }
        switch (typeName) {
            case "Array(Nullable(Bool))":
                if ("0,1".equals(key)) { // first one will be null
                    data = toBytes("[\\N,false,true]");
                }
                break;
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
                if ("0".equals(key)) {
                    data = toBytes("false");
                } else if ("1".equals(key)) {
                    data = toBytes("true");
                }
                break;
            case "Int8":
            case "Int16":
            case "Int32":
            case "Int64":
                data = toBytes(key);
                break;
            case "Float32":
            case "Float64":
                data = toBytes(key + ".0");
                break;
            case "UInt8":
                data = toBytes(String.valueOf(0xFF & Byte.parseByte(key)));
                break;
            case "UInt16":
                data = toBytes(String.valueOf(0xFFFF & Short.parseShort(key)));
                break;
            case "UInt32":
                data = toBytes(String.valueOf(0xFFFFFFFFL & Integer.parseInt(key)));
                break;
            case "UInt64":
                data = toBytes(String.valueOf(new BigInteger(key).and(UnsignedLong.MASK)));
                break;
            case "String":
                data = toBytes(key);
                break;
            default:
                break;
        }

        if (data == null) {
            Assert.fail(String.format("No raw data defined for type=[%s] and key=[%s]", typeName, key));
        }
        return toBytes(data, '\n');
    }

    @Override
    public void testDeserializeNestedTypes(ClickHouseDataConfig config, String typeName, String dataKey,
            String valueClass, Object arrVal, Object objVal, String strVal, String sqlExpr) throws IOException {
        throw new SkipException("Skip as it's not fully implemented");
    }

    @Override
    public void testSerializeNestedTypes(ClickHouseDataConfig config, String typeName, String dataKey,
            ClickHouseValue... values) throws IOException {
        throw new SkipException("Skip as it's not fully implemented");
    }

    @Test(dataProvider = "simpleTsvDataProvider", groups = { "unit" })
    public void testRead(String id, int rows) throws IOException {
        String result = String.format("\\'%s\\'\nString\n", id);
        for (int i = 0; i < rows; i++) {
            result += String.format("%s\n", id);
        }
        ClickHouseDataConfig config = new ClickHouseTestDataConfig() {
            @Override
            public ClickHouseFormat getFormat() {
                return ClickHouseFormat.TabSeparatedWithNamesAndTypes;
            }
        };
        ClickHouseInputStream input = ClickHouseInputStream.of(Collections.singletonList(result), String.class,
                s -> s.getBytes(), null);
        ClickHouseTabSeparatedProcessor p = new ClickHouseTabSeparatedProcessor(config, input, null, null, null);
        Assert.assertEquals(p.getColumns(),
                Collections.singletonList(ClickHouseColumn.of(String.format("'%s'", id), "String")));
        int count = 0;
        for (ClickHouseRecord r : p.records()) {
            Assert.assertEquals(r.getValue(0).asString(), id);
            count++;
        }
        Assert.assertEquals(count, rows);
    }

    @Test(dataProvider = "specialCkDoubleDataProvider", groups = { "unit" })
    public void testReadSpecialCkDoubleData(String specialCkDouble, Double d) throws IOException {
        String result = String.format("\\'%s\\'\nDouble\n%s\n", specialCkDouble, specialCkDouble);
        ClickHouseDataConfig config = new ClickHouseTestDataConfig() {
            @Override
            public ClickHouseFormat getFormat() {
                return ClickHouseFormat.TabSeparatedWithNamesAndTypes;
            }
        };
        ClickHouseInputStream input = ClickHouseInputStream.of(Collections.singletonList(result), String.class,
                s -> s.getBytes(), null);
        ClickHouseTabSeparatedProcessor p = new ClickHouseTabSeparatedProcessor(config, input, null, null, null);
        Assert.assertEquals(p.getColumns(),
                Collections.singletonList(ClickHouseColumn.of(String.format("'%s'", specialCkDouble), "Double")));
        for (ClickHouseRecord r : p.records()) {
            Assert.assertEquals(r.getValue(0).asDouble(), d);
        }
    }

    @Test(groups = { "unit" })
    public void testWriteTsv() throws IOException {
        List<ClickHouseColumn> list = ClickHouseColumn.parse("a String, b Nullable(Int32)");
        ClickHouseValue value = ClickHouseStringValue.of("aaa");
        ClickHouseDataConfig config = new ClickHouseTestDataConfig() {
            @Override
            public ClickHouseFormat getFormat() {
                return ClickHouseFormat.TabSeparated;
            }
        };
        try (ByteArrayOutputStream bas = new ByteArrayOutputStream();
                ClickHouseOutputStream out = ClickHouseOutputStream.of(bas)) {
            ClickHouseTabSeparatedProcessor p = new ClickHouseTabSeparatedProcessor(config, null, out,
                    ClickHouseDataProcessor.DEFAULT_COLUMNS, null);
            p.write(value);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), new byte[] { 97, 97, 97, 10 });
        }

        for (int i = 1; i <= 6; i++) {
            try (ByteArrayOutputStream bas = new ByteArrayOutputStream();
                    ClickHouseOutputStream out = ClickHouseOutputStream.of(bas, i)) {
                ClickHouseTabSeparatedProcessor p = new ClickHouseTabSeparatedProcessor(config, null, out, list, null);
                p.write(value);
                p.write(ClickHouseEmptyValue.INSTANCE);
                out.flush();
                Assert.assertEquals(bas.toByteArray(), new byte[] { 97, 97, 97, 9, 92, 78, 10 });
            }
        }
    }
}
