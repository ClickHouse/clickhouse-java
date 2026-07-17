package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.TimeZone;

public class SerializerUtilsPrimitiveSerializationTests {

    @Test
    public void testSerializeIntegerTargetFromStringValue() throws IOException {
        RowBinaryWithNamesAndTypesFormatReader reader =
                serializeSingleValue("Int32", "123456");

        reader.next();

        Assert.assertEquals(reader.getInteger("value"), Integer.valueOf(123456));
    }

    @Test
    public void testSerializeDecimalTargetFromStringValue() throws IOException {
        RowBinaryWithNamesAndTypesFormatReader reader =
                serializeSingleValue("Decimal64(3)", "123456.789");

        reader.next();

        Assert.assertEquals(reader.getBigDecimal("value"), new BigDecimal("123456.789"));
    }

    @Test
    public void testSerializeDecimalTargetFromGenericNumberValue() throws IOException {
        RowBinaryWithNamesAndTypesFormatReader reader =
                serializeSingleValue("Decimal64(3)", new CustomNumber("987654.321"));

        reader.next();

        Assert.assertEquals(reader.getBigDecimal("value"), new BigDecimal("987654.321"));
    }

    @Test(dataProvider = "floatColumnValues")
    public void testSerializeFloatColumnFromVariousNumberTypes(String type, Object value, double expected)
            throws IOException {
        RowBinaryWithNamesAndTypesFormatReader reader = serializeSingleValue(type, value);

        reader.next();

        Assert.assertEquals(reader.getDouble("value"), expected, 0.0);
    }

    @DataProvider(name = "floatColumnValues")
    private Object[][] floatColumnValues() {
        return new Object[][] {
                // A Double supplied for a Float32 column (and a Float for a Float64 column) used to
                // throw ClassCastException; any Number now narrows through Number#floatValue()/
                // doubleValue(), matching how the Int* branches accept any Number.
                {"Float32", 1.5d, 1.5},
                {"Float32", -2.5d, -2.5},
                {"Float64", 1.5f, 1.5},
                {"Float64", -2.5f, -2.5},

                // Same-type value keeps working unchanged (these already passed before the fix).
                {"Float32", 1.5f, 1.5},
                {"Float64", 2.5d, 2.5},

                // Other Number subtypes are accepted for both float widths.
                {"Float32", 3, 3.0},
                {"Float32", 7L, 7.0},
                {"Float32", (short) 5, 5.0},
                {"Float32", (byte) 2, 2.0},
                {"Float32", BigInteger.valueOf(9), 9.0},
                {"Float32", new BigDecimal("1.5"), 1.5},
                {"Float32", new CustomNumber("2.5"), 2.5},
                {"Float64", 3, 3.0},
                {"Float64", 7L, 7.0},
                {"Float64", (short) 5, 5.0},
                {"Float64", (byte) 2, 2.0},
                {"Float64", BigInteger.valueOf(9), 9.0},
                {"Float64", new BigDecimal("1.25"), 1.25},
                {"Float64", new CustomNumber("2.5"), 2.5},

                // String and Boolean are accepted too, matching the other numeric column branches.
                {"Float32", "1.5", 1.5},
                {"Float32", true, 1.0},
                {"Float32", false, 0.0},
                {"Float64", "1.5", 1.5},
                {"Float64", true, 1.0},
                {"Float64", false, 0.0},

                // Out-of-Float-range values (now reachable through the fix) narrow like a primitive
                // cast: a Double beyond Float range becomes Float32 infinity rather than throwing.
                {"Float32", Double.MAX_VALUE, Double.POSITIVE_INFINITY},
                {"Float32", -Double.MAX_VALUE, Double.NEGATIVE_INFINITY},
        };
    }

    @Test
    public void testSerializeFloatColumnRejectsUnsupportedValue() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> serializeSingleValue("Float32", new Object()));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> serializeSingleValue("Float64", new Object()));
    }

    @Test(dataProvider = "bFloat16ColumnValues")
    public void testSerializeBFloat16ColumnFromVariousNumberTypes(Object value, float expected)
            throws IOException {
        RowBinaryWithNamesAndTypesFormatReader reader = serializeSingleValue("BFloat16", value);

        reader.next();

        Assert.assertEquals(reader.getFloat("value"), expected, 0.0f);
    }

    @DataProvider(name = "bFloat16ColumnValues")
    private Object[][] bFloat16ColumnValues() {
        // A non-Float value supplied for a BFloat16 column used to throw ClassCastException from the
        // direct (float) cast; any Number now narrows through NumberConverter.toFloat (Number#floatValue()),
        // matching the Float32/Float64 branches. Every value below is exactly representable in BFloat16
        // (its float32 low 16 bits are zero), so the write-side truncation is lossless and the expected
        // read-back is exact.
        return new Object[][] {
                // Same-type value keeps working unchanged (already passed before the fix).
                {1.5f, 1.5f},

                // A Double (and other Number subtypes) supplied for a BFloat16 column.
                {1.5d, 1.5f},
                {-2.5d, -2.5f},
                {3, 3.0f},
                {7L, 7.0f},
                {(short) 5, 5.0f},
                {(byte) 2, 2.0f},
                {BigInteger.valueOf(9), 9.0f},
                {new BigDecimal("1.5"), 1.5f},
                {new CustomNumber("2.5"), 2.5f},

                // String and Boolean are accepted too, matching the other numeric column branches.
                {"1.5", 1.5f},
                {true, 1.0f},
                {false, 0.0f},
        };
    }

    @Test
    public void testSerializeBFloat16ColumnRejectsUnsupportedValue() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> serializeSingleValue("BFloat16", new Object()));
    }

    @Test
    public void testSerializeBFloat16RoundTripAllValues() throws IOException {
        // Exhaustively round-trip every one of the 2^16 BFloat16 bit patterns. For pattern b the
        // canonical float32 is intBitsToFloat(b << 16); the client writes its high 16 bits and reads
        // them back widened. Every non-NaN pattern - including +/-0, subnormals and +/-Infinity -
        // must round-trip bit-for-bit. NaN inputs collapse to a single NaN because Float#floatToIntBits
        // normalizes the payload on write, so they are only required to read back as a NaN.
        final int count = 1 << 16;
        Object[] inputs = new Object[count];
        for (int b = 0; b < count; b++) {
            inputs[b] = Float.intBitsToFloat(b << 16);
        }

        RowBinaryWithNamesAndTypesFormatReader reader = serializeColumn("BFloat16", inputs);

        for (int b = 0; b < count; b++) {
            Assert.assertNotNull(reader.next(), "missing row for BFloat16 pattern " + hex(b));
            float actual = reader.getFloat("value");
            if (Float.isNaN((Float) inputs[b])) {
                Assert.assertTrue(Float.isNaN(actual), "BFloat16 pattern " + hex(b) + " must read back as NaN");
            } else {
                Assert.assertEquals(Float.floatToRawIntBits(actual), b << 16,
                        "BFloat16 pattern " + hex(b) + " did not round-trip");
            }
        }
        Assert.assertNull(reader.next(), "unexpected extra row after all 65,536 BFloat16 patterns");
    }

    private static String hex(int bFloat16Bits) {
        return String.format("0x%04X", bFloat16Bits);
    }

    private RowBinaryWithNamesAndTypesFormatReader serializeSingleValue(String type, Object value)
            throws IOException {
        return serializeColumn(type, new Object[]{value});
    }

    private RowBinaryWithNamesAndTypesFormatReader serializeColumn(String type, Object[] values)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(out, 1);
        BinaryStreamUtils.writeString(out, "value");
        BinaryStreamUtils.writeString(out, type);
        ClickHouseColumn column = ClickHouseColumn.of("value", type);
        for (Object value : values) {
            SerializerUtils.serializeData(out, value, column);
        }

        return new RowBinaryWithNamesAndTypesFormatReader(
                new ByteArrayInputStream(out.toByteArray()),
                new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId()),
                new BinaryStreamReader.CachingByteBufferAllocator());
    }

    private static final class CustomNumber extends Number {
        private final BigDecimal value;

        private CustomNumber(String value) {
            this.value = new BigDecimal(value);
        }

        @Override
        public int intValue() {
            return value.intValue();
        }

        @Override
        public long longValue() {
            return value.longValue();
        }

        @Override
        public float floatValue() {
            return value.floatValue();
        }

        @Override
        public double doubleValue() {
            return value.doubleValue();
        }

        @Override
        public String toString() {
            return value.toPlainString();
        }
    }
}
