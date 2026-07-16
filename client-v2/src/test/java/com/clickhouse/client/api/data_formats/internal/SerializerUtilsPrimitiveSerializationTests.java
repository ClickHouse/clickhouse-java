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

    @DataProvider(name = "bFloat16Values")
    public static Object[][] bFloat16Values() {
        // input float, expected float after a BFloat16 round-trip. BFloat16 keeps the high
        // 16 bits of the float32 representation, so writing truncates the low mantissa bits
        // (matching the ClickHouse server) and reading widens the value back losslessly.
        return new Object[][] {
                {0.0f, 0.0f},                    // zero
                {1.5f, 1.5f},                    // exactly representable
                {-2.5f, -2.5f},                  // exactly representable, sign preserved
                {100.0f, 100.0f},                // exactly representable
                {256.0f, 256.0f},                // exactly representable
                {3.14f, 3.125f},                 // 0x4048F5C3 -> 0x40480000 (low bits dropped)
                {0.1f, 0.099609375f},            // 0x3DCCCCCD -> 0x3DCC0000 (low bits dropped)
        };
    }

    @Test(dataProvider = "bFloat16Values")
    public void testSerializeBFloat16RoundTrip(float input, float expected) throws IOException {
        RowBinaryWithNamesAndTypesFormatReader reader = serializeSingleValue("BFloat16", input);

        reader.next();

        Assert.assertEquals(reader.getFloat("value"), expected, 0.0f);
    }

    private RowBinaryWithNamesAndTypesFormatReader serializeSingleValue(String type, Object value)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryStreamUtils.writeVarInt(out, 1);
        BinaryStreamUtils.writeString(out, "value");
        BinaryStreamUtils.writeString(out, type);
        SerializerUtils.serializeData(out, value, ClickHouseColumn.of("value", type));

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
