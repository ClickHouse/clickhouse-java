package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.testng.Assert;
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
