package com.clickhouse.client.internal;

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.testng.annotations.Test;

import org.testng.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.TimeZone;

public class SerializerUtilsTests {

    @Test
    public void testConvertToInteger() {
        int expected = 1640995199; // Unix timestamp for the given date
        Assert.assertEquals(SerializerUtils.convertToInteger("1640995199").intValue(), expected);
        Assert.assertEquals(SerializerUtils.convertToInteger(false).intValue(), 0);
    }

    @Test
    public void testSerializeNumbersRoundTrip() throws IOException {
        String[] names = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o",
                "p", "q", "r"};
        String[] types = new String[]{"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
                "Int128", "Int256", "UInt128", "UInt256", "Float32", "Float64", "Decimal32(3)", "Decimal64(3)",
                "Decimal128(4)", "Decimal256(4)"};
        Object[] values = new Object[]{
                (byte) 120,
                (short) 120,
                120,
                120L,
                120,
                120,
                120L,
                BigInteger.valueOf(120),
                BigInteger.valueOf(120),
                BigInteger.valueOf(120),
                BigInteger.valueOf(120),
                BigInteger.valueOf(120),
                120.0f,
                120.0d,
                BigDecimal.valueOf(120),
                BigDecimal.valueOf(120),
                BigDecimal.valueOf(120),
                BigDecimal.valueOf(120)
        };

        RowBinaryWithNamesAndTypesFormatReader reader = serializeRow(names, types, values);
        reader.next();

        for (String name : names) {
            Assert.assertTrue(reader.getBigDecimal(name).compareTo(BigDecimal.valueOf(120)) == 0);
        }
    }

    @Test
    public void testSerializeIntegerWidths8To256RoundTripAsBigDecimal() throws IOException {
        String[] names = new String[]{
                "i8", "u8", "i16", "u16", "i32", "u32", "i64", "u64", "i128", "u128", "i256", "u256"
        };
        String[] types = new String[]{
                "Int8", "UInt8", "Int16", "UInt16", "Int32", "UInt32", "Int64", "UInt64",
                "Int128", "UInt128", "Int256", "UInt256"
        };

        BigInteger u64 = new BigInteger("18446744073709551615");
        BigInteger i128 = new BigInteger("-170141183460469231731687303715884105728");
        BigInteger u128 = new BigInteger("340282366920938463463374607431768211455");
        BigInteger i256 = new BigInteger("-57896044618658097711785492504343953926634992332820282019728792003956564819968");
        BigInteger u256 = new BigInteger("115792089237316195423570985008687907853269984665640564039457584007913129639935");

        Object[] values = new Object[]{
                -128,
                255,
                -32768,
                65535,
                Integer.MIN_VALUE,
                4294967295L,
                Long.MAX_VALUE,
                u64,
                i128,
                u128,
                i256,
                u256
        };

        BigDecimal[] expected = new BigDecimal[]{
                BigDecimal.valueOf(-128),
                BigDecimal.valueOf(255),
                BigDecimal.valueOf(-32768),
                BigDecimal.valueOf(65535),
                BigDecimal.valueOf(Integer.MIN_VALUE),
                BigDecimal.valueOf(4294967295L),
                BigDecimal.valueOf(Long.MAX_VALUE),
                new BigDecimal(u64),
                new BigDecimal(i128),
                new BigDecimal(u128),
                new BigDecimal(i256),
                new BigDecimal(u256)
        };

        RowBinaryWithNamesAndTypesFormatReader reader = serializeRow(names, types, values);
        reader.next();

        for (int i = 0; i < names.length; i++) {
            Assert.assertEquals(reader.getBigDecimal(names[i]), expected[i]);
        }
    }

    private RowBinaryWithNamesAndTypesFormatReader serializeRow(String[] names, String[] types, Object[] values)
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

        return new RowBinaryWithNamesAndTypesFormatReader(
                new ByteArrayInputStream(out.toByteArray()),
                new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId()),
                new BinaryStreamReader.CachingByteBufferAllocator());
    }
}
