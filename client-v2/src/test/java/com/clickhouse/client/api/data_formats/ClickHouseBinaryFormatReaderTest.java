package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ClickHouseBinaryFormatReaderTest {

    @Test
    public void testReadingNumbers() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
            "q", "r"};
        String[] types = new String[]{"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
                "Int128", "Int256", "UInt128", "UInt256", "Float32", "Float64",
                "Decimal32(3)", "Decimal64(3)", "Decimal128(4)", "Decimal256(4)"};


        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        final int testValue = 120;
        BinaryStreamUtils.writeInt8(out, testValue);
        BinaryStreamUtils.writeInt16(out, testValue);
        BinaryStreamUtils.writeInt32(out, testValue);
        BinaryStreamUtils.writeInt64(out, testValue);
        BinaryStreamUtils.writeUnsignedInt8(out, testValue);
        BinaryStreamUtils.writeUnsignedInt16(out, testValue);
        BinaryStreamUtils.writeUnsignedInt32(out, testValue);
        BinaryStreamUtils.writeUnsignedInt64(out, testValue);
        BinaryStreamUtils.writeInt128( out, new BigInteger(String.valueOf(testValue )));
        BinaryStreamUtils.writeInt256( out, new BigInteger(String.valueOf(testValue )));
        BinaryStreamUtils.writeUnsignedInt128(out, new BigInteger(String.valueOf(testValue )));
        BinaryStreamUtils.writeUnsignedInt256(out, new BigInteger(String.valueOf(testValue )));
        BinaryStreamUtils.writeFloat32(out, testValue);
        BinaryStreamUtils.writeFloat64(out, testValue);
        BinaryStreamUtils.writeDecimal32(out, BigDecimal.valueOf(testValue), 3);
        BinaryStreamUtils.writeDecimal64(out, BigDecimal.valueOf(testValue), 3);
        BinaryStreamUtils.writeDecimal128(out, BigDecimal.valueOf(testValue), 4);
        BinaryStreamUtils.writeDecimal256(out, BigDecimal.valueOf(testValue), 4);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            Assert.assertEquals(reader.getBoolean(name), Boolean.TRUE);
            Assert.assertEquals(reader.getByte(name), (byte)testValue);
            Assert.assertEquals(reader.getShort(name), (short)testValue);
            Assert.assertEquals(reader.getInteger(name), (int)testValue);
            Assert.assertEquals(reader.getLong(name), (long)testValue);

            Assert.assertEquals(reader.getFloat(name), (float) testValue);
            Assert.assertEquals(reader.getDouble(name), (double) testValue);
            Assert.assertEquals(reader.getBigInteger(name), BigInteger.valueOf((testValue)));

            Assert.assertTrue(reader.getBigDecimal(name).compareTo(BigDecimal.valueOf((testValue))) == 0);
        }
    }

    @Test
    public void testReadingNumbersWithOverflow() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
                "q", "r", "s"};
        String[] types = new String[]{"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
                "Int128", "Int256", "UInt128", "UInt256", "Float32", "Float64",
                "Decimal32(3)", "Decimal64(3)", "Decimal128(4)", "Decimal256(4)", "Float64"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        BinaryStreamUtils.writeInt8(out, 127); // a
        BinaryStreamUtils.writeInt16(out, 2000); // b
        BinaryStreamUtils.writeInt32(out, 300000); // c
        BinaryStreamUtils.writeInt64(out, 4000000000L); // d
        BinaryStreamUtils.writeUnsignedInt8(out, 255); // e
        BinaryStreamUtils.writeUnsignedInt16(out, 60000); // f
        BinaryStreamUtils.writeUnsignedInt32(out, 4000000000L); // g
        BinaryStreamUtils.writeUnsignedInt64(out, new BigInteger("18000044073709551615")); // h
        BinaryStreamUtils.writeInt128(out, new BigInteger("18000044073709551615")); // i
        BinaryStreamUtils.writeInt256(out, new BigInteger("18000044073709551615")); // j
        BinaryStreamUtils.writeUnsignedInt128(out, new BigInteger("18000044073709551615")); // k
        BinaryStreamUtils.writeUnsignedInt256(out, new BigInteger("18000044073709551615")); // l
        BinaryStreamUtils.writeFloat32(out,  900000.123f); // m
        BinaryStreamUtils.writeFloat64(out, 1000000.333); // n
        BinaryStreamUtils.writeDecimal32(out, BigDecimal.valueOf(100000), 3); // o
        BinaryStreamUtils.writeDecimal64(out, BigDecimal.valueOf(10000000.10000), 3); // p
        BinaryStreamUtils.writeDecimal128(out, BigDecimal.valueOf(1000000000.1000000), 4); // q
        BinaryStreamUtils.writeDecimal256(out, BigDecimal.valueOf(1000000000.1000000), 4); // r
        BinaryStreamUtils.writeFloat64(out, 123.456); // s

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        Consumer<String> byteConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getByte(name));
        Consumer<String> shortConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getShort(name));
        Consumer<String> integerConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getInteger(name));
        Consumer<String> longConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getLong(name));
//        Consumer<String> floatConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getFloat(name));
//        Consumer<String> doubleConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getDouble(name));

        Arrays.stream("b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r".split(",")).forEach(byteConsumer);
        Arrays.stream("c,d,f,g,h,i,j,k,l,m,n,o,p,q,r".split(",")).forEach(shortConsumer);
        Arrays.stream("d,g,h,i,j,k,l".split(",")).forEach(integerConsumer);
        Arrays.stream("h,i,j,k,l".split(",")).forEach(longConsumer);
//        Arrays.stream("h,i,j,k,l,n,p,q,r".split(",")).forEach(floatConsumer);
//        Arrays.stream("h,i,j,k,l,p,q,r".split(",")).forEach(doubleConsumer);

        Assert.assertEquals(reader.getFloat("m"), 900000.123f);
        Assert.assertEquals(reader.getDouble("m"), 900000.123f);

        Assert.assertEquals(reader.getBigDecimal("n"), BigDecimal.valueOf(1000000.333));
        Assert.assertEquals(reader.getBigDecimal("n"), BigDecimal.valueOf(1000000.333));

        Assert.assertEquals(reader.getFloat("s"), 123.456f);
    }

    @Test
    public void testReadingAsBoolean() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{ "a", "b"};
        String[] types = new String[]{"Bool", "Bool"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }


        BinaryStreamUtils.writeBoolean(out, true);
        BinaryStreamUtils.writeBoolean(out, false);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        Assert.assertEquals(reader.getBoolean("a"), Boolean.TRUE);
        Assert.assertEquals(reader.getBoolean("b"), Boolean.FALSE);
        Assert.assertEquals(reader.getByte("a"), (byte) 1);
        Assert.assertEquals(reader.getByte("b"), (byte) 0);
        Assert.assertEquals(reader.getShort("a"), (short) 1);
        Assert.assertEquals(reader.getShort("b"), (short) 0);
        Assert.assertEquals(reader.getInteger("a"),  1);
        Assert.assertEquals(reader.getInteger("b"),  0);
        Assert.assertEquals(reader.getLong("a"),  1);
        Assert.assertEquals(reader.getLong("b"),  0);
        Assert.assertEquals(reader.getFloat("a"),  1.0f);
        Assert.assertEquals(reader.getFloat("b"),  0.0f);
        Assert.assertEquals(reader.getDouble("a"),  1.0d);
        Assert.assertEquals(reader.getDouble("b"),  0.0d);
        Assert.assertEquals(reader.getBigInteger("a"),  BigInteger.ONE);
        Assert.assertEquals(reader.getBigInteger("b"),  BigInteger.ZERO);
        Assert.assertEquals(reader.getBigDecimal("a"),  BigDecimal.ONE);
        Assert.assertEquals(reader.getBigDecimal("b"),  BigDecimal.ZERO);
        Assert.assertEquals(reader.getString("a"),  "true");
        Assert.assertEquals(reader.getString("b"),  "false");
    }
}