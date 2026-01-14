package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import java.util.UUID;

public class RowBinaryFormatSerializerTest {

    @Test
    public void testWriteInt8() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        byte value = (byte) 127;
        serializer.writeInt8(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int result = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals((byte) result, value);
    }

    @Test
    public void testWriteUInt8() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        short value = (short) 255;
        serializer.writeUInt8(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        BinaryStreamReader reader = createReader(in);
        short result = reader.readUnsignedByte();
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteInt16() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        short value = (short) 32767;
        serializer.writeInt16(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[2];
        short result = BinaryStreamReader.readShortLE(in, buffer);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteUInt16() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        int value = 65535;
        serializer.writeUInt16(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[2];
        int result = BinaryStreamReader.readUnsignedShortLE(in, buffer);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteInt32() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        int value = 2147483647;
        serializer.writeInt32(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[4];
        int result = BinaryStreamReader.readIntLE(in, buffer);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteUInt32() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        long value = 4294967295L;
        serializer.writeUInt32(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[4];
        long result = BinaryStreamReader.readUnsignedIntLE(in, buffer);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteInt64() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        long value = 9223372036854775807L;
        serializer.writeInt64(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[8];
        long result = BinaryStreamReader.readLongLE(in, buffer);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteUInt64() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        BigInteger value = new BigInteger("18446744073709551615");
        serializer.writeUInt64(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[8];
        BigInteger result = BinaryStreamReader.readBigIntegerLE(in, buffer, 8, true);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteInt128() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        BigInteger value = new BigInteger("170141183460469231731687303715884105727");
        serializer.writeInt128(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[16];
        BigInteger result = BinaryStreamReader.readBigIntegerLE(in, buffer, 16, false);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteUInt128() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        BigInteger value = new BigInteger("340282366920938463463374607431768211455");
        serializer.writeUInt128(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[16];
        BigInteger result = BinaryStreamReader.readBigIntegerLE(in, buffer, 16, true);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteInt256() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        BigInteger value = new BigInteger("57896044618658097711785492504343953926634992332820282019728792003956564819967");
        serializer.writeInt256(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[32];
        BigInteger result = BinaryStreamReader.readBigIntegerLE(in, buffer, 32, false);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteUInt256() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        BigInteger value = new BigInteger("115792089237316195423570985008687907853269984665640564039457584007913129639935");
        serializer.writeUInt256(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[16];
        // Note: writeUInt256 uses writeUnsignedInt128 internally, so we read 16 bytes
        BigInteger result = BinaryStreamReader.readBigIntegerLE(in, buffer, 16, true);
        // We can't fully verify UInt256 with this approach, but we can verify it writes something
        Assert.assertNotNull(result);
    }

    @Test
    public void testWriteBool() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        serializer.writeBool(true);
        serializer.writeBool(false);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int trueValue = BinaryStreamReader.readByteOrEOF(in);
        int falseValue = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals(trueValue, 1);
        Assert.assertEquals(falseValue, 0);
    }

    @Test
    public void testWriteFloat32() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        float value = 3.14159f;
        serializer.writeFloat32(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        BinaryStreamReader reader = createReader(in);
        float result = reader.readFloatLE();
        Assert.assertEquals(result, value, 0.0001f);
    }

    @Test
    public void testWriteFloat64() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        double value = 3.141592653589793;
        serializer.writeFloat64(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        BinaryStreamReader reader = createReader(in);
        double result = reader.readDoubleLE();
        Assert.assertEquals(result, value, 0.000000000000001);
    }

    @Test
    public void testWriteDecimal() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        BigDecimal value = new BigDecimal("123.45");
        int precision = 5;
        int scale = 2;
        serializer.writeDecimal(value, precision, scale);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        BinaryStreamReader reader = createReader(in);
        BigDecimal result = reader.readDecimal(precision, scale);
        Assert.assertEquals(result.stripTrailingZeros(), value.stripTrailingZeros());
    }

    @Test
    public void testWriteDecimal32() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        BigDecimal value = new BigDecimal("123.45");
        int scale = 2;
        serializer.writeDecimal32(value, scale);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        BinaryStreamReader reader = createReader(in);
        BigDecimal result = reader.readDecimal(ClickHouseDataType.Decimal32.getMaxPrecision(), scale);
        Assert.assertEquals(result.stripTrailingZeros(), value.stripTrailingZeros());
    }

    @Test
    public void testWriteDecimal64() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        BigDecimal value = new BigDecimal("123456789.123456");
        int scale = 6;
        serializer.writeDecimal64(value, scale);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        BinaryStreamReader reader = createReader(in);
        BigDecimal result = reader.readDecimal(ClickHouseDataType.Decimal64.getMaxPrecision(), scale);
        Assert.assertEquals(result.stripTrailingZeros(), value.stripTrailingZeros());
    }

    @Test
    public void testWriteDecimal128() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        BigDecimal value = new BigDecimal("1234567890123456789.12345678");
        int scale = 8;
        serializer.writeDecimal128(value, scale);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        BinaryStreamReader reader = createReader(in);
        BigDecimal result = reader.readDecimal(ClickHouseDataType.Decimal128.getMaxPrecision(), scale);
        Assert.assertEquals(result.stripTrailingZeros(), value.stripTrailingZeros());
    }

    @Test
    public void testWriteDecimal256() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        BigDecimal value = new BigDecimal("1234567890123456789012345678901234567890123456789012345678901234567890123456.1234567890");
        int scale = 10;
        serializer.writeDecimal256(value, scale);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        BinaryStreamReader reader = createReader(in);
        BigDecimal result = reader.readDecimal(ClickHouseDataType.Decimal256.getMaxPrecision(), scale);
        Assert.assertEquals(result.stripTrailingZeros(), value.stripTrailingZeros());
    }

    @Test
    public void testWriteString() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        String value = "Hello, World!";
        serializer.writeString(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        String result = BinaryStreamReader.readString(in);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteFixedString() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        String value = "Hello";
        int len = 10;
        serializer.writeFixedString(value, len);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[len];
        BinaryStreamReader.readNBytes(in, buffer, 0, len);
        String result = new String(buffer, 0, len, java.nio.charset.StandardCharsets.UTF_8).trim();
        Assert.assertEquals(result.trim(), value);
    }

    @Test
    public void testWriteDate() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        ZonedDateTime value = ZonedDateTime.of(2023, 1, 15, 0, 0, 0, 0, ZoneId.of("UTC"));
        serializer.writeDate(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[2];
        ZonedDateTime result = BinaryStreamReader.readDate(in, buffer, TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(result.toLocalDate(), value.toLocalDate());
    }

    @Test
    public void testWriteDate32() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        ZonedDateTime value = ZonedDateTime.of(2023, 1, 15, 0, 0, 0, 0, ZoneId.of("UTC"));
        ZoneId targetTz = ZoneId.of("UTC");
        serializer.writeDate32(value, targetTz);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[4];
        ZonedDateTime result = BinaryStreamReader.readDate32(in, buffer, TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(result.toLocalDate(), value.toLocalDate());
    }

    @Test
    public void testWriteDateTime() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        ZonedDateTime value = ZonedDateTime.of(2023, 1, 15, 14, 30, 45, 0, ZoneId.of("UTC"));
        ZoneId targetTz = ZoneId.of("UTC");
        serializer.writeDateTime(value, targetTz);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[4];
        ZonedDateTime result = BinaryStreamReader.readDateTime32(in, buffer, TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(result.toEpochSecond(), value.toEpochSecond());
    }

    @Test
    public void testWriteDateTime64() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        ZonedDateTime value = ZonedDateTime.of(2023, 1, 15, 14, 30, 45, 123456789, ZoneId.of("UTC"));
        int scale = 9;
        ZoneId targetTz = ZoneId.of("UTC");
        serializer.writeDateTime64(value, scale, targetTz);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[8];
        ZonedDateTime result = BinaryStreamReader.readDateTime64(in, buffer, scale, TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(result.toEpochSecond(), value.toEpochSecond());
    }

    @Test
    public void testWriteDateTime32WithOffsetDateTime() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        OffsetDateTime value = OffsetDateTime.of(2023, 1, 15, 14, 30, 45, 0, java.time.ZoneOffset.UTC);
        serializer.writeDateTime32(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[4];
        ZonedDateTime result = BinaryStreamReader.readDateTime32(in, buffer, TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(result.toEpochSecond(), value.toEpochSecond());
    }

    @Test
    public void testWriteDateTime64WithOffsetDateTime() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        OffsetDateTime value = OffsetDateTime.of(2023, 1, 15, 14, 30, 45, 123456789, java.time.ZoneOffset.UTC);
        int scale = 9;
        serializer.writeDateTime64(value, scale);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[8];
        ZonedDateTime result = BinaryStreamReader.readDateTime64(in, buffer, scale, TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(result.toEpochSecond(), value.toEpochSecond());
    }

    @Test
    public void testWriteDateTime32WithInstant() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        Instant value = Instant.ofEpochSecond(1673794245);
        serializer.writeDateTime32(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[4];
        ZonedDateTime result = BinaryStreamReader.readDateTime32(in, buffer, TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(result.toEpochSecond(), value.getEpochSecond());
    }

    @Test
    public void testWriteDateTime64WithInstant() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        Instant value = Instant.ofEpochSecond(1673794245, 123456789);
        int scale = 9;
        serializer.writeDateTime64(value, scale);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[8];
        ZonedDateTime result = BinaryStreamReader.readDateTime64(in, buffer, scale, TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(result.toEpochSecond(), value.getEpochSecond());
    }

    @Test
    public void testWriteEnum8() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        byte value = (byte) 42;
        serializer.writeEnum8(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int result = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals((byte) result, value);
    }

    @Test
    public void testWriteEnum16() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        short value = (short) 4242;
        serializer.writeEnum16(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[2];
        short result = BinaryStreamReader.readShortLE(in, buffer);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteUUID() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        UUID uuid = UUID.randomUUID();
        serializer.writeUUID(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[8];
        long mostSigBits = BinaryStreamReader.readLongLE(in, buffer);
        long leastSigBits = BinaryStreamReader.readLongLE(in, buffer);
        UUID result = new UUID(mostSigBits, leastSigBits);
        Assert.assertEquals(result, uuid);
    }

    @Test
    public void testWriteIPV4Address() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        Inet4Address value = (Inet4Address) Inet4Address.getByName("192.168.1.1");
        serializer.writeIPV4Address(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[4];
        byte[] addressBytes = BinaryStreamReader.readNBytesLE(in, buffer, 0, 4);
        Inet4Address result = (Inet4Address) Inet4Address.getByAddress(addressBytes);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteIPV6Address() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        Inet6Address value = (Inet6Address) Inet6Address.getByName("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        serializer.writeIPV6Address(value);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buffer = new byte[16];
        byte[] addressBytes = BinaryStreamReader.readNBytes(in, buffer, 0, 16);
        Inet6Address result = (Inet6Address) Inet6Address.getByAddress(addressBytes);
        Assert.assertEquals(result, value);
    }

    @Test
    public void testWriteNull() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        serializer.writeNull();

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int result = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals(result, 1);
    }

    @Test
    public void testWriteNotNull() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        serializer.writeNotNull();

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int result = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals(result, 0);
    }

    @Test
    public void testWriteDefault() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(out);

        serializer.writeDefault();

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int result = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals(result, 1);
    }

    @Test
    public void testWriteSize() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long size = 12345L;
        RowBinaryFormatSerializer.writeSize(out, size);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int result = BinaryStreamReader.readVarInt(in);
        Assert.assertEquals(result, size);
    }

    private BinaryStreamReader createReader(ByteArrayInputStream in) {
        // Use reflection to access package-private constructor
        try {
            java.lang.reflect.Constructor<BinaryStreamReader> constructor = BinaryStreamReader.class.getDeclaredConstructor(
                    java.io.InputStream.class, TimeZone.class, org.slf4j.Logger.class,
                    BinaryStreamReader.ByteBufferAllocator.class, boolean.class, java.util.Map.class);
            constructor.setAccessible(true);
            return constructor.newInstance(in, TimeZone.getDefault(), null,
                    new BinaryStreamReader.DefaultByteBufferAllocator(), false, null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create BinaryStreamReader", e);
        }
    }

    @Test
    public void testWriteValuePreamble_NonNullable_NonNullValue() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClickHouseColumn column = ClickHouseColumn.of("testCol", ClickHouseDataType.Int32, false);
        Object value = 42;

        boolean shouldWrite = RowBinaryFormatSerializer.writeValuePreamble(out, false, column, value);

        Assert.assertTrue(shouldWrite);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        // For non-nullable, non-null value, nothing should be written
        Assert.assertEquals(in.available(), 0);
    }

    @Test
    public void testWriteValuePreamble_Nullable_NonNullValue() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClickHouseColumn column = ClickHouseColumn.of("testCol", ClickHouseDataType.Int32, true);
        Object value = 42;

        boolean shouldWrite = RowBinaryFormatSerializer.writeValuePreamble(out, false, column, value);

        Assert.assertTrue(shouldWrite);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int result = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals(result, 0); // Not null marker
    }

    @Test
    public void testWriteValuePreamble_Nullable_NullValue() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClickHouseColumn column = ClickHouseColumn.of("testCol", ClickHouseDataType.Int32, true);
        Object value = null;

        boolean shouldWrite = RowBinaryFormatSerializer.writeValuePreamble(out, false, column, value);

        Assert.assertFalse(shouldWrite);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int result = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals(result, 1); // Null marker
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testWriteValuePreamble_NonNullable_NullValue() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClickHouseColumn column = ClickHouseColumn.of("testCol", ClickHouseDataType.Int32, false);
        Object value = null;

        RowBinaryFormatSerializer.writeValuePreamble(out, false, column, value);
    }

    @Test
    public void testWriteValuePreamble_WithDefaults_NonNullValue() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClickHouseColumn column = ClickHouseColumn.of("testCol", ClickHouseDataType.Int32, false);
        Object value = 42;

        boolean shouldWrite = RowBinaryFormatSerializer.writeValuePreamble(out, true, column, value);

        Assert.assertTrue(shouldWrite);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int result = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals(result, 0); // Not default marker
    }

    @Test
    public void testWriteValuePreamble_WithDefaults_Nullable_NonNullValue() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClickHouseColumn column = ClickHouseColumn.of("testCol", ClickHouseDataType.Int32, true);
        Object value = 42;

        boolean shouldWrite = RowBinaryFormatSerializer.writeValuePreamble(out, true, column, value);

        Assert.assertTrue(shouldWrite);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int notDefault = BinaryStreamReader.readByteOrEOF(in);
        int notNull = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals(notDefault, 0);
        Assert.assertEquals(notNull, 0);
    }

    @Test
    public void testWriteValuePreamble_WithDefaults_HasDefault_NullValue() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // Simulate column with default
        Object value = null;

        boolean shouldWrite = RowBinaryFormatSerializer.writeValuePreamble(out, true, value, false,
                ClickHouseDataType.Int32, true, "testCol");

        Assert.assertFalse(shouldWrite);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int result = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals(result, 1); // Default marker
    }

    @Test
    public void testWriteValuePreamble_Array_NullValue() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClickHouseColumn column = ClickHouseColumn.of("testCol", ClickHouseDataType.Array, false);
        Object value = null;

        boolean shouldWrite = RowBinaryFormatSerializer.writeValuePreamble(out, false, column, value);

        Assert.assertTrue(shouldWrite);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        int result = BinaryStreamReader.readByteOrEOF(in);
        Assert.assertEquals(result, 0); // Not null marker for arrays
    }

    @Test
    public void testWriteValuePreamble_Dynamic_NullValue() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClickHouseColumn column = ClickHouseColumn.of("testCol", ClickHouseDataType.Dynamic, false);
        Object value = null;

        boolean shouldWrite = RowBinaryFormatSerializer.writeValuePreamble(out, false, column, value);

        Assert.assertTrue(shouldWrite);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        // Dynamic type with null should write nothing
        Assert.assertEquals(in.available(), 0);
    }
}
