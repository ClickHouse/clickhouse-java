package com.clickhouse.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.UUID;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.client.BaseClickHouseValueTest;
import com.clickhouse.client.ClickHouseDataType;

public class ClickHouseByteValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testUpdate() {
        ClickHouseByteValue v = ClickHouseByteValue.of(-1);
        Assert.assertEquals(v.getValue(), (byte) -1);
        Assert.assertEquals(v.update(true).asByte(), (byte) 1);
        Assert.assertEquals(v.update(Boolean.TRUE).getValue(), (byte) 1);
        Assert.assertEquals(v.update(false).asByte(), (byte) 0);
        Assert.assertEquals(v.update(Boolean.FALSE).getValue(), (byte) 0);
        Assert.assertEquals(v.update('a').getValue(), (byte) 97);
        Assert.assertEquals(v.update('萌').getValue(), (byte) 12);
        Assert.assertEquals(v.update((byte) 2).getValue(), (byte) 2);
        Assert.assertEquals(v.update(Byte.valueOf("2")).getValue(), (byte) 2);
        Assert.assertEquals(v.update((short) -2).getValue(), (byte) -2);
        Assert.assertEquals(v.update((short) -233).getValue(), (byte) 23);
        Assert.assertEquals(v.update(Short.valueOf("-2")).getValue(), (byte) -2);
        Assert.assertEquals(v.update(123).getValue(), (byte) 123);
        Assert.assertEquals(v.update(233).getValue(), (byte) -23);
        Assert.assertEquals(v.update(Integer.valueOf(123)).getValue(), (byte) 123);
        Assert.assertEquals(v.update(-123L).getValue(), (byte) -123);
        Assert.assertEquals(v.update(-233L).getValue(), (byte) 23);
        Assert.assertEquals(v.update(Long.valueOf(-123L)).getValue(), (byte) -123);
        Assert.assertEquals(v.update(23.3F).getValue(), (byte) 23);
        Assert.assertEquals(v.update(233.3F).getValue(), (byte) -23);
        Assert.assertEquals(v.update(Float.valueOf(23.3F)).getValue(), (byte) 23);
        Assert.assertEquals(v.update(-23.333D).getValue(), (byte) -23);
        Assert.assertEquals(v.update(-233.333D).getValue(), (byte) 23);
        Assert.assertEquals(v.update(Double.valueOf(-23.333D)).getValue(), (byte) -23);
        Assert.assertEquals(v.update(BigInteger.valueOf(Byte.MAX_VALUE)).getValue(), Byte.MAX_VALUE);
        Assert.assertThrows(ArithmeticException.class,
                () -> v.update(BigInteger.valueOf(Byte.MAX_VALUE + 1)).getValue());
        Assert.assertThrows(ArithmeticException.class,
                () -> v.update(BigDecimal.valueOf(Byte.MIN_VALUE * -1.0D)).getValue());
        Assert.assertEquals(v.update("121").getValue(), (byte) 121);
        // inconsistent with above :<
        Assert.assertThrows(NumberFormatException.class, () -> v.update("233").getValue());
    }

    @Test(groups = { "unit" })
    public void testValue() throws UnknownHostException {
        // null value
        checkNull(ClickHouseByteValue.ofNull());
        checkNull(ClickHouseByteValue.of(Byte.MIN_VALUE).resetToNullOrEmpty());
        checkNull(ClickHouseByteValue.of(Byte.MAX_VALUE).resetToNullOrEmpty());

        // non-null
        checkValue(ClickHouseByteValue.of(0), false, // isInfinity
                false, // isNan
                false, // isNull
                false, // boolean
                (byte) 0, // byte
                (short) 0, // short
                0, // int
                0L, // long
                0F, // float
                0D, // double
                BigDecimal.valueOf(0L), // BigDecimal
                new BigDecimal(BigInteger.ZERO, 3), // BigDecimal
                BigInteger.ZERO, // BigInteger
                ClickHouseDataType.values()[0].name(), // Enum<ClickHouseDataType>
                (byte) 0, // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:0")[0], // Inet6Address
                "0", // String
                "0", // SQL Expression
                LocalTime.ofSecondOfDay(0), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000000"), // UUID
                Integer.class, // Key class
                Byte.class, // Value class
                new Object[] { Byte.valueOf((byte) 0) }, // Array
                new Byte[] { Byte.valueOf((byte) 0) }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Byte.valueOf((byte) 0) }), // Map
                buildMap(new Object[] { 1 }, new Byte[] { Byte.valueOf((byte) 0) }), // typed Map
                Arrays.asList(Byte.valueOf((byte) 0)) // Tuple
        );
        checkValue(ClickHouseByteValue.of(1), false, // isInfinity
                false, // isNan
                false, // isNull
                true, // boolean
                (byte) 1, // byte
                (short) 1, // short
                1, // int
                1L, // long
                1F, // float
                1D, // double
                BigDecimal.valueOf(1L), // BigDecimal
                new BigDecimal(BigInteger.ONE, 3), // BigDecimal
                BigInteger.ONE, // BigInteger
                ClickHouseDataType.values()[1].name(), // Enum<ClickHouseDataType>
                (byte) 1, // Object
                LocalDate.ofEpochDay(1L), // Date
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 1, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.1")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:1")[0], // Inet6Address
                "1", // String
                "1", // SQL Expression
                LocalTime.ofSecondOfDay(1), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000001"), // UUID
                Integer.class, // Key class
                Byte.class, // Value class
                new Object[] { Byte.valueOf((byte) 1) }, // Array
                new Byte[] { Byte.valueOf((byte) 1) }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Byte.valueOf((byte) 1) }), // Map
                buildMap(new Object[] { 1 }, new Byte[] { Byte.valueOf((byte) 1) }), // typed Map
                Arrays.asList(Byte.valueOf((byte) 1)) // Tuple
        );
        checkValue(ClickHouseByteValue.of(2), false, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                (byte) 2, // byte
                (short) 2, // short
                2, // int
                2L, // long
                2F, // float
                2D, // double
                BigDecimal.valueOf(2L), // BigDecimal
                new BigDecimal(BigInteger.valueOf(2L), 3), // BigDecimal
                BigInteger.valueOf(2L), // BigInteger
                ClickHouseDataType.values()[2].name(), // Enum<ClickHouseDataType>
                (byte) 2, // Object
                LocalDate.ofEpochDay(2L), // Date
                LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 2, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.2")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:2")[0], // Inet6Address
                "2", // String
                "2", // SQL Expression
                LocalTime.ofSecondOfDay(2), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000002"), // UUID
                Integer.class, // Key class
                Byte.class, // Value class
                new Object[] { Byte.valueOf((byte) 2) }, // Array
                new Byte[] { Byte.valueOf((byte) 2) }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Byte.valueOf((byte) 2) }), // Map
                buildMap(new Object[] { 1 }, new Byte[] { Byte.valueOf((byte) 2) }), // typed Map
                Arrays.asList(Byte.valueOf((byte) 2)) // Tuple
        );

        checkValue(ClickHouseByteValue.of(-1), false, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                (byte) -1, // byte
                (short) -1, // short
                -1, // int
                -1L, // long
                -1F, // float
                -1D, // double
                BigDecimal.valueOf(-1L), // BigDecimal
                new BigDecimal(BigInteger.valueOf(-1L), 3), // BigDecimal
                BigInteger.valueOf(-1L), // BigInteger
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                (byte) -1, // Object
                LocalDate.ofEpochDay(-1L), // Date
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(-1L, 999999999, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("255.255.255.255")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:ff")[0], // Inet6Address
                "-1", // String
                "-1", // SQL Expression
                LocalTime.of(23, 59, 59), // Time
                UUID.fromString("00000000-0000-0000-ffff-ffffffffffff"), // UUID
                Integer.class, // Key class
                Byte.class, // Value class
                new Object[] { Byte.valueOf((byte) -1) }, // Array
                new Byte[] { Byte.valueOf((byte) -1) }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Byte.valueOf((byte) -1) }), // Map
                buildMap(new Object[] { 1 }, new Byte[] { Byte.valueOf((byte) -1) }), // typed Map
                Arrays.asList(Byte.valueOf((byte) -1)) // Tuple
        );
    }
}
