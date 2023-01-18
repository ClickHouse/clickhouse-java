package com.clickhouse.data.value;

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
import com.clickhouse.data.BaseClickHouseValueTest;
import com.clickhouse.data.ClickHouseDataType;

public class ClickHouseBoolValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testUpdate() {
        ClickHouseBoolValue v = ClickHouseBoolValue.of(0);
        Assert.assertEquals(v.getValue(), false);
        Assert.assertEquals(v.update(true).asByte(), (byte) 1);
        Assert.assertEquals(v.update(Boolean.TRUE).getValue(), true);
        Assert.assertEquals(v.update(false).asByte(), (byte) 0);
        Assert.assertEquals(v.update(Boolean.FALSE).getValue(), false);
        Assert.assertEquals(v.update('T').getValue(), true);
        Assert.assertEquals(v.update('F').getValue(), false);
        Assert.assertEquals(v.update('t').getValue(), true);
        Assert.assertEquals(v.update('f').getValue(), false);
        Assert.assertEquals(v.update('1').getValue(), true);
        Assert.assertEquals(v.update('0').getValue(), false);
        Assert.assertEquals(v.update('Y').getValue(), true);
        Assert.assertEquals(v.update('N').getValue(), false);
        Assert.assertEquals(v.update('y').getValue(), true);
        Assert.assertEquals(v.update('n').getValue(), false);
        Assert.assertEquals(v.update("tRue").getValue(), true);
        Assert.assertEquals(v.update("False").getValue(), false);
        Assert.assertEquals(v.update("yeS").getValue(), true);
        Assert.assertEquals(v.update("NO").getValue(), false);
        Assert.assertEquals(v.update("1").getValue(), true);
        Assert.assertEquals(v.update("0").getValue(), false);

        Assert.assertEquals(v.update((byte) 1).getValue(), true);
        Assert.assertEquals(v.update((byte) 0).getValue(), false);
        Assert.assertEquals(v.update(Byte.valueOf("1")).getValue(), true);
        Assert.assertEquals(v.update(Byte.valueOf("0")).getValue(), false);
        Assert.assertEquals(v.update((short) 1).getValue(), true);
        Assert.assertEquals(v.update((short) 0).getValue(), false);
        Assert.assertEquals(v.update(Short.valueOf("1")).getValue(), true);
        Assert.assertEquals(v.update(Short.valueOf("0")).getValue(), false);
        Assert.assertEquals(v.update(1).getValue(), true);
        Assert.assertEquals(v.update(0).getValue(), false);
        Assert.assertEquals(v.update(Integer.valueOf("1")).getValue(), true);
        Assert.assertEquals(v.update(Integer.valueOf("0")).getValue(), false);
        Assert.assertEquals(v.update(1L).getValue(), true);
        Assert.assertEquals(v.update(0L).getValue(), false);
        Assert.assertEquals(v.update(Long.valueOf("1")).getValue(), true);
        Assert.assertEquals(v.update(Long.valueOf("0")).getValue(), false);
        Assert.assertEquals(v.update(1F).getValue(), true);
        Assert.assertEquals(v.update(0F).getValue(), false);
        Assert.assertEquals(v.update(Float.valueOf("1")).getValue(), true);
        Assert.assertEquals(v.update(Float.valueOf("0")).getValue(), false);
        Assert.assertEquals(v.update(1D).getValue(), true);
        Assert.assertEquals(v.update(0D).getValue(), false);
        Assert.assertEquals(v.update(Double.valueOf("1")).getValue(), true);
        Assert.assertEquals(v.update(Double.valueOf("0")).getValue(), false);
        Assert.assertEquals(v.update(BigInteger.ONE).getValue(), true);
        Assert.assertEquals(v.update(BigInteger.ZERO).getValue(), false);
        Assert.assertEquals(v.update(BigDecimal.ONE).getValue(), true);
        Assert.assertEquals(v.update(BigDecimal.ZERO).getValue(), false);
        Assert.assertEquals(v.update(BigDecimal.valueOf(1L, 4)).getValue(), true);
        Assert.assertEquals(v.update(BigDecimal.valueOf(0L, 5)).getValue(), false);

        Assert.assertThrows(IllegalArgumentException.class, () -> v.update('x'));
        Assert.assertThrows(IllegalArgumentException.class, () -> v.update('v'));
        Assert.assertThrows(IllegalArgumentException.class, () -> v.update("enabled"));
        Assert.assertThrows(IllegalArgumentException.class, () -> v.update("disabled"));
        Assert.assertThrows(IllegalArgumentException.class, () -> v.update((byte) 2));
        Assert.assertThrows(IllegalArgumentException.class, () -> v.update((short) -1));
        Assert.assertThrows(IllegalArgumentException.class, () -> v.update(3));
        Assert.assertThrows(IllegalArgumentException.class, () -> v.update(4L));
        Assert.assertThrows(IllegalArgumentException.class, () -> v.update(5F));
        Assert.assertThrows(IllegalArgumentException.class, () -> v.update(6D));
        Assert.assertThrows(IllegalArgumentException.class, () -> v.update(BigInteger.TEN));
        Assert.assertThrows(IllegalArgumentException.class, () -> v.update(BigDecimal.TEN));
    }

    @Test(groups = { "unit" })
    public void testValue() throws UnknownHostException {
        // null value
        checkNull(ClickHouseBoolValue.ofNull());
        checkNull(ClickHouseBoolValue.of(1).resetToNullOrEmpty());
        checkNull(ClickHouseBoolValue.of(0).resetToNullOrEmpty());
        checkNull(ClickHouseBoolValue.of(true).resetToNullOrEmpty());
        checkNull(ClickHouseBoolValue.of(false).resetToNullOrEmpty());

        // invalid values
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseBoolValue.of(2));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseBoolValue.of(-1));

        // non-null
        checkValue(ClickHouseBoolValue.of(0), false, // isInfinity
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
                false, // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:0")[0], // Inet6Address
                "false", // String
                "0", // SQL Expression
                LocalTime.ofSecondOfDay(0), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000000"), // UUID
                Integer.class, // Key class
                Boolean.class, // Value class
                new Object[] { Boolean.FALSE }, // Array
                new Boolean[] { Boolean.FALSE }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Boolean.FALSE }), // Map
                buildMap(new Object[] { 1 }, new Boolean[] { Boolean.FALSE }), // typed Map
                Arrays.asList(Boolean.FALSE) // Tuple
        );
        checkValue(ClickHouseBoolValue.of(1), false, // isInfinity
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
                true, // Object
                LocalDate.ofEpochDay(1L), // Date
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 1, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.1")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:1")[0], // Inet6Address
                "true", // String
                "1", // SQL Expression
                LocalTime.ofSecondOfDay(1), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000001"), // UUID
                Integer.class, // Key class
                Boolean.class, // Value class
                new Object[] { Boolean.TRUE }, // Array
                new Boolean[] { Boolean.TRUE }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Boolean.TRUE }), // Map
                buildMap(new Object[] { 1 }, new Boolean[] { Boolean.TRUE }), // typed Map
                Arrays.asList(Boolean.TRUE) // Tuple
        );

        checkValue(ClickHouseBoolValue.of(false), false, // isInfinity
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
                false, // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:0")[0], // Inet6Address
                "false", // String
                "0", // SQL Expression
                LocalTime.ofSecondOfDay(0), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000000"), // UUID
                Integer.class, // Key class
                Boolean.class, // Value class
                new Object[] { Boolean.FALSE }, // Array
                new Boolean[] { Boolean.FALSE }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Boolean.FALSE }), // Map
                buildMap(new Object[] { 1 }, new Boolean[] { Boolean.FALSE }), // typed Map
                Arrays.asList(Boolean.FALSE) // Tuple
        );
        checkValue(ClickHouseBoolValue.of(true), false, // isInfinity
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
                true, // Object
                LocalDate.ofEpochDay(1L), // Date
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 1, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.1")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:1")[0], // Inet6Address
                "true", // String
                "1", // SQL Expression
                LocalTime.ofSecondOfDay(1), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000001"), // UUID
                Integer.class, // Key class
                Boolean.class, // Value class
                new Object[] { Boolean.TRUE }, // Array
                new Boolean[] { Boolean.TRUE }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Boolean.TRUE }), // Map
                buildMap(new Object[] { 1 }, new Boolean[] { Boolean.TRUE }), // typed Map
                Arrays.asList(Boolean.TRUE) // Tuple
        );
    }
}
