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
import com.clickhouse.data.ClickHouseValues;

public class ClickHouseDateTimeValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testUpdate() {
        Assert.assertEquals(ClickHouseDateTimeValue.ofNull(0, ClickHouseValues.UTC_TIMEZONE).update(-1L).getValue(),
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC));
        Assert.assertEquals(ClickHouseDateTimeValue.ofNull(0, ClickHouseValues.UTC_TIMEZONE).update(-1.1F).getValue(),
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC));
        Assert.assertEquals(ClickHouseDateTimeValue.ofNull(3, ClickHouseValues.UTC_TIMEZONE).update(-1L).getValue(),
                LocalDateTime.ofEpochSecond(-1L, 999000000, ZoneOffset.UTC));
        Assert.assertEquals(ClickHouseDateTimeValue.ofNull(3, ClickHouseValues.UTC_TIMEZONE).update(-1.1F).getValue(),
                LocalDateTime.ofEpochSecond(-2L, 900000000, ZoneOffset.UTC));

        Assert.assertEquals(
                ClickHouseDateTimeValue.ofNull(9, ClickHouseValues.UTC_TIMEZONE)
                        .update(new BigDecimal(BigInteger.ONE, 9)).getValue(),
                LocalDateTime.ofEpochSecond(0L, 1, ZoneOffset.UTC));
        Assert.assertEquals(
                ClickHouseDateTimeValue.ofNull(9, ClickHouseValues.UTC_TIMEZONE)
                        .update(new BigDecimal(BigInteger.valueOf(-1L), 9)).getValue(),
                LocalDateTime.ofEpochSecond(-1L, 999999999, ZoneOffset.UTC));
    }

    @Test(groups = { "unit" })
    public void testValueWithoutScale() throws UnknownHostException {
        // null value
        checkNull(ClickHouseDateTimeValue.ofNull(0, ClickHouseValues.UTC_TIMEZONE));
        checkNull(
                ClickHouseDateTimeValue.of(LocalDateTime.now(), 0, ClickHouseValues.UTC_TIMEZONE).resetToNullOrEmpty());

        // non-null
        checkValue(
                ClickHouseDateTimeValue.of(LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC), 0,
                        ClickHouseValues.UTC_TIMEZONE),
                false, // isInfinity
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
                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC), // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:0")[0], // Inet6Address
                "1970-01-01 00:00:00", // String
                "'1970-01-01 00:00:00'", // SQL Expression
                LocalTime.ofSecondOfDay(0L), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000000"), // UUID
                Object.class, // Key class
                LocalDateTime.class, // Value class
                new Object[] { LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC) }, // Array
                new LocalDateTime[] { LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC) }, // typed
                                                                                            // Array
                buildMap(new Object[] { 1 }, new Object[] { LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC) }), // Map
                buildMap(new Object[] { 1 },
                        new LocalDateTime[] { LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC) }), // typed
                                                                                                     // Map
                Arrays.asList(LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC)) // Tuple
        );
        checkValue(
                ClickHouseDateTimeValue.of(LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), 0,
                        ClickHouseValues.UTC_TIMEZONE),
                false, // isInfinity
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
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.1")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:1")[0], // Inet6Address
                "1970-01-01 00:00:01", // String
                "'1970-01-01 00:00:01'", // SQL Expression
                LocalTime.ofSecondOfDay(1L), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000001"), // UUID
                Object.class, // Key class
                LocalDateTime.class, // Value class
                new Object[] { LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC) }, // Array
                new LocalDateTime[] { LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC) }, // typed
                                                                                            // Array
                buildMap(new Object[] { 1 }, new Object[] { LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC) }), // Map
                buildMap(new Object[] { 1 },
                        new LocalDateTime[] { LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC) }), // typed
                                                                                                     // Map
                Arrays.asList(LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC)) // Tuple
        );
        checkValue(
                ClickHouseDateTimeValue.of(LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC), 0,
                        ClickHouseValues.UTC_TIMEZONE),
                false, // isInfinity
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
                LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC), // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.2")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:2")[0], // Inet6Address
                "1970-01-01 00:00:02", // String
                "'1970-01-01 00:00:02'", // SQL Expression
                LocalTime.ofSecondOfDay(2L), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000002"), // UUID
                Object.class, // Key class
                LocalDateTime.class, // Value class
                new Object[] { LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC) }, // Array
                new LocalDateTime[] { LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC) }, // typed
                                                                                            // Array
                buildMap(new Object[] { 1 }, new Object[] { LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC) }), // Map
                buildMap(new Object[] { 1 },
                        new LocalDateTime[] { LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC) }), // typed
                                                                                                     // Map
                Arrays.asList(LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC)) // Tuple
        );
    }

    @Test(groups = { "unit" })
    public void testValueWithScale() throws UnknownHostException {
        // null value
        checkNull(ClickHouseDateTimeValue.ofNull(3, ClickHouseValues.UTC_TIMEZONE));
        checkNull(
                ClickHouseDateTimeValue.of(LocalDateTime.now(), 9, ClickHouseValues.UTC_TIMEZONE).resetToNullOrEmpty());

        // non-null
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(0L, 123456789, ZoneOffset.UTC);
        checkValue(ClickHouseDateTimeValue.of(dateTime, 3, ClickHouseValues.UTC_TIMEZONE), false, // isInfinity
                false, // isNan
                false, // isNull
                false, // boolean
                (byte) 0, // byte
                (short) 0, // short
                0, // int
                0L, // long
                0.123456789F, // float
                0.123456789D, // double
                BigDecimal.valueOf(0L), // BigDecimal
                BigDecimal.valueOf(0.123D), // BigDecimal
                BigInteger.ZERO, // BigInteger
                ClickHouseDataType.values()[0].name(), // Enum<ClickHouseDataType>
                dateTime, // Object
                LocalDate.ofEpochDay(0L), // Date
                dateTime, // DateTime
                dateTime, // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:0")[0], // Inet6Address
                "1970-01-01 00:00:00.123456789", // String
                "'1970-01-01 00:00:00.123456789'", // SQL Expression
                LocalTime.ofNanoOfDay(123456789L), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000000"), // UUID
                Object.class, // Key class
                LocalDateTime.class, // Value class
                new Object[] { dateTime }, // Array
                new LocalDateTime[] { dateTime }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { dateTime }), // Map
                buildMap(new Object[] { 1 }, new LocalDateTime[] { dateTime }), // typed
                                                                                // Map
                Arrays.asList(dateTime) // Tuple
        );
        Assert.assertEquals(ClickHouseDateTimeValue.of(dateTime, 3, ClickHouseValues.UTC_TIMEZONE).asBigDecimal(4),
                BigDecimal.valueOf(0.1234D));
    }
}
