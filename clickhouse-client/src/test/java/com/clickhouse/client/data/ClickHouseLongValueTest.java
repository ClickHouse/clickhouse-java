package com.clickhouse.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.UUID;
import org.junit.Assert;
import org.testng.annotations.Test;
import com.clickhouse.client.BaseClickHouseValueTest;
import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

public class ClickHouseLongValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testSignedValue() throws Exception {
        // null value
        checkNull(ClickHouseLongValue.ofNull(false));
        checkNull(ClickHouseLongValue.of(Long.MAX_VALUE, false).resetToNullOrEmpty());
        checkNull(ClickHouseLongValue.of(Long.MIN_VALUE, false).resetToNullOrEmpty());

        // non-null
        checkValue(ClickHouseLongValue.of(0, false), false, // isInfinity
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
                0L, // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:0")[0], // Inet6Address
                "0", // String
                "0", // SQL Expression
                LocalTime.ofSecondOfDay(0), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000000"), // UUID
                Object.class, // Key class
                Long.class, // Value class
                new Object[] { 0L }, // Array
                new Long[] { 0L }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 0L }), // Map
                buildMap(new Object[] { 1 }, new Long[] { 0L }), // typed Map
                Arrays.asList(Long.valueOf(0L)) // Tuple
        );
        checkValue(ClickHouseLongValue.of(1, false), false, // isInfinity
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
                1L, // Object
                LocalDate.ofEpochDay(1L), // Date
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 1, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.1")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:1")[0], // Inet6Address
                "1", // String
                "1", // SQL Expression
                LocalTime.ofSecondOfDay(1), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000001"), // UUID
                Object.class, // Key class
                Long.class, // Value class
                new Object[] { 1L }, // Array
                new Long[] { 1L }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 1L }), // Map
                buildMap(new Object[] { 1 }, new Long[] { 1L }), // typed Map
                Arrays.asList(Long.valueOf(1L)) // Tuple
        );
        checkValue(ClickHouseLongValue.of(2, false), false, // isInfinity
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
                2L, // Object
                LocalDate.ofEpochDay(2L), // Date
                LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 2, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.2")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:2")[0], // Inet6Address
                "2", // String
                "2", // SQL Expression
                LocalTime.ofSecondOfDay(2), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000002"), // UUID
                Object.class, // Key class
                Long.class, // Value class
                new Object[] { 2L }, // Array
                new Long[] { 2L }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 2L }), // Map
                buildMap(new Object[] { 1 }, new Long[] { 2L }), // typed Map
                Arrays.asList(Long.valueOf(2L)) // Tuple
        );

        ClickHouseLongValue v = ClickHouseLongValue.of(-1, false);
        Assert.assertFalse(v.isUnsigned());
        checkValue(ClickHouseLongValue.of(-1, false), false, // isInfinity
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
                -1L, // Object
                LocalDate.ofEpochDay(-1L), // Date
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(-1L, 999999999, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("255.255.255.255")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:ff")[0], // Inet6Address
                "-1", // String
                "-1", // SQL Expression
                java.time.DateTimeException.class, // Time
                UUID.fromString("00000000-0000-0000-ffff-ffffffffffff"), // UUID
                Object.class, // Key class
                Long.class, // Value class
                new Object[] { -1L }, // Array
                new Long[] { -1L }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { -1L }), // Map
                buildMap(new Object[] { 1 }, new Long[] { -1L }), // typed Map
                Arrays.asList(Long.valueOf(-1L)) // Tuple
        );
    }

    @Test(groups = { "unit" })
    public void testUnsignedValue() throws Exception {
        // null value
        checkNull(ClickHouseLongValue.ofNull(true));
        checkNull(ClickHouseLongValue.of(Long.MAX_VALUE, true).resetToNullOrEmpty());
        checkNull(ClickHouseLongValue.of(Long.MIN_VALUE, true).resetToNullOrEmpty());

        // non-null
        checkValue(ClickHouseLongValue.of(0, true), false, // isInfinity
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
                0L, // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:0")[0], // Inet6Address
                "0", // String
                "0", // SQL Expression
                LocalTime.ofSecondOfDay(0), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000000"), // UUID
                Object.class, // Key class
                Long.class, // Value class
                new Object[] { 0L }, // Array
                new Long[] { 0L }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 0L }), // Map
                buildMap(new Object[] { 1 }, new Long[] { 0L }), // typed Map
                Arrays.asList(Long.valueOf(0L)) // Tuple
        );
        checkValue(ClickHouseLongValue.of(1, true), false, // isInfinity
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
                1L, // Object
                LocalDate.ofEpochDay(1L), // Date
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 1, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.1")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:1")[0], // Inet6Address
                "1", // String
                "1", // SQL Expression
                LocalTime.ofSecondOfDay(1), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000001"), // UUID
                Object.class, // Key class
                Long.class, // Value class
                new Object[] { 1L }, // Array
                new Long[] { 1L }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 1L }), // Map
                buildMap(new Object[] { 1 }, new Long[] { 1L }), // typed Map
                Arrays.asList(Long.valueOf(1L)) // Tuple
        );
        checkValue(ClickHouseLongValue.of(2, true), false, // isInfinity
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
                2L, // Object
                LocalDate.ofEpochDay(2L), // Date
                LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 2, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.2")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:2")[0], // Inet6Address
                "2", // String
                "2", // SQL Expression
                LocalTime.ofSecondOfDay(2), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000002"), // UUID
                Object.class, // Key class
                Long.class, // Value class
                new Object[] { 2L }, // Array
                new Long[] { 2L }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 2L }), // Map
                buildMap(new Object[] { 1 }, new Long[] { 2L }), // typed Map
                Arrays.asList(Long.valueOf(2L)) // Tuple
        );

        ClickHouseLongValue v = ClickHouseLongValue.of(-1, true);
        Assert.assertTrue(v.isUnsigned());
        BigInteger bigInt = new BigInteger(1, new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF });
        checkValue(v, false, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                (byte) -1, // byte
                (short) -1, // short
                -1, // int
                -1L, // long
                -1F, // float
                -1D, // double
                new BigDecimal(bigInt), // BigDecimal
                new BigDecimal(bigInt, 3), // BigDecimal
                bigInt, // BigInteger
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                -1L, // Object
                LocalDate.ofEpochDay(-1L), // Date
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(new BigDecimal(bigInt, 9).longValue(),
                        new BigDecimal(bigInt, 9).remainder(BigDecimal.ONE).multiply(ClickHouseValues.NANOS).intValue(),
                        ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("255.255.255.255")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:ffff:ffff:ffff:ffff")[0], // Inet6Address
                "18446744073709551615", // String
                "18446744073709551615", // SQL Expression
                java.time.DateTimeException.class, // Time
                UUID.fromString("00000000-0000-0000-ffff-ffffffffffff"), // UUID
                Object.class, // Key class
                Long.class, // Value class
                new Object[] { -1L }, // Array
                new Long[] { -1L }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { -1L }), // Map
                buildMap(new Object[] { 1 }, new Long[] { -1L }), // typed Map
                Arrays.asList(Long.valueOf(-1L)) // Tuple
        );
    }
}
