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
import org.testng.annotations.Test;
import com.clickhouse.data.BaseClickHouseValueTest;
import com.clickhouse.data.ClickHouseDataType;

public class ClickHouseIntegerValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testSignedValue() throws UnknownHostException {
        // null value
        checkNull(ClickHouseIntegerValue.ofNull());
        checkNull(ClickHouseIntegerValue.of(Integer.MAX_VALUE).resetToNullOrEmpty());

        // non-null
        checkValue(ClickHouseIntegerValue.of(0), false, // isInfinity
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
                (int) 0, // Object
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
                Integer.class, // Value class
                new Object[] { 0 }, // Array
                new Integer[] { 0 }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 0 }), // Map
                buildMap(new Object[] { 1 }, new Integer[] { 0 }), // typed Map
                Arrays.asList(Integer.valueOf(0)) // Tuple
        );
        checkValue(ClickHouseIntegerValue.of(1), false, // isInfinity
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
                (int) 1, // Object
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
                Integer.class, // Value class
                new Object[] { 1 }, // Array
                new Integer[] { 1 }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 1 }), // Map
                buildMap(new Object[] { 1 }, new Integer[] { 1 }), // typed Map
                Arrays.asList(1) // Tuple
        );
        checkValue(ClickHouseIntegerValue.of(2), false, // isInfinity
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
                (int) 2, // Object
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
                Integer.class, // Value class
                new Object[] { 2 }, // Array
                new Integer[] { 2 }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 2 }), // Map
                buildMap(new Object[] { 1 }, new Integer[] { 2 }), // typed Map
                Arrays.asList(2) // Tuple
        );

        checkValue(ClickHouseIntegerValue.of(-1), false, // isInfinity
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
                (int) -1, // Object
                LocalDate.ofEpochDay(-1L), // Date
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(-1L, 999999999, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("255.255.255.255")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:ff")[0], // Inet6Address
                "-1", // String
                "-1", // SQL Expression
                LocalTime.of(23, 59, 59), // Time
                UUID.fromString("00000000-0000-0000-ffff-ffffffffffff"), // UUID
                Object.class, // Key class
                Integer.class, // Value class
                new Object[] { -1 }, // Array
                new Integer[] { -1 }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { -1 }), // Map
                buildMap(new Object[] { 1 }, new Integer[] { -1 }), // typed Map
                Arrays.asList(-1) // Tuple
        );
    }

    @Test(groups = { "unit" })
    public void testUnsignedValue() throws UnknownHostException {
        // null value
        checkNull(ClickHouseIntegerValue.ofUnsignedNull());
        checkNull(ClickHouseIntegerValue.ofUnsigned(Integer.MAX_VALUE).resetToNullOrEmpty());

        // non-null
        checkValue(ClickHouseIntegerValue.ofUnsigned(0), false, // isInfinity
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
                UnsignedInteger.ZERO, // Object
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
                UnsignedInteger.class, // Value class
                new Object[] { UnsignedInteger.ZERO }, // Array
                new UnsignedInteger[] { UnsignedInteger.ZERO }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { UnsignedInteger.ZERO }), // Map
                buildMap(new Object[] { 1 }, new UnsignedInteger[] { UnsignedInteger.ZERO }), // typed Map
                Arrays.asList(UnsignedInteger.ZERO) // Tuple
        );
        checkValue(ClickHouseIntegerValue.ofUnsigned(1), false, // isInfinity
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
                UnsignedInteger.ONE, // Object
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
                UnsignedInteger.class, // Value class
                new Object[] { UnsignedInteger.ONE }, // Array
                new UnsignedInteger[] { UnsignedInteger.ONE }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { UnsignedInteger.ONE }), // Map
                buildMap(new Object[] { 1 }, new UnsignedInteger[] { UnsignedInteger.ONE }), // typed Map
                Arrays.asList(UnsignedInteger.ONE) // Tuple
        );
        checkValue(ClickHouseIntegerValue.ofUnsigned(2), false, // isInfinity
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
                UnsignedInteger.TWO, // Object
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
                UnsignedInteger.class, // Value class
                new Object[] { UnsignedInteger.TWO }, // Array
                new UnsignedInteger[] { UnsignedInteger.TWO }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { UnsignedInteger.TWO }), // Map
                buildMap(new Object[] { 1 }, new UnsignedInteger[] { UnsignedInteger.TWO }), // typed Map
                Arrays.asList(UnsignedInteger.TWO) // Tuple
        );

        checkValue(ClickHouseIntegerValue.ofUnsigned(-1), false, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                (byte) -1, // byte
                (short) -1, // short
                -1, // int
                4294967295L, // long
                4294967295F, // float
                4294967295D, // double
                BigDecimal.valueOf(4294967295L), // BigDecimal
                new BigDecimal(BigInteger.valueOf(4294967295L), 3), // BigDecimal
                BigInteger.valueOf(4294967295L), // BigInteger
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                UnsignedInteger.MAX_VALUE, // Object
                LocalDate.ofEpochDay(4294967295L), // Date
                LocalDateTime.ofEpochSecond(4294967295L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(4L, 294967295, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("255.255.255.255")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:ffff:ffff")[0], // Inet6Address
                "4294967295", // String
                "4294967295", // SQL Expression
                LocalTime.of(6, 28, 15), // Time
                UUID.fromString("00000000-0000-0000-0000-0000ffffffff"), // UUID
                Object.class, // Key class
                UnsignedInteger.class, // Value class
                new Object[] { UnsignedInteger.MAX_VALUE }, // Array
                new UnsignedInteger[] { UnsignedInteger.MAX_VALUE }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { UnsignedInteger.MAX_VALUE }), // Map
                buildMap(new Object[] { 1 }, new UnsignedInteger[] { UnsignedInteger.MAX_VALUE }), // typed Map
                Arrays.asList(UnsignedInteger.MAX_VALUE) // Tuple
        );
    }
}
