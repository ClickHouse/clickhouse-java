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

public class ClickHouseShortValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testSignedValue() throws UnknownHostException {
        // null value
        checkNull(ClickHouseShortValue.ofNull());
        checkNull(ClickHouseShortValue.of(Short.MAX_VALUE).resetToNullOrEmpty());

        // non-null
        checkValue(ClickHouseShortValue.of(0), false, // isInfinity
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
                (short) 0, // Object
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
                Short.class, // Value class
                new Object[] { (short) 0 }, // Array
                new Short[] { (short) 0 }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { (short) 0 }), // Map
                buildMap(new Object[] { 1 }, new Short[] { (short) 0 }), // typed Map
                Arrays.asList((short) 0) // Tuple
        );
        checkValue(ClickHouseShortValue.of(1), false, // isInfinity
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
                (short) 1, // Object
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
                Short.class, // Value class
                new Object[] { (short) 1 }, // Array
                new Short[] { (short) 1 }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { (short) 1 }), // Map
                buildMap(new Object[] { 1 }, new Short[] { (short) 1 }), // typed Map
                Arrays.asList((short) 1) // Tuple
        );
        checkValue(ClickHouseShortValue.of(2), false, // isInfinity
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
                (short) 2, // Object
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
                Short.class, // Value class
                new Object[] { (short) 2 }, // Array
                new Short[] { (short) 2 }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { (short) 2 }), // Map
                buildMap(new Object[] { 1 }, new Short[] { (short) 2 }), // typed Map
                Arrays.asList((short) 2) // Tuple
        );

        checkValue(ClickHouseShortValue.of(-1), false, // isInfinity
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
                (short) -1, // Object
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
                Short.class, // Value class
                new Object[] { (short) -1 }, // Array
                new Short[] { (short) -1 }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { (short) -1 }), // Map
                buildMap(new Object[] { 1 }, new Short[] { (short) -1 }), // typed Map
                Arrays.asList((short) -1) // Tuple
        );
    }

    @Test(groups = { "unit" })
    public void testUnsignedValue() throws UnknownHostException {
        // null value
        checkNull(ClickHouseShortValue.ofUnsignedNull());
        checkNull(ClickHouseShortValue.ofUnsigned(Short.MAX_VALUE).resetToNullOrEmpty());

        // non-null
        checkValue(ClickHouseShortValue.ofUnsigned(0), false, // isInfinity
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
                UnsignedShort.ZERO, // Object
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
                UnsignedShort.class, // Value class
                new Object[] { UnsignedShort.ZERO }, // Array
                new UnsignedShort[] { UnsignedShort.ZERO }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { UnsignedShort.ZERO }), // Map
                buildMap(new Object[] { 1 }, new UnsignedShort[] { UnsignedShort.ZERO }), // typed Map
                Arrays.asList(UnsignedShort.ZERO) // Tuple
        );
        checkValue(ClickHouseShortValue.ofUnsigned(1), false, // isInfinity
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
                UnsignedShort.ONE, // Object
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
                UnsignedShort.class, // Value class
                new Object[] { UnsignedShort.ONE }, // Array
                new UnsignedShort[] { UnsignedShort.ONE }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { UnsignedShort.ONE }), // Map
                buildMap(new Object[] { 1 }, new UnsignedShort[] { UnsignedShort.ONE }), // typed Map
                Arrays.asList(UnsignedShort.ONE) // Tuple
        );
        checkValue(ClickHouseShortValue.ofUnsigned(2), false, // isInfinity
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
                UnsignedShort.valueOf((short) 2), // Object
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
                UnsignedShort.class, // Value class
                new Object[] { UnsignedShort.valueOf((short) 2) }, // Array
                new UnsignedShort[] { UnsignedShort.valueOf((short) 2) }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { UnsignedShort.valueOf((short) 2) }), // Map
                buildMap(new Object[] { 1 }, new UnsignedShort[] { UnsignedShort.valueOf((short) 2) }), // typed Map
                Arrays.asList(UnsignedShort.valueOf((short) 2)) // Tuple
        );

        checkValue(ClickHouseShortValue.ofUnsigned(-1), false, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                (byte) -1, // byte
                (short) -1, // short
                65535, // int
                65535L, // long
                65535F, // float
                65535D, // double
                BigDecimal.valueOf(65535L), // BigDecimal
                new BigDecimal(BigInteger.valueOf(65535L), 3), // BigDecimal
                BigInteger.valueOf(65535L), // BigInteger
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                UnsignedShort.MAX_VALUE, // Object
                LocalDate.ofEpochDay(65535L), // Date
                LocalDateTime.ofEpochSecond(65535L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 65535, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.255.255")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:ffff")[0], // Inet6Address
                "65535", // String
                "65535", // SQL Expression
                LocalTime.of(18, 12, 15), // Time
                UUID.fromString("00000000-0000-0000-0000-00000000ffff"), // UUID
                Object.class, // Key class
                UnsignedShort.class, // Value class
                new Object[] { UnsignedShort.MAX_VALUE }, // Array
                new UnsignedShort[] { UnsignedShort.MAX_VALUE }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { UnsignedShort.MAX_VALUE }), // Map
                buildMap(new Object[] { 1 }, new UnsignedShort[] { UnsignedShort.MAX_VALUE }), // typed Map
                Arrays.asList(UnsignedShort.MAX_VALUE) // Tuple
        );
    }
}
