package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.testng.annotations.Test;
import com.clickhouse.data.BaseClickHouseValueTest;
import com.clickhouse.data.ClickHouseDataType;

public class ClickHouseDoubleValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testValue() throws UnknownHostException {
        // null value
        checkNull(ClickHouseDoubleValue.ofNull());
        checkNull(ClickHouseDoubleValue.of(Double.MAX_VALUE).resetToNullOrEmpty());

        // NaN and Infinity
        checkValue(ClickHouseDoubleValue.of(Double.NaN), false, // isInfinity
                true, // isNan
                false, // isNull
                false, // boolean
                (byte) 0, // byte
                (short) 0, // short
                0, // int
                0L, // long
                Float.NaN, // float
                Double.NaN, // double
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigInteger
                ClickHouseDataType.values()[0].name(), // Enum<ClickHouseDataType>
                Double.NaN, // Object
                LocalDate.ofEpochDay(0L), // Date
                NumberFormatException.class, // DateTime
                NumberFormatException.class, // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                NumberFormatException.class, // Inet6Address
                "NaN", // String
                "NaN", // SQL Expression
                NumberFormatException.class, // Time
                NumberFormatException.class, // UUID
                Object.class, // Key class
                Double.class, // Value class
                new Object[] { Double.NaN }, // Array
                new Double[] { Double.NaN }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Double.NaN }), // Map
                buildMap(new Object[] { 1 }, new Double[] { Double.NaN }), // typed Map
                Arrays.asList(Double.NaN) // Tuple
        );
        checkValue(ClickHouseDoubleValue.of(Double.POSITIVE_INFINITY), true, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                (byte) -1, // byte
                (short) -1, // short
                2147483647, // int
                9223372036854775807L, // long
                Float.POSITIVE_INFINITY, // float
                Double.POSITIVE_INFINITY, // double
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigInteger
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                Double.POSITIVE_INFINITY, // Object
                DateTimeException.class, // Date
                NumberFormatException.class, // DateTime
                NumberFormatException.class, // DateTime(9)
                Inet4Address.getAllByName("127.255.255.255")[0], // Inet4Address
                NumberFormatException.class, // Inet6Address
                "Infinity", // String
                "Inf", // SQL Expression
                NumberFormatException.class, // Time
                NumberFormatException.class, // UUID
                Object.class, // Key class
                Double.class, // Value class
                new Object[] { Double.POSITIVE_INFINITY }, // Array
                new Double[] { Double.POSITIVE_INFINITY }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Double.POSITIVE_INFINITY }), // Map
                buildMap(new Object[] { 1 }, new Double[] { Double.POSITIVE_INFINITY }), // typed Map
                Arrays.asList(Double.POSITIVE_INFINITY) // Tuple
        );
        checkValue(ClickHouseDoubleValue.of(Double.NEGATIVE_INFINITY), true, // isInfinity
                false, // isNan
                false, // isNull
                false, // boolean
                (byte) 0, // byte
                (short) 0, // short
                -2147483648, // int
                -9223372036854775808L, // long
                Float.NEGATIVE_INFINITY, // float
                Double.NEGATIVE_INFINITY, // double
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigInteger
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                Double.NEGATIVE_INFINITY, // Object
                DateTimeException.class, // Date
                NumberFormatException.class, // DateTime
                NumberFormatException.class, // DateTime(9)
                Inet4Address.getAllByName("128.0.0.0")[0], // Inet4Address
                NumberFormatException.class, // Inet6Address
                "-Infinity", // String
                "-Inf", // SQL Expression
                NumberFormatException.class, // Time
                NumberFormatException.class, // UUID
                Object.class, // Key class
                Double.class, // Value class
                new Object[] { Double.NEGATIVE_INFINITY }, // Array
                new Double[] { Double.NEGATIVE_INFINITY }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Double.NEGATIVE_INFINITY }), // Map
                buildMap(new Object[] { 1 }, new Double[] { Double.NEGATIVE_INFINITY }), // typed Map
                Arrays.asList(Double.NEGATIVE_INFINITY) // Tuple
        );

        // non-null
        checkValue(ClickHouseDoubleValue.of(0D), false, // isInfinity
                false, // isNan
                false, // isNull
                false, // boolean
                (byte) 0, // byte
                (short) 0, // short
                0, // int
                0L, // long
                0F, // float
                0D, // double
                BigDecimal.ZERO, // BigDecimal
                new BigDecimal(BigInteger.ZERO, 3), // BigDecimal
                BigInteger.ZERO, // BigInteger
                ClickHouseDataType.values()[0].name(), // Enum<ClickHouseDataType>
                0D, // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:0")[0], // Inet6Address
                "0.0", // String
                "0.0", // SQL Expression
                LocalTime.ofSecondOfDay(0), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000000"), // UUID
                Object.class, // Key class
                Double.class, // Value class
                new Object[] { 0D }, // Array
                new Double[] { 0D }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 0D }), // Map
                buildMap(new Object[] { 1 }, new Double[] { 0D }), // typed Map
                Arrays.asList(Double.valueOf(0D)) // Tuple
        );
        checkValue(ClickHouseDoubleValue.of(1D), false, // isInfinity
                false, // isNan
                false, // isNull
                true, // boolean
                (byte) 1, // byte
                (short) 1, // short
                1, // int
                1L, // long
                1F, // float
                1D, // double
                BigDecimal.ONE, // BigDecimal
                new BigDecimal(BigInteger.ONE, 3), // BigDecimal
                BigInteger.ONE, // BigInteger
                ClickHouseDataType.values()[1].name(), // Enum<ClickHouseDataType>
                1D, // Object
                LocalDate.ofEpochDay(1L), // Date
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 1, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.1")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:1")[0], // Inet6Address
                "1.0", // String
                "1.0", // SQL Expression
                LocalTime.ofSecondOfDay(1), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000001"), // UUID
                Object.class, // Key class
                Double.class, // Value class
                new Object[] { 1D }, // Array
                new Double[] { 1D }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 1D }), // Map
                buildMap(new Object[] { 1 }, new Double[] { 1D }), // typed Map
                Arrays.asList(Double.valueOf(1D)) // Tuple
        );
        checkValue(ClickHouseDoubleValue.of(2D), false, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                (byte) 2, // byte
                (short) 2, // short
                2, // int
                2L, // long
                2F, // float
                2D, // double
                BigDecimal.valueOf(2D), // BigDecimal
                new BigDecimal(BigInteger.valueOf(2L), 3), // BigDecimal
                BigInteger.valueOf(2L), // BigInteger
                ClickHouseDataType.values()[2].name(), // Enum<ClickHouseDataType>
                2D, // Object
                LocalDate.ofEpochDay(2L), // Date
                LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 2, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.2")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:2")[0], // Inet6Address
                "2.0", // String
                "2.0", // SQL Expression
                LocalTime.ofSecondOfDay(2), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000002"), // UUID
                Object.class, // Key class
                Double.class, // Value class
                new Object[] { 2D }, // Array
                new Double[] { 2D }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 2D }), // Map
                buildMap(new Object[] { 1 }, new Double[] { 2D }), // typed Map
                Arrays.asList(Double.valueOf(2D)) // Tuple
        );

        checkValue(ClickHouseDoubleValue.of(-1D), false, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                (byte) -1, // byte
                (short) -1, // short
                -1, // int
                -1L, // long
                -1F, // float
                -1D, // double
                BigDecimal.valueOf(-1D), // BigDecimal
                new BigDecimal(BigInteger.valueOf(-1L), 3), // BigDecimal
                BigInteger.valueOf(-1L), // BigInteger
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                -1D, // Object
                LocalDate.ofEpochDay(-1L), // Date
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(-1L, 999999999, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("255.255.255.255")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:ff")[0], // Inet6Address
                "-1.0", // String
                "-1.0", // SQL Expression
                LocalTime.of(23, 59, 59), // Time
                UUID.fromString("00000000-0000-0000-ffff-ffffffffffff"), // UUID
                Object.class, // Key class
                Double.class, // Value class
                new Object[] { -1D }, // Array
                new Double[] { -1D }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { -1D }), // Map
                buildMap(new Object[] { 1 }, new Double[] { -1D }), // typed Map
                Arrays.asList(Double.valueOf(-1D)) // Tuple
        );

        checkValue(ClickHouseDoubleValue.of(4D / 3), false, // isInfinity
                false, // isNan
                false, // isNull
                true, // boolean
                (byte) 1, // byte
                (short) 1, // short
                1, // int
                1L, // long
                1.3333334F, // float
                1.3333333333333333D, // double
                BigDecimal.valueOf(4D / 3), // BigDecimal
                BigDecimal.valueOf(1333, 3), // BigDecimal
                BigInteger.ONE, // BigInteger
                ClickHouseDataType.values()[1].name(), // Enum<ClickHouseDataType>
                1.3333333333333333D, // Object
                LocalDate.ofEpochDay(1L), // Date
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(1L, 333333333, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.1")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:1")[0], // Inet6Address
                "1.3333333333333333", // String
                "1.3333333333333333", // SQL Expression
                LocalTime.ofSecondOfDay(1), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000001"), // UUID
                Object.class, // Key class
                Double.class, // Value class
                new Object[] { 1.3333333333333333D }, // Array
                new Double[] { 1.3333333333333333D }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { 1.3333333333333333D }), // Map
                buildMap(new Object[] { 1 }, new Double[] { 1.3333333333333333D }), // typed Map
                Collections.singletonList(1.3333333333333333D) // Tuple
        );
        checkValue(ClickHouseDoubleValue.of(-4D / 3), false, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                (byte) -1, // byte
                (short) -1, // short
                -1, // int
                -1L, // long
                -1.3333334F, // float
                -1.3333333333333333D, // double
                BigDecimal.valueOf(-4D / 3), // BigDecimal
                BigDecimal.valueOf(-1333, 3), // BigDecimal
                BigInteger.valueOf(-1L), // BigInteger
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                -1.3333333333333333D, // Object
                LocalDate.ofEpochDay(-1L), // Date
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(-2L, 666666667, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("255.255.255.255")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:ff")[0], // Inet6Address
                "-1.3333333333333333", // String
                "-1.3333333333333333", // SQL Expression
                LocalTime.of(23, 59, 59), // Time
                UUID.fromString("00000000-0000-0000-ffff-ffffffffffff"), // UUID
                Object.class, // Key class
                Double.class, // Value class
                new Object[] { -1.3333333333333333D }, // Array
                new Double[] { -1.3333333333333333D }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { -1.3333333333333333D }), // Map
                buildMap(new Object[] { 1 }, new Double[] { -1.3333333333333333D }), // typed Map
                Collections.singletonList(-1.3333333333333333D) // Tuple
        );
    }
}
