package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.UUID;
import org.testng.annotations.Test;
import com.clickhouse.data.BaseClickHouseValueTest;
import com.clickhouse.data.ClickHouseDataType;

public class ClickHouseDateValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testValue() throws UnknownHostException {
        // null value
        checkNull(ClickHouseDateValue.ofNull());
        checkNull(ClickHouseDateValue.of(LocalDate.now()).resetToNullOrEmpty());

        // non-null
        checkValue(ClickHouseDateValue.of(0), false, // isInfinity
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
                LocalDate.ofEpochDay(0L), // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime
                LocalDateTime.of(LocalDate.ofEpochDay(0L), LocalTime.ofSecondOfDay(0)), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:0")[0], // Inet6Address
                "1970-01-01", // String
                "'1970-01-01'", // SQL Expression
                LocalTime.ofSecondOfDay(0), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000000"), // UUID
                Object.class, // Key class
                LocalDate.class, // Value class
                new Object[] { LocalDate.ofEpochDay(0L) }, // Array
                new LocalDate[] { LocalDate.ofEpochDay(0L) }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { LocalDate.ofEpochDay(0L) }), // Map
                buildMap(new Object[] { 1 }, new LocalDate[] { LocalDate.ofEpochDay(0L) }), // typed Map
                Arrays.asList(LocalDate.ofEpochDay(0L)) // Tuple
        );
        checkValue(ClickHouseDateValue.of(1L), false, // isInfinity
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
                LocalDate.ofEpochDay(1L), // Object
                LocalDate.ofEpochDay(1L), // Date
                LocalDateTime.of(LocalDate.ofEpochDay(1L), LocalTime.ofSecondOfDay(0)), // DateTime
                LocalDateTime.of(LocalDate.ofEpochDay(1L), LocalTime.ofSecondOfDay(0)), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.1")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:1")[0], // Inet6Address
                "1970-01-02", // String
                "'1970-01-02'", // SQL Expression
                LocalTime.ofSecondOfDay(0), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000001"), // UUID
                Object.class, // Key class
                LocalDate.class, // Value class
                new Object[] { LocalDate.ofEpochDay(1L) }, // Array
                new LocalDate[] { LocalDate.ofEpochDay(1L) }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { LocalDate.ofEpochDay(1L) }), // Map
                buildMap(new Object[] { 1 }, new LocalDate[] { LocalDate.ofEpochDay(1L) }), // typed Map
                Arrays.asList(LocalDate.ofEpochDay(1L)) // Tuple
        );
        checkValue(ClickHouseDateValue.of(2L), false, // isInfinity
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
                LocalDate.ofEpochDay(2L), // Object
                LocalDate.ofEpochDay(2L), // Date
                LocalDateTime.of(LocalDate.ofEpochDay(2L), LocalTime.ofSecondOfDay(0)), // DateTime
                LocalDateTime.of(LocalDate.ofEpochDay(2L), LocalTime.ofSecondOfDay(0)), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.2")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:2")[0], // Inet6Address
                "1970-01-03", // String
                "'1970-01-03'", // SQL Expression
                LocalTime.ofSecondOfDay(0), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000002"), // UUID
                Object.class, // Key class
                LocalDate.class, // Value class
                new Object[] { LocalDate.ofEpochDay(2L) }, // Array
                new LocalDate[] { LocalDate.ofEpochDay(2L) }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { LocalDate.ofEpochDay(2L) }), // Map
                buildMap(new Object[] { 1 }, new LocalDate[] { LocalDate.ofEpochDay(2L) }), // typed Map
                Arrays.asList(LocalDate.ofEpochDay(2L)) // Tuple
        );
    }
}
