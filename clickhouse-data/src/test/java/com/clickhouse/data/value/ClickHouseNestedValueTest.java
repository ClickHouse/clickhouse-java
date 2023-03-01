package com.clickhouse.data.value;

import java.time.LocalDate;
import java.util.Arrays;
import org.testng.annotations.Test;
import com.clickhouse.data.BaseClickHouseValueTest;
import com.clickhouse.data.ClickHouseColumn;

public class ClickHouseNestedValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testMultipleValues() {
        // single type
        checkValue(
                ClickHouseNestedValue.of(ClickHouseColumn.parse("a String not null, b String null"),
                        new Object[][] { new String[] { "a1", "b1" }, new String[] { null, "b2" } }),
                UnsupportedOperationException.class, // isInfinity
                UnsupportedOperationException.class, // isNan
                false, // isNull
                UnsupportedOperationException.class, // boolean
                UnsupportedOperationException.class, // byte
                UnsupportedOperationException.class, // short
                UnsupportedOperationException.class, // int
                UnsupportedOperationException.class, // long
                UnsupportedOperationException.class, // float
                UnsupportedOperationException.class, // double
                UnsupportedOperationException.class, // BigDecimal
                UnsupportedOperationException.class, // BigDecimal
                UnsupportedOperationException.class, // BigInteger
                UnsupportedOperationException.class, // Enum<ClickHouseDataType>
                new Object[][] { new String[] { "a1", "b1" }, new String[] { null, "b2" } }, // Object
                UnsupportedOperationException.class, // Date
                UnsupportedOperationException.class, // DateTime
                UnsupportedOperationException.class, // DateTime(9)
                UnsupportedOperationException.class, // Inet4Address
                UnsupportedOperationException.class, // Inet6Address
                "[[a1, b1], [null, b2]]", // String
                "['a1','b1'],[NULL,'b2']", // SQL Expression
                UnsupportedOperationException.class, // Time
                UnsupportedOperationException.class, // UUID
                String.class, // Key class
                String[].class, // Value class
                new Object[][] { new String[] { "a1", "b1" }, new String[] { null, "b2" } }, // Array
                new Object[][] { new String[] { "a1", "b1" }, new String[] { null, "b2" } }, // typed Array
                buildMap(new Object[] { "a", "b" },
                        new Object[][] { new String[] { "a1", null }, new String[] { "b1", "b2" } }), // Map
                buildMap(new String[] { "a", "b" },
                        new Object[][] { new String[] { "a1", null }, new String[] { "b1", "b2" } }), // typed Map
                Arrays.asList(new Object[][] { new String[] { "a1", "b1" }, new String[] { null, "b2" } }) // Tuple
        );

        // mixed types
        checkValue(
                ClickHouseNestedValue.of(ClickHouseColumn.parse("a Nullable(UInt8), b Date"),
                        new Object[][] { { (short) 1, LocalDate.ofEpochDay(1L) }, { null, LocalDate.ofEpochDay(2L) },
                                { (short) 3, LocalDate.ofEpochDay(3L) } }),
                UnsupportedOperationException.class, // isInfinity
                UnsupportedOperationException.class, // isNan
                false, // isNull
                UnsupportedOperationException.class, // boolean
                UnsupportedOperationException.class, // byte
                UnsupportedOperationException.class, // short
                UnsupportedOperationException.class, // int
                UnsupportedOperationException.class, // long
                UnsupportedOperationException.class, // float
                UnsupportedOperationException.class, // double
                UnsupportedOperationException.class, // BigDecimal
                UnsupportedOperationException.class, // BigDecimal
                UnsupportedOperationException.class, // BigInteger
                UnsupportedOperationException.class, // Enum<ClickHouseDataType>
                new Object[][] { { (short) 1, LocalDate.ofEpochDay(1L) }, { null, LocalDate.ofEpochDay(2L) },
                        { (short) 3, LocalDate.ofEpochDay(3L) } }, // Object
                UnsupportedOperationException.class, // Date
                UnsupportedOperationException.class, // DateTime
                UnsupportedOperationException.class, // DateTime(9)
                UnsupportedOperationException.class, // Inet4Address
                UnsupportedOperationException.class, // Inet6Address
                "[[1, 1970-01-02], [null, 1970-01-03], [3, 1970-01-04]]", // String
                "[1,'1970-01-02'],[NULL,'1970-01-03'],[3,'1970-01-04']", // SQL Expression
                UnsupportedOperationException.class, // Time
                UnsupportedOperationException.class, // UUID
                String.class, // Key class
                Object[].class, // Value class
                new Object[][] { { (short) 1, LocalDate.ofEpochDay(1L) }, { null, LocalDate.ofEpochDay(2L) },
                        { (short) 3, LocalDate.ofEpochDay(3L) } }, // Array
                new Object[][] { { (short) 1, LocalDate.ofEpochDay(1L) }, { null, LocalDate.ofEpochDay(2L) },
                        { (short) 3, LocalDate.ofEpochDay(3L) } }, // typed Array
                buildMap(new Object[] { "a", "b" },
                        new Object[][] { { (short) 1, null, (short) 3 },
                                { LocalDate.ofEpochDay(1L), LocalDate.ofEpochDay(2L), LocalDate.ofEpochDay(3L) } }), // Map
                buildMap(new String[] { "a", "b" },
                        new Object[][] { new Short[] { (short) 1, null, (short) 3 },
                                new LocalDate[] { LocalDate.ofEpochDay(1L), LocalDate.ofEpochDay(2L),
                                        LocalDate.ofEpochDay(3L) } }), // typed Map
                Arrays.asList(new Object[][] { { (short) 1, LocalDate.ofEpochDay(1L) },
                        { null, LocalDate.ofEpochDay(2L) }, { (short) 3, LocalDate.ofEpochDay(3L) } }) // Tuple
        );
    }

    @Test(groups = { "unit" })
    public void testSingleValue() {
        // null value
        checkNull(ClickHouseNestedValue.ofEmpty(ClickHouseColumn.parse("a Nullable(String)")), false, 3, 9);
        checkNull(ClickHouseNestedValue.ofEmpty(ClickHouseColumn.parse("a String not null")), false, 3, 9);
        checkNull(
                ClickHouseNestedValue.ofEmpty(ClickHouseColumn.parse("a String null")).update("x").resetToNullOrEmpty(),
                false, 3, 9);
        checkNull(
                ClickHouseNestedValue.of(ClickHouseColumn.parse("a String not null, b Int8"),
                        new Object[][] { { "a", (byte) 1 }, { "b", (byte) 2 } }).resetToNullOrEmpty(),
                false, 3, 9);

        checkValue(
                ClickHouseNestedValue.of(ClickHouseColumn.parse("a Int8 not null"),
                        new Object[][] { new Byte[] { (byte) 1 } }),
                UnsupportedOperationException.class, // isInfinity
                UnsupportedOperationException.class, // isNan
                false, // isNull
                UnsupportedOperationException.class, // boolean
                UnsupportedOperationException.class, // byte
                UnsupportedOperationException.class, // short
                UnsupportedOperationException.class, // int
                UnsupportedOperationException.class, // long
                UnsupportedOperationException.class, // float
                UnsupportedOperationException.class, // double
                UnsupportedOperationException.class, // BigDecimal
                UnsupportedOperationException.class, // BigDecimal
                UnsupportedOperationException.class, // BigInteger
                UnsupportedOperationException.class, // Enum<ClickHouseDataType>
                new Object[][] { new Byte[] { (byte) 1 } }, // Object
                UnsupportedOperationException.class, // Date
                UnsupportedOperationException.class, // DateTime
                UnsupportedOperationException.class, // DateTime(9)
                UnsupportedOperationException.class, // Inet4Address
                UnsupportedOperationException.class, // Inet6Address
                "[[1]]", // String
                "[1]", // SQL Expression
                UnsupportedOperationException.class, // Time
                UnsupportedOperationException.class, // UUID
                String.class, // Key class
                Object[].class, // Value class
                new Object[][] { new Byte[] { Byte.valueOf((byte) 1) } }, // Array
                new Object[][] { new Byte[] { Byte.valueOf((byte) 1) } }, // typed Array
                buildMap(new Object[] { "a" }, new Object[][] { new Byte[] { Byte.valueOf((byte) 1) } }), // Map
                buildMap(new String[] { "a" }, new Object[][] { new Byte[] { Byte.valueOf((byte) 1) } }), // typed Map
                Arrays.asList(new Object[][] { new Byte[] { Byte.valueOf((byte) 1) } }) // Tuple
        );
    }
}
