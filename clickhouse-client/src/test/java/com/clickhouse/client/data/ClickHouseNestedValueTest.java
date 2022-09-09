package com.clickhouse.client.data;

import java.time.LocalDate;
import java.util.Arrays;
import org.testng.annotations.Test;
import com.clickhouse.client.BaseClickHouseValueTest;
import com.clickhouse.client.ClickHouseColumn;

public class ClickHouseNestedValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testMultipleValues() {
        // single type
        checkValue(
                ClickHouseNestedValue.of(ClickHouseColumn.parse("a String not null, b String null"),
                        new Object[][] { new String[] { "a1", "a2" }, new String[] { null, "b2" } }),
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
                new Object[][] { new String[] { "a1", "a2" }, new String[] { null, "b2" } }, // Object
                UnsupportedOperationException.class, // Date
                UnsupportedOperationException.class, // DateTime
                UnsupportedOperationException.class, // DateTime(9)
                UnsupportedOperationException.class, // Inet4Address
                UnsupportedOperationException.class, // Inet6Address
                "[[a1, a2], [null, b2]]", // String
                "['a1','a2'],[NULL,'b2']", // SQL Expression
                UnsupportedOperationException.class, // Time
                UnsupportedOperationException.class, // UUID
                String.class, // Key class
                Object[].class, // Value class
                new Object[][] { new String[] { "a1", "a2" }, new String[] { null, "b2" } }, // Array
                new Object[][] { new String[] { "a1", "a2" }, new String[] { null, "b2" } }, // typed Array
                buildMap(new Object[] { "a", "b" },
                        new Object[][] { new String[] { "a1", "a2" }, new String[] { null, "b2" } }), // Map
                buildMap(new String[] { "a", "b" },
                        new Object[][] { new String[] { "a1", "a2" }, new String[] { null, "b2" } }), // typed Map
                Arrays.asList(new Object[][] { new String[] { "a1", "a2" }, new String[] { null, "b2" } }) // Tuple
        );

        // mixed types
        checkValue(
                ClickHouseNestedValue.of(ClickHouseColumn.parse("a Nullable(UInt8), b Date"),
                        new Object[][] { new Short[] { (short) 1, null },
                                new LocalDate[] { LocalDate.ofEpochDay(1L), LocalDate.ofEpochDay(2L) } }),
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
                new Object[][] { new Short[] { (short) 1, null },
                        new LocalDate[] { LocalDate.ofEpochDay(1L), LocalDate.ofEpochDay(2L) } }, // Object
                UnsupportedOperationException.class, // Date
                UnsupportedOperationException.class, // DateTime
                UnsupportedOperationException.class, // DateTime(9)
                UnsupportedOperationException.class, // Inet4Address
                UnsupportedOperationException.class, // Inet6Address
                "[[1, null], [1970-01-02, 1970-01-03]]", // String
                "[1,NULL],['1970-01-02','1970-01-03']", // SQL Expression
                UnsupportedOperationException.class, // Time
                UnsupportedOperationException.class, // UUID
                String.class, // Key class
                Object[].class, // Value class
                new Object[][] { new Short[] { (short) 1, null },
                        new LocalDate[] { LocalDate.ofEpochDay(1L), LocalDate.ofEpochDay(2L) } }, // Array
                new Object[][] { new Short[] { (short) 1, null },
                        new LocalDate[] { LocalDate.ofEpochDay(1L), LocalDate.ofEpochDay(2L) } }, // typed Array
                buildMap(new Object[] { "a", "b" },
                        new Object[][] { new Short[] { (short) 1, null },
                                new LocalDate[] { LocalDate.ofEpochDay(1L), LocalDate.ofEpochDay(2L) } }), // Map
                buildMap(new String[] { "a", "b" },
                        new Object[][] { new Short[] { (short) 1, null },
                                new LocalDate[] { LocalDate.ofEpochDay(1L), LocalDate.ofEpochDay(2L) } }), // typed Map
                Arrays.asList(new Object[][] { new Short[] { (short) 1, null },
                        new LocalDate[] { LocalDate.ofEpochDay(1L), LocalDate.ofEpochDay(2L) } }) // Tuple
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
                        new Object[][] { new String[] { "a" }, new Byte[] { (byte) 1 } }).resetToNullOrEmpty(),
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
