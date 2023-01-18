package com.clickhouse.data.value;

import java.util.Arrays;
import org.testng.annotations.Test;
import com.clickhouse.data.BaseClickHouseValueTest;

public class ClickHouseTupleValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testMultipleValues() {
        // single type
        checkValue(ClickHouseTupleValue.of("one", "two"), UnsupportedOperationException.class, // isInfinity
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
                Arrays.asList("one", "two"), // Object
                UnsupportedOperationException.class, // Date
                UnsupportedOperationException.class, // DateTime
                UnsupportedOperationException.class, // DateTime(9)
                UnsupportedOperationException.class, // Inet4Address
                UnsupportedOperationException.class, // Inet6Address
                "[one, two]", // String
                "('one','two')", // SQL Expression
                UnsupportedOperationException.class, // Time
                UnsupportedOperationException.class, // UUID
                Integer.class, // Key class
                String.class, // Value class
                new Object[] { "one", "two" }, // Array
                new Object[] { "one", "two" }, // typed Array
                buildMap(new Object[] { 1, 2 }, new Object[] { "one", "two" }), // Map
                buildMap(new Integer[] { 1, 2 }, new String[] { "one", "two" }), // typed Map
                Arrays.asList("one", "two") // Tuple
        );

        // mixed types
        checkValue(ClickHouseTupleValue.of("seven", (byte) 7), UnsupportedOperationException.class, // isInfinity
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
                Arrays.asList("seven", (byte) 7), // Object
                UnsupportedOperationException.class, // Date
                UnsupportedOperationException.class, // DateTime
                UnsupportedOperationException.class, // DateTime(9)
                UnsupportedOperationException.class, // Inet4Address
                UnsupportedOperationException.class, // Inet6Address
                "[seven, 7]", // String
                "('seven',7)", // SQL Expression
                UnsupportedOperationException.class, // Time
                UnsupportedOperationException.class, // UUID
                Integer.class, // Key class
                Object.class, // Value class
                new Object[] { "seven", Byte.valueOf((byte) 7) }, // Array
                new Object[] { "seven", Byte.valueOf((byte) 7) }, // typed Array
                buildMap(new Object[] { 1, 2 }, new Object[] { "seven", Byte.valueOf((byte) 7) }), // Map
                buildMap(new Integer[] { 1, 2 }, new Object[] { "seven", Byte.valueOf((byte) 7) }), // typed
                                                                                                    // Map
                Arrays.asList("seven", (byte) 7) // Tuple
        );
    }

    @Test(groups = { "unit" })
    public void testSingleValue() {
        // null value
        checkNull(ClickHouseTupleValue.of().resetToNullOrEmpty(), false, 3, 9);
        checkNull(ClickHouseTupleValue.of(ClickHouseByteValue.of(0).asByte()).resetToNullOrEmpty(), false, 3, 9);

        // non-null
        checkValue(ClickHouseTupleValue.of(ClickHouseByteValue.of(0).asByte()), UnsupportedOperationException.class, // isInfinity
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
                Arrays.asList((byte) 0), // Object
                UnsupportedOperationException.class, // Date
                UnsupportedOperationException.class, // DateTime
                UnsupportedOperationException.class, // DateTime(9)
                UnsupportedOperationException.class, // Inet4Address
                UnsupportedOperationException.class, // Inet6Address
                "[0]", // String
                "(0)", // SQL Expression
                UnsupportedOperationException.class, // Time
                UnsupportedOperationException.class, // UUID
                Integer.class, // Key class
                Byte.class, // Value class
                new Object[] { Byte.valueOf((byte) 0) }, // Array
                new Byte[] { Byte.valueOf((byte) 0) }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { Byte.valueOf((byte) 0) }), // Map
                buildMap(new Integer[] { 1 }, new Byte[] { Byte.valueOf((byte) 0) }), // typed Map
                Arrays.asList((byte) 0) // Tuple
        );
    }
}
