package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.clickhouse.data.BaseClickHouseValueTest;
import com.clickhouse.data.ClickHouseValues;

public class ClickHouseStringValueTest extends BaseClickHouseValueTest {
    @DataProvider(name = "stateProvider")
    public Object[][] getNodeSelectors() {
        return new Object[][] { new Object[] { null, true, false, false }, new Object[] { "2", false, false, false },
                new Object[] { "NaN", false, true, false }, new Object[] { "-Infinity", false, false, true },
                new Object[] { "Infinity", false, false, true }, new Object[] { "+Infinity", false, false, true } };
    }

    @Test(groups = { "unit" })
    public void testInitiation() {
        String value = null;
        ClickHouseStringValue v = ClickHouseStringValue.of(null, value);
        Assert.assertEquals(v.asString(), value);
        Assert.assertEquals(v.asObject(), value);

        v = ClickHouseStringValue.of(null, value = "");
        Assert.assertEquals(v.asString(), value);
        Assert.assertEquals(v.asObject(), value);

        v = ClickHouseStringValue.of(null, value = "123");
        Assert.assertEquals(v.asString(), value);
        Assert.assertEquals(v.asObject(), value);

        // same instance but different value
        Assert.assertEquals(v, v.update("321"));
        Assert.assertEquals(v, ClickHouseStringValue.of(v, "456"));
        Assert.assertNotEquals(v.asString(), v.update("789").asString());
        Assert.assertNotEquals(v.asString(), v.update("987").asString());
    }

    @Test(dataProvider = "stateProvider", groups = { "unit" })
    public void testState(String value, boolean isNull, boolean isNaN, boolean isInf) {
        ClickHouseStringValue v = ClickHouseStringValue.of(null, value);
        Assert.assertEquals(v.isNullOrEmpty(), isNull);
        Assert.assertEquals(v.isNaN(), isNaN);
        Assert.assertEquals(v.isInfinity(), isInf);
    }

    @Test(groups = { "unit" })
    public void testTypeConversion() {
        Assert.assertEquals(ClickHouseStringValue.of(null, "2021-03-04 15:06:27.123456789").asDateTime(),
                LocalDateTime.of(2021, 3, 4, 15, 6, 27, 123456789));
    }

    @Test(groups = { "unit" })
    public void testBinaryValue() {
        Assert.assertEquals(ClickHouseStringValue.of((byte[]) null).asBinary(), null);
        Assert.assertEquals(ClickHouseStringValue.of((String) null).asBinary(), null);
        Assert.assertEquals(ClickHouseStringValue.of(new byte[0]).asBinary(), new byte[0]);
        Assert.assertEquals(ClickHouseStringValue.of("").asBinary(), new byte[0]);
        Assert.assertEquals(ClickHouseStringValue.of((byte[]) null).asBinary(0), null);
        Assert.assertEquals(ClickHouseStringValue.of((String) null).asBinary(0), null);
        Assert.assertEquals(ClickHouseStringValue.of(new byte[0]).asBinary(0), new byte[0]);
        Assert.assertEquals(ClickHouseStringValue.of("").asBinary(0), new byte[0]);

        Assert.assertEquals(ClickHouseStringValue.of("").asBinary(1), new byte[] { 0 });
        Assert.assertEquals(ClickHouseStringValue.of("ab").asBinary(1), new byte[] { 97, 98 });
        Assert.assertEquals(ClickHouseStringValue.of("ab").asBinary(5), new byte[] { 97, 98, 0, 0, 0 });

        Assert.assertEquals(ClickHouseStringValue.of("a").asBinary(1), new byte[] { 97 });
        Assert.assertEquals(ClickHouseStringValue.of("a").asBinary(0), new byte[] { 97 });
        Assert.assertEquals(ClickHouseStringValue.of("a").asBinary(), new byte[] { 97 });

        Assert.assertEquals(ClickHouseStringValue.of(new byte[0]).toSqlExpression(), "''");
        Assert.assertEquals(ClickHouseStringValue.of(new byte[] { 97, 98, 99 }).toSqlExpression(), "unhex('616263')");
    }

    @Test(groups = { "unit" })
    public void testValue() throws UnknownHostException {
        // null value
        checkNull(ClickHouseStringValue.ofNull());
        checkNull(ClickHouseStringValue.of("abc").resetToNullOrEmpty());

        // non-null
        checkValue(ClickHouseStringValue.of(""), NumberFormatException.class, // isInfinity
                NumberFormatException.class, // isNan
                false, // isNull
                false, // boolean
                NumberFormatException.class, // byte
                NumberFormatException.class, // short
                NumberFormatException.class, // int
                NumberFormatException.class, // long
                NumberFormatException.class, // float
                NumberFormatException.class, // double
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigDecimal
                NumberFormatException.class, // BigInteger
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                "", // Object
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                ClickHouseValues.convertToIpv6("0:0:0:0:0:0:0:0"), // Inet6Address
                "", // String
                "''", // SQL Expression
                DateTimeParseException.class, // Time
                IllegalArgumentException.class, // UUID
                Object.class, // Key class
                String.class, // Value class
                new Object[] { "" }, // Array
                new String[] { "" }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { "" }), // Map
                buildMap(new Object[] { 1 }, new String[] { "" }), // typed Map
                Arrays.asList("") // Tuple
        );

        // numbers
        checkValue(ClickHouseStringValue.of("0"), false, // isInfinity
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
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                "0", // Object
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                ClickHouseValues.convertToIpv6("0.0.0.0"), // Inet6Address
                "0", // String
                "'0'", // SQL Expression
                DateTimeParseException.class, // Time
                IllegalArgumentException.class, // UUID
                Object.class, // Key class
                String.class, // Value class
                new Object[] { "0" }, // Array
                new String[] { "0" }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { "0" }), // Map
                buildMap(new Object[] { 1 }, new String[] { "0" }), // typed Map
                Arrays.asList("0") // Tuple
        );
        checkValue(ClickHouseStringValue.of("1"), false, // isInfinity
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
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                "1", // Object
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                Inet4Address.getAllByName("0.0.0.1")[0], // Inet4Address
                ClickHouseValues.convertToIpv6("0.0.0.1"), // Inet6Address
                "1", // String
                "'1'", // SQL Expression
                DateTimeParseException.class, // Time
                IllegalArgumentException.class, // UUID
                Object.class, // Key class
                String.class, // Value class
                new Object[] { "1" }, // Array
                new String[] { "1" }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { "1" }), // Map
                buildMap(new Object[] { 1 }, new String[] { "1" }), // typed Map
                Arrays.asList("1") // Tuple
        );
        checkValue(ClickHouseStringValue.of("2"), false, // isInfinity
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
                IllegalArgumentException.class, // Enum<ClickHouseDataType>
                "2", // Object
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                Inet4Address.getAllByName("0.0.0.2")[0], // Inet4Address
                ClickHouseValues.convertToIpv6("0.0.0.2"), // Inet6Address
                "2", // String
                "'2'", // SQL Expression
                DateTimeParseException.class, // Time
                IllegalArgumentException.class, // UUID
                Object.class, // Key class
                String.class, // Value class
                new Object[] { "2" }, // Array
                new String[] { "2" }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { "2" }), // Map
                buildMap(new Object[] { 1 }, new String[] { "2" }), // typed Map
                Arrays.asList("2") // Tuple
        );
        checkValue(ClickHouseStringValue.of("-1"), false, // isInfinity
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
                "-1", // Object
                DateTimeParseException.class, // Date
                DateTimeParseException.class, // DateTime
                DateTimeParseException.class, // DateTime(9)
                IllegalArgumentException.class, // Inet4Address
                IllegalArgumentException.class, // Inet6Address
                "-1", // String
                "'-1'", // SQL Expression
                DateTimeParseException.class, // Time
                IllegalArgumentException.class, // UUID
                Object.class, // Key class
                String.class, // Value class
                new Object[] { "-1" }, // Array
                new String[] { "-1" }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { "-1" }), // Map
                buildMap(new Object[] { 1 }, new String[] { "-1" }), // typed Map
                Arrays.asList("-1") // Tuple
        );
    }
}
