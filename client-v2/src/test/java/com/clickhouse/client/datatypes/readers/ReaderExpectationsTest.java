package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.query.NullValueException;
import com.clickhouse.client.datatypes.readers.ReaderExpectations.Expected;
import com.clickhouse.client.testdata.DataTypeGenerator;
import com.clickhouse.client.testdata.GenerateTestDataSet;
import com.clickhouse.data.ClickHouseColumn;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;

import static com.clickhouse.client.datatypes.readers.ReaderAccessor.*;

public class ReaderExpectationsTest {

    @DataProvider
    public Object[][] expectations() throws Exception {
        return new Object[][] {
                {"Int16", (short) 100, BYTE, (byte) 100},
                {"Int16", (short) 200, BYTE, ClientException.class},
                {"UInt64", BigInteger.ONE.shiftLeft(63), LONG, ClientException.class},
                {"Float64", 2.0, INTEGER, 2},
                {"Float64", 1.5, INTEGER, ClientException.class},
                {"Float64", Double.NaN, BIG_DECIMAL, ClientException.class},
                {"Float64", 0.1, BIG_DECIMAL, new BigDecimal("0.1")},
                {"Float64", 1e300, FLOAT, ClientException.class},
                {"Float64", 1e300, DOUBLE, 1e300},
                {"Float32", Float.NaN, FLOAT, Float.NaN},
                {"Decimal(9, 2)", new BigDecimal("3.00"), BIG_INTEGER, BigInteger.valueOf(3)},
                {"Int8", (byte) 1, BOOLEAN, true},
                {"Int8", (byte) 2, BOOLEAN, ClientException.class},
                {"String", "12", INTEGER, 12},
                {"String", "a", INTEGER, ClientException.class},
                {"String", "true", BOOLEAN, true},
                {"String", "a\tb", STRING, "a\tb"},
                {"String", "a", UUID, ClientException.class},
                {"Enum8", "max", BYTE, (byte) 127},
                {"Enum16", "max", ENUM8, ClientException.class},
                {"Time", Duration.ofHours(1), LOCAL_TIME, LocalTime.of(1, 0)},
                {"Time", Duration.ofHours(25), LOCAL_TIME, ClientException.class},
                {"Time", Duration.ofSeconds(-1), LOCAL_TIME, ClientException.class},
                {"IPv6", InetAddress.getByName("::ffff:1.2.3.4"), INET4_ADDRESS, InetAddress.getByName("1.2.3.4")},
                {"IPv6", InetAddress.getByName("::1"), INET4_ADDRESS, ClientException.class},
                {"Nullable(Int32)", null, INTEGER, NullValueException.class},
                {"Nullable(Int32)", null, BIG_INTEGER, null},
                {"Nullable(Int32)", null, UUID, ClientException.class},
                {"Array(Int32)", Arrays.asList(1, 2), INT_ARRAY, new int[] {1, 2}},
                {"Array(Int32)", Arrays.asList(1, 2), LONG_ARRAY, ClientException.class},
                {"Array(Nullable(String))", Arrays.asList("a", null), STRING_ARRAY, new String[] {"a", null}},
                {"Array(String)", Arrays.asList("a\t"), STRING, "['a\\t']"},
                {"Tuple(Int32, String)", Arrays.asList(1, "a"), TUPLE, new Object[] {1, "a"}},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "expectations")
    public void testExpected(String type, Object value, ReaderAccessor accessor, Object expected) {
        DataTypeGenerator generator = GenerateTestDataSet.parseType(type);
        ReaderExpectations expectations = new ReaderExpectations(ClickHouseColumn.of("value", generator.getType()),
                generator);
        Expected actual = expectations.expected(accessor, value);
        if (expected instanceof Class) {
            Assert.assertEquals(actual.getException(), expected, actual.toString());
        } else {
            Assert.assertFalse(actual.isException(), actual.toString());
            Assert.assertTrue(actual.matches(expected), actual + " does not match " + expected);
        }
    }

    @Test(groups = {"unit"})
    public void testUnescape() {
        Assert.assertEquals(ReaderExpectations.unescape("a\\tb\\nc\\\\d\\'e\\0"), "a\tb\nc\\d'e\0");
    }
}
