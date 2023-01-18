package com.clickhouse.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseValuesTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testCreateArray() {
        Class<?>[] primitiveTypes = new Class<?>[] { boolean.class, byte.class, char.class, short.class,
                int.class,
                long.class, float.class, double.class };
        Class<?>[] wrapperTypes = new Class<?>[] { Boolean.class, Byte.class, Character.class, Short.class,
                Integer.class, Long.class, Float.class, Double.class };
        Class<?>[] miscTypes = new Class<?>[] { Object.class, Map.class, Collection.class, Class.class };
        for (Class<?> c : primitiveTypes) {
            Assert.assertEquals(ClickHouseValues.createObjectArray(c, 0, 1),
                    ClickHouseValues.EMPTY_OBJECT_ARRAY);
        }
        for (Class<?> c : wrapperTypes) {
            Assert.assertEquals(ClickHouseValues.createObjectArray(c, 0, 1),
                    ClickHouseValues.EMPTY_OBJECT_ARRAY);
        }
        for (Class<?> c : miscTypes) {
            Assert.assertEquals(ClickHouseValues.createObjectArray(c, 0, 1),
                    ClickHouseValues.EMPTY_OBJECT_ARRAY);
        }

        Object[] expectedValues = new Object[] { ClickHouseValues.EMPTY_BYTE_ARRAY,
                ClickHouseValues.EMPTY_BYTE_ARRAY,
                ClickHouseValues.EMPTY_INT_ARRAY, ClickHouseValues.EMPTY_SHORT_ARRAY,
                ClickHouseValues.EMPTY_INT_ARRAY,
                ClickHouseValues.EMPTY_LONG_ARRAY, ClickHouseValues.EMPTY_FLOAT_ARRAY,
                ClickHouseValues.EMPTY_DOUBLE_ARRAY };
        int index = 0;
        for (Class<?> c : primitiveTypes) {
            Assert.assertEquals(ClickHouseValues.createPrimitiveArray(c, 0, 1), expectedValues[index++]);
        }
        index = 0;
        for (Class<?> c : wrapperTypes) {
            Assert.assertEquals(ClickHouseValues.createPrimitiveArray(c, 0, 1), expectedValues[index++]);
        }
        index = 0;
        for (Class<?> c : miscTypes) {
            Assert.assertEquals(ClickHouseValues.createPrimitiveArray(c, 0, 1),
                    ClickHouseValues.EMPTY_OBJECT_ARRAY);
        }

        int[][] intArray = (int[][]) ClickHouseValues.createPrimitiveArray(int.class, 3, 2);
        Assert.assertEquals(intArray, new int[][] { new int[0], new int[0], new int[0] });
    }

    @Test(groups = { "unit" })
    public void testConvertToDateTime() {
        Assert.assertEquals(ClickHouseValues.convertToDateTime(null), null);
        Assert.assertEquals(ClickHouseValues.convertToDateTime(BigDecimal.valueOf(0L)),
                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC));
        Assert.assertEquals(ClickHouseValues.convertToDateTime(BigDecimal.valueOf(1L)),
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC));
        for (int i = 1; i < 9; i++) {
            BigDecimal d = BigDecimal.TEN.pow(i);
            Assert.assertEquals(
                    ClickHouseValues.convertToDateTime(
                            BigDecimal.valueOf(1L).add(BigDecimal.valueOf(1L).divide(d))),
                    LocalDateTime.ofEpochSecond(1L, BigDecimal.TEN.pow(9 - i).intValue(),
                            ZoneOffset.UTC));
        }

        Assert.assertEquals(ClickHouseValues.convertToDateTime(BigDecimal.valueOf(-1L)),
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC));
        for (int i = 1; i < 9; i++) {
            BigDecimal d = BigDecimal.TEN.pow(i);
            Assert.assertEquals(
                    ClickHouseValues.convertToDateTime(
                            BigDecimal.valueOf(-1L).add(BigDecimal.valueOf(-1L).divide(d))),
                    LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC)
                            .minus(BigDecimal.TEN.pow(9 - i).longValue(),
                                    ChronoUnit.NANOS));
        }
    }

    @Test(groups = { "unit" })
    public void testConvertToIpv4() throws UnknownHostException {
        Assert.assertEquals(ClickHouseValues.convertToIpv4((String) null), null);
        Assert.assertEquals(ClickHouseValues.convertToIpv4("127.0.0.1"), Inet4Address.getByName("127.0.0.1"));
        Assert.assertEquals(ClickHouseValues.convertToIpv4("0:0:0:0:0:ffff:7f00:1"),
                Inet4Address.getByName("127.0.0.1"));
    }

    @Test(groups = { "unit" })
    public void testConvertToIpv6() throws UnknownHostException {
        Assert.assertEquals(ClickHouseValues.convertToIpv6((String) null), null);
        Assert.assertEquals(ClickHouseValues.convertToIpv6("::1"), Inet6Address.getByName("::1"));
        Assert.assertEquals(ClickHouseValues.convertToIpv6("127.0.0.1").getClass(), Inet6Address.class);
        Assert.assertEquals(ClickHouseValues.convertToIpv6("127.0.0.1").getAddress(),
                new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xFF, (byte) 0xFF, 0x7F, 0, 0, 1 });
    }

    @Test(groups = { "unit" })
    public void testConvertToSqlExpression() throws UnknownHostException {
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(null), ClickHouseValues.NULL_EXPR);

        // primitives
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(true), String.valueOf(true));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(false), String.valueOf(false));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression('\0'), String.valueOf(0));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression('a'), String.valueOf(97));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Byte.MAX_VALUE),
                String.valueOf(Byte.MAX_VALUE));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Byte.MIN_VALUE),
                String.valueOf(Byte.MIN_VALUE));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Short.MAX_VALUE),
                String.valueOf(Short.MAX_VALUE));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Short.MIN_VALUE),
                String.valueOf(Short.MIN_VALUE));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Integer.MAX_VALUE),
                String.valueOf(Integer.MAX_VALUE));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Integer.MIN_VALUE),
                String.valueOf(Integer.MIN_VALUE));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Long.MAX_VALUE),
                String.valueOf(Long.MAX_VALUE));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Long.MIN_VALUE),
                String.valueOf(Long.MIN_VALUE));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Float.MAX_VALUE),
                String.valueOf(Float.MAX_VALUE));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Float.MIN_VALUE),
                String.valueOf(Float.MIN_VALUE));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Double.MAX_VALUE),
                String.valueOf(Double.MAX_VALUE));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Double.MIN_VALUE),
                String.valueOf(Double.MIN_VALUE));

        // stringlike types
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(""), String.valueOf("''"));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression("萌萌哒"), String.valueOf("'萌萌哒'"));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression("'萌\\'\\\'萌哒'"),
                String.valueOf("'\\'萌\\\\\\'\\\\\\'萌哒\\''"));
        Assert.assertEquals(
                ClickHouseValues.convertToSqlExpression(
                        UUID.fromString("00000000-0000-0000-0000-000000000000")),
                "'00000000-0000-0000-0000-000000000000'");
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(InetAddress.getByName("localhost")),
                "'127.0.0.1'");
        Assert.assertEquals(
                ClickHouseValues.convertToSqlExpression(
                        (Inet4Address) InetAddress.getAllByName("127.0.0.1")[0]),
                "'127.0.0.1'");
        Assert.assertEquals(
                ClickHouseValues.convertToSqlExpression(
                        (Inet6Address) InetAddress.getAllByName("::1")[0]),
                "'0:0:0:0:0:0:0:1'");

        // enum, big integer and decimals
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(ClickHouseDataType.DateTime64),
                String.valueOf(ClickHouseDataType.DateTime64.ordinal()));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(BigInteger.valueOf(1L)), String.valueOf(1));
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(BigDecimal.valueOf(123456789.123456789D)),
                "123456789.12345679");

        // date, time and date time
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(LocalDate.of(2021, 11, 12)),
                "'2021-11-12'");
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(LocalTime.of(11, 12, 13, 123456789)),
                "'11:12:13.123456789'");
        Assert.assertEquals(
                ClickHouseValues.convertToSqlExpression(
                        LocalDateTime.of(LocalDate.of(2021, 11, 12),
                                LocalTime.of(11, 12, 13, 123456789))),
                "'2021-11-12 11:12:13.123456789'");

        // arrays
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(new Object[0]), "[]");
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(new boolean[] { true, false, true }),
                "[true,false,true]");
        Assert.assertEquals(
                ClickHouseValues.convertToSqlExpression(new boolean[][] { new boolean[] { true, false, true } }),
                "[[true,false,true]]");
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(new char[] { 'a', '\0', '囧' }),
                "[97,0,22247]");
        Assert.assertEquals(
                ClickHouseValues.convertToSqlExpression(
                        new byte[] { (byte) 11, (byte) -12, (byte) 127 }),
                "[11,-12,127]");
        Assert.assertEquals(
                ClickHouseValues.convertToSqlExpression(
                        new short[] { (short) 11, (short) -22247, (short) 25534 }),
                "[11,-22247,25534]");
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(new int[] { 233, -122247, 165535 }),
                "[233,-122247,165535]");
        Assert.assertEquals(
                ClickHouseValues.convertToSqlExpression(new long[] { 233333333L, -122247L, 165535L }),
                "[233333333,-122247,165535]");
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(new float[] { 2.33F, -1.22247F, 165535F }),
                "[2.33,-1.22247,165535.0]");
        Assert.assertEquals(
                ClickHouseValues.convertToSqlExpression(new double[] { 2.33333D, -1.22247D, 165535D }),
                "[2.33333,-1.22247,165535.0]");

        // tuple
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(new ArrayList<>(0)), "()");
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(Arrays.asList('a', true, "'x'", 233)),
                "(97,true,'\\'x\\'',233)");

        // map
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(new HashMap<>()), "{}");
        Assert.assertEquals(ClickHouseValues.convertToSqlExpression(
                buildMap(new Integer[] { 2, 3 }, new String[] { "two", "three" })),
                "{2 : 'two',3 : 'three'}");

        // mixed
        Assert.assertEquals(
                ClickHouseValues.convertToSqlExpression(new Object[] { true, 'a', (byte) 1, (short) 2,
                        3, 4L, 5.555F,
                        6.666666D, "'x'",
                        UUID.fromString("00000000-0000-0000-0000-000000000002"),
                        InetAddress.getByName("127.0.0.1"), InetAddress.getByName("::1"),
                        ClickHouseDataType.Decimal256,
                        BigInteger.valueOf(123456789L), BigDecimal.valueOf(1.23456789D), null,
                        LocalDate.of(2021, 11, 12), LocalTime.of(11, 12, 13, 123456789),
                        LocalDateTime.of(LocalDate.of(2021, 11, 12),
                                LocalTime.of(11, 12, 13, 123456789)),
                        new boolean[] { false, true } }),
                "[true,97,1,2,3,4,5.555,6.666666,'\\'x\\'','00000000-0000-0000-0000-000000000002','127.0.0.1','0:0:0:0:0:0:0:1',"
                        + ClickHouseDataType.Decimal256.ordinal()
                        + ",123456789,1.23456789,NULL,'2021-11-12','11:12:13.123456789','2021-11-12 11:12:13.123456789',[false,true]]");
    }
}
