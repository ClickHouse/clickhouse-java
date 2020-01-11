package ru.yandex.clickhouse.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;

import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseArrayUtilTest {

    @Test
    public void testArrayToString() throws Exception {
        assertEquals(
            ClickHouseArrayUtil.arrayToString(new String[]{"a", "b"}),
            "['a','b']"
        );

        assertEquals(
            ClickHouseArrayUtil.arrayToString(new String[]{"a", "'b\t"}),
            "['a','\\'b\\t']"
        );

        assertEquals(
                ClickHouseArrayUtil.arrayToString(new String[]{"\\xEF\\xBC", "\\x3C\\x22"}), // quote == true
                "['\\\\xEF\\\\xBC','\\\\x3C\\\\x22']"
        );

        assertEquals(
            ClickHouseArrayUtil.arrayToString(new Integer[]{21, 42}),
            "[21,42]"
        );

        assertEquals(
            ClickHouseArrayUtil.arrayToString(new int[]{21, 42}),
            "[21,42]"
        );

        assertEquals(
                ClickHouseArrayUtil.arrayToString(new double[]{0.1, 1.2}),
                "[0.1,1.2]"
        );

        assertEquals(
            ClickHouseArrayUtil.arrayToString(new char[]{'a', 'b'}),
            "['a','b']"
        );

        assertEquals(
            ClickHouseArrayUtil.arrayToString(new String[][]{{"a", "b"},{"c", "d"}}),
            "[['a','b'],['c','d']]"
        );

        assertEquals(
            ClickHouseArrayUtil.arrayToString(new String[][]{{"a", "'b\t"},{"c", "'d\t"}}),
            "[['a','\\'b\\t'],['c','\\'d\\t']]"
        );

        assertEquals(
            ClickHouseArrayUtil.arrayToString(new Integer[][]{{21, 42},{63, 84}}),
            "[[21,42],[63,84]]"
        );

        assertEquals(
            ClickHouseArrayUtil.arrayToString(new double[][]{{0.1, 1.2}, {0.2, 2.2}}),
            "[[0.1,1.2],[0.2,2.2]]"
        );

        assertEquals(
            ClickHouseArrayUtil.arrayToString(new int[][]{{1, 2}, {3, 4}}),
            "[[1,2],[3,4]]"
        );

        assertEquals(
            ClickHouseArrayUtil.arrayToString(new char[][]{{'a', 'b'}, {'c', 'd'}}),
            "[['a','b'],['c','d']]"
        );

        assertEquals(
            ClickHouseArrayUtil.arrayToString(new byte[][]{{'a', 'b'}, {'c', 'd'}}),
            "['\\x61\\x62','\\x63\\x64']"
        );

    }

    @Test
    public void testCollectionToString() throws Exception {
        assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList("a", "b"))),
                "['a','b']"
        );

        assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList("a", "'b\t"))),
                "['a','\\'b\\t']"
        );

        assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList("\\xEF\\xBC", "\\x3C\\x22"))), // quote == true
                "['\\\\xEF\\\\xBC','\\\\x3C\\\\x22']"
        );

        assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList(21, 42))),
                "[21,42]"
        );

        assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList(21, 42))),
                "[21,42]"
        );

        assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList(0.1, 1.2))),
                "[0.1,1.2]"
        );

        assertEquals(
                ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList('a', 'b'))),
                "['a','b']"
        );

        ArrayList<Object> arrayOfArrays = new ArrayList<>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(1, 2)));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(3, 4)));
        assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays),
            "[[1,2],[3,4]]"
        );

        arrayOfArrays = new ArrayList<>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(1.1, 2.4)));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(3.9, 4.16)));
        assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays),
            "[[1.1,2.4],[3.9,4.16]]"
        );

        arrayOfArrays = new ArrayList<>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList("a", "b")));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList("c", "'d\t")));
        assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays),
            "[['a','b'],['c','\\'d\\t']]"
        );

        arrayOfArrays = new ArrayList<>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList('a', 'b')));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList('c', 'd')));
        assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays),
            "[['a','b'],['c','d']]"
        );

        assertEquals(
            ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList(21, null))),
            "[21,NULL]"
        );

        assertEquals(
            ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList(null, 42))),
            "[NULL,42]"
        );

        assertEquals(
            ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList("a", null))),
            "['a',NULL]"
        );

        assertEquals(
            ClickHouseArrayUtil.toString(new ArrayList<Object>(Arrays.asList(null, "b"))),
            "[NULL,'b']"
        );

        arrayOfArrays = new ArrayList<>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(null, 'b')));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList('c', 'd')));
        assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays),
            "[[NULL,'b'],['c','d']]"
        );

        arrayOfArrays = new ArrayList<>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(null, 'b')));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList('c', null)));
        assertEquals(
            ClickHouseArrayUtil.toString(arrayOfArrays),
            "[[NULL,'b'],['c',NULL]]"
        );

        List<byte[]> listOfByteArrays = new ArrayList<>();
        listOfByteArrays.add("foo".getBytes("UTF-8"));
        listOfByteArrays.add("bar".getBytes("UTF-8"));
        assertEquals(
            ClickHouseArrayUtil.toString(listOfByteArrays),
            "['\\x66\\x6F\\x6F','\\x62\\x61\\x72']"
        );
    }

    @Test
    public void testArrayDateTimeDefaultTimeZone() {
        Timestamp ts0 = new Timestamp(1557136800000L);
        Timestamp ts1 = new Timestamp(1560698526598L);
        Timestamp[] timestamps = new Timestamp[] { ts0, null, ts1 };
        String formatted = ClickHouseArrayUtil.arrayToString(timestamps);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        assertEquals(
            formatted,
            "['" + sdf.format(ts0) + "',NULL,'" + sdf.format(ts1) + "']");
    }

    @Test
    public void testArrayDateTimeOtherTimeZone() {
        TimeZone tzTokyo = TimeZone.getTimeZone("Asia/Tokyo");
        Timestamp ts0 = new Timestamp(1557136800000L);
        Timestamp ts1 = new Timestamp(1560698526598L);
        Timestamp[] timestamps = new Timestamp[] { ts0, null, ts1 };
        String formatted = ClickHouseArrayUtil.arrayToString(
            timestamps, tzTokyo, tzTokyo);
        assertEquals(
            formatted,
            "['2019-05-06 19:00:00',NULL,'2019-06-17 00:22:06']");
    }


    @Test(dataProvider = "doubleArrayWithNan")
    public void testDoubleNan(String[] source, double[] expected) throws Exception
    {
        String sourceString = source.length == 0 ? "[]" : "['" + Joiner.on("','").join(Iterables.transform(Arrays.asList(source), new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s.replace("'", "\\'");
            }
        })) + "']";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        double[] arr= (double[]) ClickHouseArrayUtil.parseArray(fragment, Double.class, false,
            getStringArrayColumnInfo(1));
        assertEquals(arr, expected);
    }

    @Test(dataProvider = "floatArrayWithNan")
    public void testFloatNan(String[] source, float[] expected) throws Exception
    {
        String sourceString = source.length == 0 ? "[]" : "['" + Joiner.on("','").join(Iterables.transform(Arrays.asList(source), new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s.replace("'", "\\'");
            }
        })) + "']";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        float[] arr= (float[]) ClickHouseArrayUtil.parseArray(fragment, Float.class, false,
            getStringArrayColumnInfo(1));
        assertEquals(arr, expected);
    }

    @Test(dataProvider = "stringArray")
    public void testParseArray(String[] array) throws Exception {
        String sourceString = array.length == 0 ? "[]" : "['" + Joiner.on("','").join(Iterables.transform(Arrays.asList(array), new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s.replace("'", "\\'");
            }
        })) + "']";

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        String[] parsedArray = (String[]) ClickHouseArrayUtil.parseArray(fragment, String.class, false,
            getStringArrayColumnInfo(1));

        assertNotNull(parsedArray);
        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }

    @Test(dataProvider = "intBoxedArray")
    public void testParseBoxedArray(Integer[] array) throws Exception {
        String sourceString = "[" + Joiner.on(",").join(Iterables.transform(Arrays.asList(array), new Function<Integer, String>() {
            @Override
            public String apply(Integer s) {
                return s.toString();
            }
        })) + "]";

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        Integer[] parsedArray = (Integer[]) ClickHouseArrayUtil.parseArray(fragment, Integer.class, true,
            getStringArrayColumnInfo(1));

        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }

    @Test(dataProvider = "longArray")
    public void testParseArray(long[] array) throws Exception {
        String sourceString = "[" + Joiner.on(",").join(Iterables.transform(Longs.asList(array), new Function<Long, String>() {
            @Override
            public String apply(Long s) {
                return s.toString();
            }
        })) + "]";

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        long[] parsedArray = (long[]) ClickHouseArrayUtil.parseArray(fragment, Long.class, false,
            getStringArrayColumnInfo(1));

        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }

    @Test(dataProvider = "floatArray")
    public void testParseArray(float[] array) throws Exception {
        String sourceString = "[" + Joiner.on(",").join(Iterables.transform(Floats.asList(array), new Function<Float, String>() {
            @Override
            public String apply(Float s) {
                return s.toString();
            }
        })) + "]";

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        float[] parsedArray = (float[]) ClickHouseArrayUtil.parseArray(fragment, Float.class, false,
            getStringArrayColumnInfo(1));

        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }

    @Test(dataProvider = "doubleArray")
    public void testParseArray(double[] array) throws Exception {
        String sourceString = "[" + Joiner.on(",").join(Iterables.transform(Doubles.asList(array), new Function<Double, String>() {
            @Override
            public String apply(Double s) {
                return s.toString();
            }
        })) + "]";

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        double[] parsedArray = (double[]) ClickHouseArrayUtil.parseArray(fragment, Double.class, false,
            getStringArrayColumnInfo(1));

        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }

    @Test(dataProvider = "booleanArray")
    public void testParseArray(String[] input, boolean[] array) throws Exception {
        String sourceString = "[" + String.join(",", input) + "]";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        boolean[] parsedArray = (boolean[]) ClickHouseArrayUtil.parseArray(
            fragment, Boolean.class, false, getStringArrayColumnInfo(1));
        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }


    @Test(dataProvider = "dateArray")
    public void testParseArray(Date[] array) throws Exception {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String sourceString = "[" + Joiner.on(",").join(Iterables.transform(Arrays.asList(array), new Function<Date, String>() {
            @Override
            public String apply(Date s) {
                return dateFormat.format(s);
            }
        })) + "]";

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        Date[] parsedArray = (Date[]) ClickHouseArrayUtil.parseArray(fragment, true, dateFormat.getTimeZone(),
            ClickHouseColumnInfo.parse("Array(Date)", "myDate"));

        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }

    @Test(dataProvider = "decimalArray")
    public void testParseArray(BigDecimal[] array) throws Exception {
        String sourceString = "[" + Joiner.on(",").join(Iterables.transform(Arrays.asList(array), new Function<BigDecimal, String>() {

            @Override
            public String apply(BigDecimal s) {
                return s.toPlainString();
            }
        })) + "]";

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        BigDecimal[] parsedArray = (BigDecimal[]) ClickHouseArrayUtil.parseArray(fragment, BigDecimal.class, true,
            getStringArrayColumnInfo(1));

        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }

    @Test
    public void testParseArrayThreeLevels() throws Exception {
        int[][][] expected  =  {{{10,11,12},{13,14,15}},{{20,21,22},{23,24,25}},{{30,31,32},{33,34,35}}};
        String sourceString = "[[[10,11,12],[13,14,15]],[[20,21,22],[23,24,25]],[[30,31,32],[33,34,35]]]";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        int[][][] actual = (int[][][]) ClickHouseArrayUtil.parseArray(fragment, Integer.class, false,
            getStringArrayColumnInfo(3));
        assertEquals(actual, expected);
    }

    @Test
    public void testParseArrayTwoLevelsEmpty() throws Exception {
        String sourceString = "[[]]";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        String[][] actual = (String[][]) ClickHouseArrayUtil.parseArray(fragment, String.class, true,
            getStringArrayColumnInfo(2));
        assertEquals(1, actual.length);
        assertEquals(0, actual[0].length);
    }

    @Test
    public void testParseSparseArray() throws Exception {
        String sourceString = "[[],[NULL],['a','b',NULL]]";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        String[][] actual = (String[][]) ClickHouseArrayUtil.parseArray(fragment, String.class, true,
            getStringArrayColumnInfo(2));
        assertEquals(3, actual.length);
        assertEquals(0, actual[0].length);
        assertEquals(1, actual[1].length);
        assertEquals(3, actual[2].length);
        assertNull(actual[1][0]);
        assertEquals("a", actual[2][0]);
        assertEquals("b", actual[2][1]);
        assertNull(actual[2][2]);
    }

    @Test
    public void testParseArrayOf32Levels() throws Exception {
        String sourceString = "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[32]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        int[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][] actual =
            (int[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][])
            ClickHouseArrayUtil.parseArray(fragment, Integer.class, false, getStringArrayColumnInfo(32));

        assertEquals(actual[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0], 32);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Maximum parse depth exceeded")
    public void testParseArrayMaximumDepthExceeded() throws SQLException {
        String sourceString = "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[33]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        ClickHouseArrayUtil.parseArray(fragment, Integer.class, false, getStringArrayColumnInfo(33));
    }


    @Test(
            dataProvider = "invalidArray",
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "not an array.*"
    )
    public void testParseInvalidArray(String sourceString, int arrayLevel) throws Exception {
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        ClickHouseArrayUtil.parseArray(fragment, String.class, true,
            getStringArrayColumnInfo(arrayLevel));
    }

    @DataProvider(name = "invalidArray")
    private static Object[][] invalidArray() {
        return new Object[][] {
                {"['a']", 2}, // wrong level
                {"[", 1},
                {"[]]", 2},
                {"[['a'],'b']", 2} // arrays of different levels
        };
    }

    @DataProvider(name = "stringArray")
    private static Object[][] stringArray() {
        return new Object[][]{
                {new String[]{"a'aa", "a,,',,a"}},
                {new String[]{"a'','sadf',aa", "", ",", "юникод,'юникод'", ",2134,saldfk"}},
                {new String[]{"", ""}},
                {new String[]{""}},
                {new String[]{}}
        };
    }

    @DataProvider(name = "intBoxedArray")
    private static Object[][] intBoxedArray() {
        return new Object[][]{
                {new Integer[]{1, 23, -123}},
                {new Integer[]{-87654321, 233252355, -12321342}},
                {new Integer[]{}}
        };
    }

    @DataProvider(name = "longArray")
    private static Object[][] longArray() {
        return new Object[][]{
                {new long[]{1L, 23L, -123L}},
                {new long[]{-12345678987654321L, 23325235235L, -12321342L}},
                {new long[]{}}
        };
    }

    @DataProvider(name = "decimalArray")
    private static Object[][] decimalArray() {
        return new Object[][]{
                {new BigDecimal[]{BigDecimal.ONE, BigDecimal.valueOf(23L), BigDecimal.valueOf(-123L)}},
                {new BigDecimal[]{BigDecimal.valueOf(-12345678987654321L), BigDecimal.valueOf(23325235235L), BigDecimal.valueOf(-12321342L)}},
                {new BigDecimal[]{}}
        };
    }

    @DataProvider(name = "floatArray")
    private static Object[][] floatArray() {
        return new Object[][]{
                {new float[]{1F, 23F, -123F}},
                {new float[]{-123123123.123123F, 2332.12334234234F, -12321342F}},
                {new float[]{}}
        };
    }

    @DataProvider(name = "doubleArray")
    private static Object[][] doubleArray() {
        return new Object[][]{
                {new double[]{1, 23, -123}},
                {new double[]{-123123123.123123, 2332.12334234234, -12321342}},
                {new double[]{}}
        };
    }

    @DataProvider(name = "dateArray")
    private static Object[][] dateArray() {
        return new Object[][]{
                {new Date[]{new Date(0L)}},
                {new Date[]{new Date(1263945600000L), new Date(1606780800000L)}},
                {new Date[]{}}
        };
    }

    @DataProvider(name = "doubleArrayWithNan")
    private static Object[][] doubleArrayWithNan() {
        return new Object[][]{
                { new String[]{ "nan", "23.45" }, new double[]{Double.NaN, 23.45}},
                { new String[]{}, new double[]{}}
        };
    }

    @DataProvider(name = "floatArrayWithNan")
    private static Object[][] floatArrayWithNan() {
        return new Object[][]{
                { new String[]{ "nan", "23.45" }, new float[]{Float.NaN, 23.45F}},
                { new String[]{}, new float[]{}}
        };
    }

    @DataProvider(name = "booleanArray")
    private static Object[][] booleanArrayWithNan() {
        return new Object[][]{
                { new String[]{ "1", "0" }, new boolean[]{ true, false }},
                { new String[]{ "1", "\\N", "1", "0"}, new boolean[]{ true, false, true, false }}
        };
    }

    private static ClickHouseColumnInfo getStringArrayColumnInfo(int level) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < level; i++) {
            sb.append("Array(");
        }
        sb.append("String");
        for (int i = 0; i < level; i++) {
            sb.append(")");
        }
        return ClickHouseColumnInfo.parse(sb.toString(), "columnName");
    }

}
