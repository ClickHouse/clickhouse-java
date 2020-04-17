package ru.yandex.clickhouse.response;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * @author Aleksandr Kormushin <kormushin@yandex-team.ru>
 */
public class ByteFragmentUtilsTest {

    @DataProvider(name = "stringArray")
    public Object[][] stringArray() {
        return new Object[][]{
                {new String[]{"a'aa", "a,,',,a"}},
                {new String[]{"a'','sadf',aa", "", ",", "юникод,'юникод'", ",2134,saldfk"}},
                {new String[]{"", ""}},
                {new String[]{""}},
                {new String[]{}}
        };
    }

    @DataProvider(name = "uuidArray")
    public Object[][] uuidArray() {
        return new Object[][]{
                {new UUID[]{UUID.fromString("00000000-0000-0000-0000-000000000000"), UUID.fromString("00000000-0000-0000-0000-000000000000")}},
                {new UUID[]{UUID.fromString("00000000-0000-0000-0000-000000000000")}},
                {new UUID[]{}}
        };
    }

    @DataProvider(name = "intBoxedArray")
    public Object[][] intBoxedArray() {
        return new Object[][]{
                {new Integer[]{1, 23, -123}},
                {new Integer[]{-87654321, 233252355, -12321342}},
                {new Integer[]{}}
        };
    }

    @DataProvider(name = "longArray")
    public Object[][] longArray() {
        return new Object[][]{
                {new long[]{1L, 23L, -123L}},
                {new long[]{-12345678987654321L, 23325235235L, -12321342L}},
                {new long[]{}}
        };
    }

    @DataProvider(name = "decimalArray")
    public Object[][] decimalArray() {
        return new Object[][]{
                {new BigDecimal[]{BigDecimal.ONE, BigDecimal.valueOf(23L), BigDecimal.valueOf(-123L)}},
                {new BigDecimal[]{BigDecimal.valueOf(-12345678987654321L), BigDecimal.valueOf(23325235235L), BigDecimal.valueOf(-12321342L)}},
                {new BigDecimal[]{}}
        };
    }

    @DataProvider(name = "floatArray")
    public Object[][] floatArray() {
        return new Object[][]{
                {new float[]{1F, 23F, -123F}},
                {new float[]{-123123123.123123F, 2332.12334234234F, -12321342F}},
                {new float[]{}}
        };
    }

    @DataProvider(name = "doubleArray")
    public Object[][] doubleArray() {
        return new Object[][]{
                {new double[]{1, 23, -123}},
                {new double[]{-123123123.123123, 2332.12334234234, -12321342}},
                {new double[]{}}
        };
    }

    @DataProvider(name = "dateArray")
    public Object[][] dateArray() {
        return new Object[][]{
                {new Date[]{new Date(0L)}},
                {new Date[]{new Date(1263945600000L), new Date(1606780800000L)}},
                {new Date[]{}}
        };
    }

    @DataProvider(name = "doubleArrayWithNan")
    public Object[][] doubleArrayWithNan() {
        return new Object[][]{
                { new String[]{ "nan", "23.45" }, new double[]{Double.NaN, 23.45}},
                { new String[]{}, new double[]{}}
        };
    }

    @DataProvider(name = "floatArrayWithNan")
    public Object[][] floatArrayWithNan() {
        return new Object[][]{
                { new String[]{ "nan", "23.45" }, new float[]{Float.NaN, 23.45F}},
                { new String[]{}, new float[]{}}
        };
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
        double[] arr= (double[]) ByteFragmentUtils.parseArray(fragment, Double.class, 1);
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
        float[] arr= (float[]) ByteFragmentUtils.parseArray(fragment, Float.class, 1);
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
        String[] parsedArray = (String[]) ByteFragmentUtils.parseArray(fragment, String.class, 1);

        assertNotNull(parsedArray);
        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }

    @Test(dataProvider = "uuidArray")
    public void testParseUuidArray(UUID[] array) throws Exception {
        String sourceString = array.length == 0 ? "[]" : "['" + Joiner.on("','").join(Iterables.transform(Arrays.asList(array), new Function<UUID, String>() {
            @Override
            public String apply(UUID s) {
                return s.toString();
            }
        })) + "']";

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        UUID[] parsedArray = (UUID[]) ByteFragmentUtils.parseArray(fragment, UUID.class, 1);

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
        Integer[] parsedArray = (Integer[]) ByteFragmentUtils.parseArray(fragment, Integer.class, true, 1);

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
        long[] parsedArray = (long[]) ByteFragmentUtils.parseArray(fragment, Long.class, 1);

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
        float[] parsedArray = (float[]) ByteFragmentUtils.parseArray(fragment, Float.class, 1);

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
        double[] parsedArray = (double[]) ByteFragmentUtils.parseArray(fragment, Double.class, 1);

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
        Date[] parsedArray = (Date[]) ByteFragmentUtils.parseArray(fragment, Date.class, dateFormat, 1);

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
        BigDecimal[] parsedArray = (BigDecimal[]) ByteFragmentUtils.parseArray(fragment, BigDecimal.class, 1);

        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }

    @Test
    public void testParseArrayThreeLevels() {
        int[][][] expected  =  {{{10,11,12},{13,14,15}},{{20,21,22},{23,24,25}},{{30,31,32},{33,34,35}}};
        String sourceString = "[[[10,11,12],[13,14,15]],[[20,21,22],[23,24,25]],[[30,31,32],[33,34,35]]]";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        int[][][] actual = (int[][][]) ByteFragmentUtils.parseArray(fragment, Integer.class, 3);
        assertEquals(expected, actual);
    }

    @Test
    public void testParseArrayTwoLevelsEmpty() {
        String sourceString = "[[]]";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        String[][] actual = (String[][]) ByteFragmentUtils.parseArray(fragment, String.class, 2);
        assertEquals(1, actual.length);
        assertEquals(0, actual[0].length);
    }

    @Test
    public void testParseSparseArray() {
        String sourceString = "[[],[NULL],['a','b',NULL]]";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        String[][] actual = (String[][]) ByteFragmentUtils.parseArray(fragment, String.class, 2);
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
    public void testParseArrayOf32Levels() {
        String sourceString = "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[32]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        int[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][] actual =
                (int[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][])
                        ByteFragmentUtils.parseArray(fragment, Integer.class, 32);
        assertEquals(32, actual[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Maximum parse depth exceeded")
    public void testParseArrayMaximumDepthExceeded() {
        String sourceString = "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[33]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]";
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        ByteFragmentUtils.parseArray(fragment, Integer.class, 33);
    }

    @DataProvider(name = "invalidArray")
    public Object[][] invalidArray() {
        return new Object[][] {
                {"['a']", 2}, // wrong level
                {"[", 1},
                {"[]]", 2},
                {"[['a'],'b']", 2} // arrays of different levels
        };
    }

    @Test(
            dataProvider = "invalidArray",
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "not an array.*"
    )
    public void testParseInvalidArray(String sourceString, int arrayLevel) {
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);
        ByteFragmentUtils.parseArray(fragment, String.class, arrayLevel);
    }
}
