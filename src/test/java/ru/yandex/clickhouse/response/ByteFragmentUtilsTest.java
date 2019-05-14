package ru.yandex.clickhouse.response;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import java.sql.Date;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

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
        double[] arr= (double[]) ByteFragmentUtils.parseArray(fragment, Double.class);
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
        float[] arr= (float[]) ByteFragmentUtils.parseArray(fragment, Float.class);
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
        String[] parsedArray = (String[]) ByteFragmentUtils.parseArray(fragment, String.class);

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
        Integer[] parsedArray = (Integer[]) ByteFragmentUtils.parseArray(fragment, Integer.class, true);

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
        long[] parsedArray = (long[]) ByteFragmentUtils.parseArray(fragment, Long.class);

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
        float[] parsedArray = (float[]) ByteFragmentUtils.parseArray(fragment, Float.class);

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
        double[] parsedArray = (double[]) ByteFragmentUtils.parseArray(fragment, Double.class);

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
        Date[] parsedArray = (Date[]) ByteFragmentUtils.parseArray(fragment, Date.class, dateFormat);

        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }

    private static String joinStrings(String[] array) {
        return "[" + Joiner.on(",").join(Iterables.transform(Arrays.asList(array), new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s.replace("'", "\\'");
            }
        })) + "]";
    }

    @DataProvider(name = "doubleWithNanArraysOfArrays")
    public Object[][] doubleWithNanArrayOfArrays() {
        return new Object[][][] {
                {new String[][]{{"nan", "12.13"}, {"nan", "nan"}},
                new double[][]{{Double.NaN, 12.13d},{Double.NaN, Double.NaN}}},
                {new String[][]{{}},
                new double[][]{{}}}
        };
    }

    @Test(dataProvider = "doubleWithNanArraysOfArrays")
    public void testDoubleWithNanArrayOfArrays(String[][] source, double[][] expected) throws Exception {
        String[] arrays = new String[source.length];
        for (int i = 0; i < arrays.length; i++) {
            arrays[i] = joinStrings(source[i]);
        }
        String sourceString = joinStrings(arrays);
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);

        ClickHouseArray[] chArray = (ClickHouseArray[])
                ByteFragmentUtils.parseArrayOfArrays(fragment, Double.class, false, Types.DOUBLE, false, null);

        for (int i = 0; i < expected.length; i++) {
            double[] doubles = (double[]) chArray[i].getArray();
            assertEquals(doubles, expected[i]);
        }
    }

    @DataProvider(name = "floatWithNanArrayOfArrays")
    public Object[][] floatWithNanArrayOfArrays() {
        return new Object[][][] {
                {new String[][]{{"nan", "12.13"}, {"nan", "nan"}},
                        new float[][]{{Float.NaN, 12.13f},{Float.NaN, Float.NaN}}},
                {new String[][]{{}},
                        new float[][]{{}}}
        };
    }

    @Test(dataProvider = "floatWithNanArrayOfArrays")
    public void testFloatWithNanArrayOfArray(String[][] source, float[][] expected) throws Exception {
        String[] arrays = new String[source.length];
        for (int i = 0; i < arrays.length; i++) {
            arrays[i] = joinStrings(source[i]);
        }

        String sourceString = joinStrings(arrays);
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);

        ClickHouseArray[] chArray = (ClickHouseArray[])
                ByteFragmentUtils.parseArrayOfArrays(fragment, Float.class, false, Types.FLOAT, false, null);

        for (int i = 0; i < expected.length; i++) {
            float[] floats = (float[]) chArray[i].getArray();
            assertEquals(floats, expected[i]);
        }
    }

    @DataProvider(name = "stringArrayOfArrays")
    public Object[][] stringArrayOfArrays() {
        return new Object[][][] {
                {new String[][]{{"aa',''',//'','", "b,"}, {"бфывф"}}},
                {new String[][]{{"1234", "12.56"}, {}}},
                {new String[][]{{""}, {}}}
        };
    }

    @Test(dataProvider = "stringArrayOfArrays")
    public void testArrayOfArrays(String[][] source) throws Exception {
        String[] arrays = new String[source.length];
        for (int i = 0; i < arrays.length; i++) {
            arrays[i] = source[i].length == 0 ? "[]" :  "['" + Joiner.on("','").join(Iterables.transform(Arrays.asList(source[i]), new Function<String, String>() {
                @Override
                public String apply(String s) {
                    return s.replace("'", "\\'");
                }
            })) + "']";

        }
        String sourceString =  "[" + Joiner.on(",").join(Iterables.transform(Arrays.asList(arrays), new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s;
            }
        })) + "]";

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);

        ClickHouseArray[] chArray = (ClickHouseArray[])
                ByteFragmentUtils.parseArrayOfArrays(fragment, String.class, false, Types.VARCHAR, false, null);

        for(int i = 0; i < chArray.length; i++) {
            String[] strings = (String[]) chArray[i].getArray();
            assertEquals(strings, source[i]);
        }
    }

    @DataProvider(name = "intBoxedArrayOfArrays")
    public Object[][] intBoxedArrayOfArrays() {
        return new Object[][][] {
                {new Integer[][]{{1, 2, 3}, {100, 200, 300}}},
                {new Integer[][]{{0, -1245124312}, {}}},
                {new Integer[][]{{Integer.MIN_VALUE, Integer.MAX_VALUE}, {}}}
        };
    }

    @Test(dataProvider = "intBoxedArrayOfArrays")
    public void testIntBoxedArrayOfArrays(Integer[][] source) throws Exception {
        String[] arrays = new String[source.length];
        for (int i = 0; i < source.length; i ++) {
            arrays[i] = "[" + Joiner.on(",").join(Iterables.transform(Arrays.asList(source[i]), new Function<Integer, String>() {
                @Override
                public String apply(Integer s) {
                    return s.toString();
                }
            })) + "]";
        }

        String sourceString = joinStrings(arrays);
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);

        ClickHouseArray[] chArray = (ClickHouseArray[])
                ByteFragmentUtils.parseArrayOfArrays(fragment, Integer.class, true, Types.INTEGER, false, null);

        for (int i = 0; i < chArray.length; i++) {
            Integer[] integers = (Integer[]) chArray[i].getArray();
            assertEquals(integers, source[i]);
        }
    }

    @DataProvider(name = "longArrayOfArrays")
    public Object[][] longArrayOfArrays() {
        return new Object[][][]{
                {new long[][]{{1L, 2L}, {23L, 233L}, {-123L, -1234L}}},
                {new long[][]{{-12345678987654321L}, {23325235235L, -12321342L}}},
                {new long[][]{{}}}
        };
    }


    @Test(dataProvider = "longArrayOfArrays")
    public void testParseLongArrayOfArrays(long[][] arrayOfArrays) throws Exception {
        String[] arrays = new String[arrayOfArrays.length];
        for (int i = 0; i < arrayOfArrays.length; i++) {
            long[] array = arrayOfArrays[i];
            String sourceString = "[" + Joiner.on(",").join(Iterables.transform(Longs.asList(array), new Function<Long, String>() {
                @Override
                public String apply(Long s) {
                    return s.toString();
                }
            })) + "]";
            arrays[i] = sourceString;
        }

        String sourceString = joinStrings(arrays);

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);

        ClickHouseArray[] chArray = (ClickHouseArray[])
                ByteFragmentUtils.parseArrayOfArrays(fragment, Long.class, false, Types.BIGINT, false, null);

        for (int i = 0; i < arrayOfArrays.length; i++) {
            long[] longs = (long[]) chArray[i].getArray();
            assertEquals(longs, arrayOfArrays[i]);
        }
    }

    @DataProvider(name = "floatArrayOfArrays")
    public Object[][] floatArrayOfArrays() {
        return new Object[][][]{
                {new float[][]{{1.1F, 2.222F}, {0F, 111F}, {-123F, -1234F}}},
                {new float[][]{{-12345678987654321.14343F}, {Float.MIN_VALUE, Float.MAX_VALUE}}},
                {new float[][]{{0F}, {}}},
                {new float[][]{{}}}
        };
    }

    @Test(dataProvider = "floatArrayOfArrays")
    public void testFloatArrayOfArrays(float[][] source) throws Exception {
        String[] arrays = new String[source.length];
        for (int i = 0; i < arrays.length; i++) {
            arrays[i] = "[" + Joiner.on(",").join(Iterables.transform(Floats.asList(source[i]), new Function<Float, String>() {
                @Override
                public String apply(Float s) {
                    return s.toString();
                }
            })) + "]";
        }
        String sourceString = joinStrings(arrays);

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);

        ClickHouseArray[] chArray = (ClickHouseArray[])
                ByteFragmentUtils.parseArrayOfArrays(fragment, Float.class, false, Types.FLOAT, false, null);
        for (int i = 0; i < chArray.length; i++) {
            float[] floats = (float[]) chArray[i].getArray();
            assertEquals(floats, source[i]);
        }
    }

    @DataProvider(name = "doubleArrayOfArrays")
    public Object[][] doubleArrayOfArrays() {
        return new Object[][][]{
                {new double[][]{{1.1d, 2.222d}, {0d, 111d}, {-123d, -1234d}}},
                {new double[][]{{-12345678987654321.14343d}, {Double.MIN_VALUE, Double.MAX_VALUE}}},
                {new double[][]{{0d}, {}}},
                {new double[][]{{}}}
        };
    }

    @Test(dataProvider = "doubleArrayOfArrays")
    public void testFloatArrayOfArrays(double[][] source) throws Exception {
        String[] arrays = new String[source.length];
        for (int i = 0; i < arrays.length; i++) {
            arrays[i] = "[" + Joiner.on(",").join(Iterables.transform(Doubles.asList(source[i]), new Function<Double, String>() {
                @Override
                public String apply(Double s) {
                    return s.toString();
                }
            })) + "]";
        }
        String sourceString = joinStrings(arrays);

        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);

        ClickHouseArray[] chArray = (ClickHouseArray[])
                ByteFragmentUtils.parseArrayOfArrays(fragment, Double.class, false, Types.DOUBLE, false, null);
        for (int i = 0; i < chArray.length; i++) {
            double[] doubles = (double[]) chArray[i].getArray();
            assertEquals(doubles, source[i]);
        }
    }

    @DataProvider(name = "dateArrayOfArrays")
    public Object[][] dateArrayOfArrays() {
        return new Object[][][] {
                {new Date[][]{{new Date(0L)}, {}}},
                {new Date[][]{{}, {new Date(1606780800000L)}}},
                {new Date[][]{{}}}
        };
    }

    @Test(dataProvider = "dateArrayOfArrays")
    public void testDateArrayOfArrays(Date[][] source) throws Exception {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String[] arrays = new String[source.length];
        for (int i = 0; i < source.length; i++) {
            arrays[i] = "[" + Joiner.on(",").join(Iterables.transform(Arrays.asList(source[i]), new Function<Date, String>() {
                @Override
                public String apply(Date s) {
                   return dateFormat.format(s);
             }
            })) + "]";
        }
        String sourceString = joinStrings(arrays);
        byte[] bytes = sourceString.getBytes(StreamUtils.UTF_8);
        ByteFragment fragment = new ByteFragment(bytes, 0, bytes.length);

        ClickHouseArray[] chArray = (ClickHouseArray[])
                ByteFragmentUtils.parseArrayOfArrays(fragment, Date.class, false, Types.DATE, false, dateFormat);

        for (int i = 0; i < chArray.length; i++) {
            Date[] dates = (Date[]) chArray[i].getArray();
            assertEquals(dates, source[i]);
        }
    }
}