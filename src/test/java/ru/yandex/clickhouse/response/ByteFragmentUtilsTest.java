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
        BigDecimal[] parsedArray = (BigDecimal[]) ByteFragmentUtils.parseArray(fragment, BigDecimal.class);

        assertEquals(parsedArray.length, array.length);
        for (int i = 0; i < parsedArray.length; i++) {
            assertEquals(parsedArray[i], array[i]);
        }
    }
}
