package com.clickhouse.client.api.data_formats.internal;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.net.URL;
import java.util.Map;
import java.util.function.Function;

public class ValueConvertersTests {

    private final ValueConverters converters = new ValueConverters();

    @Test
    public void testConvertNumberToBigDecimalSupportsGenericNumberValues() {
        Assert.assertEquals(converters.convertNumberToBigDecimal(new CustomNumber("1234567890.123456789")),
                new BigDecimal("1234567890.123456789"));
    }

    @Test(dataProvider = "integerNumberValues")
    public void testConvertNumberToBigDecimalSupportsIntegerNumberValues(Number value, BigDecimal expected) {
        Assert.assertEquals(converters.convertNumberToBigDecimal(value), expected);
    }

    @Test(dataProvider = "floatingNumberValues")
    public void testConvertNumberToBigDecimalSupportsFloatingNumberValues(Number value, BigDecimal expected) {
        Assert.assertEquals(converters.convertNumberToBigDecimal(value), expected);
    }

    @Test
    public void testNumberToBigDecimalConverterIsRegisteredForNumericSourceTypes() {
        Map<Class<?>, Function<Object, Object>> longConverters = converters.getConvertersForType(Long.class);
        Map<Class<?>, Function<Object, Object>> doubleConverters = converters.getConvertersForType(Double.class);

        Assert.assertEquals(longConverters.get(BigDecimal.class).apply(123L), BigDecimal.valueOf(123L));
        Assert.assertEquals(doubleConverters.get(BigDecimal.class).apply(12.5d), BigDecimal.valueOf(12.5d));
    }

    @Test
    public void testGetConvertersForStringTypeIncludesCommonTargets() throws Exception {
        Map<Class<?>, Function<Object, Object>> stringConverters = converters.getConvertersForType(String.class);

        Assert.assertEquals(stringConverters.get(Integer.class).apply("42"), 42);
        Assert.assertEquals(stringConverters.get(URL.class).apply("https://clickhouse.com"),
                new URL("https://clickhouse.com"));
        Assert.assertEquals(stringConverters.get(byte[].class).apply("abc"), "abc".getBytes());
    }

    @Test
    public void testGetConvertersForBooleanTypeIncludesNumericAndStringTargets() {
        Map<Class<?>, Function<Object, Object>> booleanConverters = converters.getConvertersForType(Boolean.class);

        Assert.assertEquals(booleanConverters.get(Long.class).apply(Boolean.TRUE), 1L);
        Assert.assertEquals(booleanConverters.get(String.class).apply(Boolean.FALSE), "false");
        Assert.assertEquals(((Number) booleanConverters.get(BigDecimal.class).apply(Boolean.TRUE)).longValue(), 1L);
    }

    @Test
    public void testGetConvertersForUnknownTypeReturnsEmptyMap() {
        Assert.assertTrue(converters.getConvertersForType(Void.class).isEmpty());
    }

    @DataProvider
    public Object[][] integerNumberValues() {
        return new Object[][]{
                {(byte) 7, BigDecimal.valueOf(7)},
                {(short) 12, BigDecimal.valueOf(12)},
                {34, BigDecimal.valueOf(34)},
                {56L, BigDecimal.valueOf(56L)}
        };
    }

    @DataProvider
    public Object[][] floatingNumberValues() {
        return new Object[][]{
                {12.5f, BigDecimal.valueOf(12.5d)},
                {98.765d, BigDecimal.valueOf(98.765d)}
        };
    }

    private static final class CustomNumber extends Number {
        private final BigDecimal value;

        private CustomNumber(String value) {
            this.value = new BigDecimal(value);
        }

        @Override
        public int intValue() {
            return value.intValue();
        }

        @Override
        public long longValue() {
            return value.longValue();
        }

        @Override
        public float floatValue() {
            return value.floatValue();
        }

        @Override
        public double doubleValue() {
            return value.doubleValue();
        }

        @Override
        public String toString() {
            return value.toPlainString();
        }
    }
}
