package com.clickhouse.client.api.data_formats.internal;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;

public class NumberConverterTests {

    @Test
    public void testToBigDecimalSupportsGenericNumberValues() {
        Assert.assertEquals(NumberConverter.toBigDecimal(new CustomNumber("1234567890.123456789")),
                new BigDecimal("1234567890.123456789"));
    }

    @Test
    public void testToBigDecimalSupportsStringValues() {
        Assert.assertEquals(NumberConverter.toBigDecimal("98765.4321"), new BigDecimal("98765.4321"));
    }

    @Test
    public void testToBigDecimalPreservesFractionalFloatBoundaries() {
        Assert.assertEquals(NumberConverter.toBigDecimal(0.0001f).compareTo(new BigDecimal("0.0001")), 0);
        Assert.assertEquals(NumberConverter.toBigDecimal(0.0256f).compareTo(new BigDecimal("0.0256")), 0);
        Assert.assertEquals(NumberConverter.toBigDecimal(6.5536f).compareTo(new BigDecimal("6.5536")), 0);
        Assert.assertEquals(NumberConverter.toBigDecimal(838.8608f).compareTo(new BigDecimal("838.8608")), 0);
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
