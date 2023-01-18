package com.clickhouse.data;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.testng.Assert;

public abstract class BaseClickHouseValueTest {
    protected <K, V> Map<K, V> buildMap(K[] keys, V[] values) {
        Map<K, V> map = new LinkedHashMap<>();
        int len = keys == null ? 0 : keys.length;
        if (values == null || values.length != len) {
            throw new IllegalArgumentException("Keys and values should have same length");
        }
        for (int i = 0; i < len; i++) {
            map.put(keys[i], values[i]);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    protected void checkValueOrException(Supplier<?> actual, Object expected) {
        if (expected == null) {
            Assert.assertNull(actual.get());
        } else if (expected instanceof Class && Throwable.class.isAssignableFrom((Class<?>) expected)) {
            Assert.assertThrows((Class<Throwable>) expected, () -> actual.get());
        } else if (expected instanceof String) {
            Assert.assertEquals(String.valueOf(actual.get()), (String) expected, String.class.getName());
        } else if (expected instanceof List) {
            List<?> l1 = (List<?>) actual.get();
            List<?> l2 = (List<?>) expected;
            // unfortunately TestNG 6.x didn't work
            Assert.assertTrue(Arrays.deepEquals(l1.toArray(new Object[l1.size()]), l2.toArray(new Object[l2.size()])),
                    List.class.getName());
            // Assert.assertEquals(l1.toArray(new Object[l1.size()]), l2.toArray(new
            // Object[l2.size()]));
        } else if (expected instanceof Map) {
            Assert.assertEqualsDeep((Map<?, ?>) actual.get(), (Map<?, ?>) expected, Map.class.getName());
        } else {
            Assert.assertEquals(actual.get(), expected, expected == null ? null : expected.getClass().getName());
        }
    }

    protected void checkNull(ClickHouseValue v) {
        checkNull(v, true, 3, 9);
    }

    protected void checkNull(ClickHouseValue v, boolean nullable, int bigDecimalScale, int dateTimeScale) {
        Assert.assertFalse(v.isInfinity());
        Assert.assertFalse(v.isNaN());
        Assert.assertTrue(v.isNullOrEmpty());

        Assert.assertEquals(v.asBoolean(), false);
        Assert.assertEquals(v.asCharacter(), '\0');
        Assert.assertEquals(v.asByte(), (byte) 0);
        Assert.assertEquals(v.asShort(), (short) 0);
        Assert.assertEquals(v.asInteger(), 0);
        Assert.assertEquals(v.asLong(), 0L);
        Assert.assertEquals(v.asFloat(), 0F);
        Assert.assertEquals(v.asDouble(), 0D);

        Assert.assertNull(v.asBigDecimal());
        Assert.assertNull(v.asBigDecimal(bigDecimalScale));
        Assert.assertNull(v.asBigInteger());
        Assert.assertNull(v.asEnum(ClickHouseDataType.class));
        Assert.assertNull(v.asDate());
        Assert.assertNull(v.asDateTime());
        Assert.assertNull(v.asTime());
        Assert.assertNull(v.asDateTime(dateTimeScale));
        Assert.assertNull(v.asInet4Address());
        Assert.assertNull(v.asInet6Address());
        Assert.assertNull(v.asUuid());
        if (nullable) {
            Assert.assertNull(v.asObject());
            Assert.assertNull(v.asObject(Object.class));
            Assert.assertNull(v.asString());
            Assert.assertEquals(v.toSqlExpression(), ClickHouseValues.NULL_EXPR);
        } else {
            Assert.assertNotNull(v.asObject());
            Assert.assertNotNull(v.asObject(Object.class));
            Assert.assertNotNull(v.asString());
            Assert.assertNotNull(v.toSqlExpression());
            Assert.assertNotEquals(v.toSqlExpression(), ClickHouseValues.NULL_EXPR);
        }

        Assert.assertEquals(v.asArray(), new Object[0]);
        Assert.assertEquals(v.asArray(ClickHouseDataType.class), new Object[0]);
        Assert.assertNotNull(v.asMap());
        Assert.assertEquals(v.asMap().size(), 0);
        Assert.assertEquals(v.asMap(Integer.class, ClickHouseDataType.class).size(), 0);
        Assert.assertEquals(v.asTuple(), Collections.emptyList());
    }

    protected void checkValue(ClickHouseValue v, Object... expected) {
        checkValue(v, 3, 9, expected);
    }

    protected void checkValue(ClickHouseValue v, int bigDecimalScale, int dateTimeScale, Object... expected) {
        int i = 0;
        checkValueOrException(v::isInfinity, expected[i++]);
        checkValueOrException(v::isNaN, expected[i++]);
        checkValueOrException(v::isNullOrEmpty, expected[i++]);

        checkValueOrException(v::asBoolean, expected[i++]);
        checkValueOrException(v::asByte, expected[i++]);
        checkValueOrException(v::asShort, expected[i++]);
        checkValueOrException(v::asInteger, expected[i++]);
        checkValueOrException(v::asLong, expected[i++]);
        Object nanValue = expected[i++];
        if (nanValue != null) { // skip NaN
            checkValueOrException(v::asFloat, nanValue);
        }
        nanValue = expected[i++];
        if (nanValue != null) { // skip NaN
            checkValueOrException(v::asDouble, nanValue);
        }

        checkValueOrException(v::asBigDecimal, expected[i++]);
        checkValueOrException(() -> v.asBigDecimal(bigDecimalScale), expected[i++]);
        checkValueOrException(v::asBigInteger, expected[i++]);
        checkValueOrException(() -> v.asEnum(ClickHouseDataType.class), expected[i++]);
        checkValueOrException(v::asObject, expected[i++]);
        checkValueOrException(v::asDate, expected[i++]);
        checkValueOrException(v::asDateTime, expected[i++]);
        checkValueOrException(() -> v.asDateTime(dateTimeScale), expected[i++]);
        checkValueOrException(v::asInet4Address, expected[i++]);
        checkValueOrException(v::asInet6Address, expected[i++]);
        checkValueOrException(v::asString, expected[i++]);
        checkValueOrException(v::toSqlExpression, expected[i++]);
        checkValueOrException(v::asTime, expected[i++]);
        checkValueOrException(v::asUuid, expected[i++]);

        Class<?> keyClass = (Class<?>) expected[i++];
        Class<?> valueClass = (Class<?>) expected[i++];
        checkValueOrException(v::asArray, expected[i++]);
        checkValueOrException(() -> v.asArray(valueClass), expected[i++]);
        checkValueOrException(v::asMap, expected[i++]);
        checkValueOrException(() -> v.asMap(keyClass, valueClass), expected[i++]);
        checkValueOrException(v::asTuple, expected[i++]);
    }

    protected Object getReturnValue(Supplier<?> func) {
        try {
            return func.get();
        } catch (Throwable t) {
            return t.getClass().getName() + ':' + t.getMessage();
        }
    }

    protected <T extends Enum<T>> void sameValue(ClickHouseValue v1, ClickHouseValue v2, int bigDecimalScale,
            int dateTimeScale, Class<?> arrayType, Class<T> enumType, Class<?> keyType, Class<?> valueType) {
        // you're supposed to pass two different values for comparison
        // Assert.assertFalse(v1 == v2);

        Assert.assertEquals(getReturnValue(v1::isInfinity), getReturnValue(v2::isInfinity));
        Assert.assertEquals(getReturnValue(v1::isNaN), getReturnValue(v2::isNaN));
        Assert.assertEquals(getReturnValue(v1::isNullOrEmpty), getReturnValue(v2::isNullOrEmpty));

        Assert.assertEquals(getReturnValue(v1::asBoolean), getReturnValue(v2::asBoolean));
        Assert.assertEquals(getReturnValue(v1::asCharacter), getReturnValue(v2::asCharacter));
        Assert.assertEquals(getReturnValue(v1::asByte), getReturnValue(v2::asByte));
        Assert.assertEquals(getReturnValue(v1::asShort), getReturnValue(v2::asShort));
        Assert.assertEquals(getReturnValue(v1::asInteger), getReturnValue(v2::asInteger));
        Assert.assertEquals(getReturnValue(v1::asLong), getReturnValue(v2::asLong));
        Assert.assertEquals(getReturnValue(v1::asFloat), getReturnValue(v2::asFloat));
        Assert.assertEquals(getReturnValue(v1::asDouble), getReturnValue(v2::asDouble));
        Assert.assertEquals(getReturnValue(v1::asBigDecimal), getReturnValue(v2::asBigDecimal));
        Assert.assertEquals(getReturnValue(() -> v1.asBigDecimal(bigDecimalScale)),
                getReturnValue(() -> v2.asBigDecimal(bigDecimalScale)));
        Assert.assertEquals(getReturnValue(v1::asBigInteger), getReturnValue(v2::asBigInteger));

        Assert.assertEquals(getReturnValue(v1::asDate), getReturnValue(v2::asDate));
        Assert.assertEquals(getReturnValue(v1::asDateTime), getReturnValue(v2::asDateTime));
        Assert.assertEquals(getReturnValue(() -> v1.asDateTime(dateTimeScale)),
                getReturnValue(() -> v2.asDateTime(dateTimeScale)));
        Assert.assertEquals(getReturnValue(v1::asTime), getReturnValue(v2::asTime));
        Assert.assertEquals(getReturnValue(() -> v1.asEnum(enumType)), getReturnValue(() -> v2.asEnum(enumType)));
        Assert.assertEquals(getReturnValue(v1::asInet4Address), getReturnValue(v2::asInet4Address));
        Assert.assertEquals(getReturnValue(v1::asInet6Address), getReturnValue(v2::asInet6Address));
        Assert.assertEquals(getReturnValue(v1::asString), getReturnValue(v2::asString));
        Assert.assertEquals(getReturnValue(v1::toSqlExpression), getReturnValue(v2::toSqlExpression));
        Assert.assertEquals(getReturnValue(v1::asUuid), getReturnValue(v2::asUuid));

        Assert.assertEquals(getReturnValue(v1::asArray), getReturnValue(v2::asArray));
        Assert.assertEquals(getReturnValue(() -> v1.asArray(arrayType)), getReturnValue(() -> v2.asArray(arrayType)));
        Assert.assertEquals(getReturnValue(v1::asMap), getReturnValue(v2::asMap));
        Assert.assertEquals(getReturnValue(() -> v1.asMap(keyType, valueType)),
                getReturnValue(() -> v2.asMap(keyType, valueType)));
        Assert.assertEquals(getReturnValue(v1::asTuple), getReturnValue(v2::asTuple));
    }
}
