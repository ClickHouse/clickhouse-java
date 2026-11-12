package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.StringValue;
import com.clickhouse.data.value.ClickHouseObjectValue;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Converts values returned by readers and generated values to a common form, so they can be compared with
 * {@link #deepEquals}: arrays and lists become lists, internal value holders their values, decimals lose trailing
 * zeros.
 */
final class ReaderValues {

    private static final int MAX_DESCRIPTION_LENGTH = 200;

    private ReaderValues() {
    }

    static Object normalize(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof BinaryStreamReader.ArrayValue) {
            return normalize(((BinaryStreamReader.ArrayValue) value).asList());
        } else if (value instanceof BinaryStreamReader.EnumValue) {
            return ((BinaryStreamReader.EnumValue) value).getName();
        } else if (value instanceof StringValue) {
            return ((StringValue) value).asString();
        } else if (value instanceof ClickHouseObjectValue) {
            return normalize(((ClickHouseObjectValue<?>) value).getValue());
        } else if (value instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value;
            return decimal.signum() == 0 ? BigDecimal.ZERO : decimal.stripTrailingZeros();
        } else if (value instanceof List) {
            List<Object> list = new ArrayList<>();
            for (Object element : (List<?>) value) {
                list.add(normalize(element));
            }
            return list;
        } else if (value.getClass().isArray()) {
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < Array.getLength(value); i++) {
                list.add(normalize(Array.get(value, i)));
            }
            return list;
        } else if (value instanceof Map) {
            Map<Object, Object> map = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                map.put(normalize(entry.getKey()), normalize(entry.getValue()));
            }
            return map;
        }
        return value;
    }

    static boolean deepEquals(Object expected, Object actual) {
        return Objects.equals(expected, actual);
    }

    static String describe(Object value) {
        Object normalized = normalize(value);
        String text = normalized == null ? "null"
                : normalized.getClass().getSimpleName() + " " + normalized;
        return text.length() > MAX_DESCRIPTION_LENGTH ? text.substring(0, MAX_DESCRIPTION_LENGTH) + "..." : text;
    }
}
