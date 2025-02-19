package com.clickhouse.data.value.array;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import com.clickhouse.data.ClickHouseArraySequence;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.value.ClickHouseObjectValue;
import com.clickhouse.data.value.UnsignedByte;

/**
 * Wrapper of {@code byte[]}.
 */
@Deprecated
public class ClickHouseByteArrayValue extends ClickHouseObjectValue<byte[]> implements ClickHouseArraySequence {
    static final class UnsignedByteArrayValue extends ClickHouseByteArrayValue {
        @Override
        public Object[] asArray() {
            byte[] v = getValue();
            int len = v.length;
            UnsignedByte[] array = new UnsignedByte[len];
            for (int i = 0; i < len; i++) {
                array[i] = UnsignedByte.valueOf(v[i]);
            }
            return array;
        }

        @Override
        public String asString() {
            byte[] v = getValue();
            int len = v.length;
            if (len == 0) {
                return ClickHouseValues.EMPTY_ARRAY_EXPR;
            }
            StringBuilder builder = new StringBuilder().append('[').append(UnsignedByte.toString(v[0]));
            for (int i = 1; i < len; i++) {
                builder.append(',').append(UnsignedByte.toString(v[i]));
            }
            return builder.append(']').toString();
        }

        @Override
        public ClickHouseByteArrayValue copy(boolean deep) {
            if (!deep) {
                return new UnsignedByteArrayValue(getValue());
            }

            byte[] value = getValue();
            return new UnsignedByteArrayValue(Arrays.copyOf(value, value.length));
        }

        @Override
        public ClickHouseByteArrayValue update(String value) {
            if (ClickHouseChecker.isNullOrBlank(value)) {
                set(ClickHouseValues.EMPTY_BYTE_ARRAY);
            } else {
                List<String> list = ClickHouseUtils.readValueArray(value, 0, value.length());
                if (list.isEmpty()) {
                    set(ClickHouseValues.EMPTY_BYTE_ARRAY);
                } else {
                    byte[] arr = new byte[list.size()];
                    int index = 0;
                    for (String v : list) {
                        arr[index++] = v == null ? (byte) 0 : UnsignedByte.valueOf(v).byteValue();
                    }
                    set(arr);
                }
            }
            return this;
        }

        protected UnsignedByteArrayValue(byte[] value) {
            super(value);
        }
    }

    private static final String TYPE_NAME = "byte[]";

    /**
     * Creates a new instance representing empty {@code Int8} array.
     *
     * @return new instance representing an empty array
     */
    public static ClickHouseByteArrayValue ofEmpty() {
        return of(null, ClickHouseValues.EMPTY_BYTE_ARRAY, false);
    }

    /**
     * Creates a new instance representing empty {@code UInt8} array.
     *
     * @return new instance representing an empty array
     */
    public static ClickHouseByteArrayValue ofUnsignedEmpty() {
        return of(null, ClickHouseValues.EMPTY_BYTE_ARRAY, true);
    }

    /**
     * Wraps the given {@code Int8} array.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseByteArrayValue of(byte[] value) {
        return of(null, value, false);
    }

    /**
     * Wraps the given {@code UInt8} array.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseByteArrayValue ofUnsigned(byte[] value) {
        return of(null, value, true);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref      object to update, could be null
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return same object as {@code ref} or a new instance if it's null
     */

    public static ClickHouseByteArrayValue of(ClickHouseValue ref, byte[] value, boolean unsigned) {
        if (unsigned) {
            return ref instanceof UnsignedByteArrayValue ? ((UnsignedByteArrayValue) ref).set(value)
                    : new UnsignedByteArrayValue(value);
        }

        return ref instanceof ClickHouseByteArrayValue ? ((ClickHouseByteArrayValue) ref).set(value)
                : new ClickHouseByteArrayValue(value);
    }

    protected ClickHouseByteArrayValue(byte[] value) {
        super(value);
    }

    @Override
    protected final ClickHouseByteArrayValue set(byte[] value) {
        super.set(ClickHouseChecker.nonNull(value, ClickHouseValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        byte[] v = getValue();
        int len = v.length;
        Byte[] array = new Byte[len];
        for (int i = 0; i < len; i++) {
            array[i] = Byte.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        byte[] v = getValue();
        int len = v.length;
        if (clazz == Boolean.class) {
            Boolean[] array = new Boolean[len];
            for (int i = 0; i < len; i++) {
                array[i] = v[i] == (byte) 1 ? Boolean.TRUE : Boolean.FALSE;
            }
            return (E[]) array;
        } else {
            E[] array = ClickHouseValues.createObjectArray(clazz, len, 1);
            for (int i = 0; i < len; i++) {
                array[i] = clazz.cast(v[i]);
            }
            return array;
        }
    }

    @Override
    public <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Non-null key and value classes are required");
        }
        byte[] v = getValue();
        Map<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < v.length; i++) {
            map.put(keyClass.cast(i + 1), valueClass.cast(v[i]));
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString() {
        byte[] value = getValue();
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return ClickHouseValues.EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[').append(value[0]);
        for (int i = 1; i < len; i++) {
            builder.append(',').append(value[i]);
        }
        return builder.append(']').toString();
    }

    @Override
    public ClickHouseByteArrayValue copy(boolean deep) {
        if (!deep) {
            return new ClickHouseByteArrayValue(getValue());
        }

        byte[] value = getValue();
        return new ClickHouseByteArrayValue(Arrays.copyOf(value, value.length));
    }

    @Override
    public final boolean isNullable() {
        return false;
    }

    @Override
    public final boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override
    public ClickHouseByteArrayValue resetToDefault() {
        set(ClickHouseValues.EMPTY_BYTE_ARRAY);
        return this;
    }

    @Override
    public ClickHouseByteArrayValue resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        return asString();
    }

    @Override
    public ClickHouseByteArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] ? (byte) 1 : (byte) 0;
        }
        return set(v);
    }

    @Override
    public ClickHouseByteArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseByteArrayValue update(byte value) {
        return set(new byte[] { value });
    }

    @Override
    public ClickHouseByteArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(value);
    }

    @Override
    public ClickHouseByteArrayValue update(short value) {
        return set(new byte[] { (byte) value });
    }

    @Override
    public ClickHouseByteArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseByteArrayValue update(int value) {
        return set(new byte[] { (byte) value });
    }

    @Override
    public ClickHouseByteArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseByteArrayValue update(long value) {
        return set(new byte[] { (byte) value });
    }

    @Override
    public ClickHouseByteArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseByteArrayValue update(float value) {
        return set(new byte[] { (byte) value });
    }

    @Override
    public ClickHouseByteArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseByteArrayValue update(double value) {
        return set(new byte[] { (byte) value });
    }

    @Override
    public ClickHouseByteArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseByteArrayValue update(BigInteger value) {
        return set(value == null ? ClickHouseValues.EMPTY_BYTE_ARRAY : new byte[] { value.byteValue() });
    }

    @Override
    public ClickHouseByteArrayValue update(BigDecimal value) {
        return set(value == null ? ClickHouseValues.EMPTY_BYTE_ARRAY : new byte[] { value.byteValue() });
    }

    @Override
    public ClickHouseByteArrayValue update(Enum<?> value) {
        return set(value == null ? ClickHouseValues.EMPTY_BYTE_ARRAY : new byte[] { (byte) value.ordinal() });
    }

    @Override
    public ClickHouseByteArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        // return set(value == null ? ClickHouseValues.EMPTY_BYTE_ARRAY :
        // value.getAddress());
        throw newUnsupportedException(ClickHouseValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ClickHouseByteArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        // return set(value == null ? ClickHouseValues.EMPTY_BYTE_ARRAY :
        // value.getAddress());
        throw newUnsupportedException(ClickHouseValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ClickHouseByteArrayValue update(LocalDate value) {
        return set(value == null ? ClickHouseValues.EMPTY_BYTE_ARRAY : new byte[] { (byte) value.toEpochDay() });
    }

    @Override
    public ClickHouseByteArrayValue update(LocalTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_BYTE_ARRAY : new byte[] { (byte) value.toSecondOfDay() });
    }

    @Override
    public ClickHouseByteArrayValue update(LocalDateTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_BYTE_ARRAY
                : new byte[] { (byte) value.toEpochSecond(ZoneOffset.UTC) });
    }

    @Override
    public ClickHouseByteArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o == null ? 0 : ((Number) o).byteValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseByteArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        byte[] values = new byte[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n == null ? 0 : n.byteValue();
        }
        return set(values);
    }

    @Override
    public ClickHouseByteArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o == null ? 0 : ((Number) e.getValue()).byteValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseByteArrayValue update(String value) {
        if (ClickHouseChecker.isNullOrBlank(value)) {
            set(ClickHouseValues.EMPTY_BYTE_ARRAY);
        } else {
            List<String> list = ClickHouseUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ClickHouseValues.EMPTY_BYTE_ARRAY);
            } else {
                byte[] arr = new byte[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = v == null ? (byte) 0 : Byte.parseByte(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ClickHouseByteArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        // ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        // buffer.putLong(value.getMostSignificantBits());
        // buffer.putLong(value.getLeastSignificantBits());
        // return set(buffer.array());
        throw newUnsupportedException(ClickHouseValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ClickHouseByteArrayValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ClickHouseByteArrayValue) {
            set(((ClickHouseByteArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ClickHouseByteArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else if (value instanceof Boolean[]) {
            byte[] values = new byte[len];
            for (int i = 0; i < len; i++) {
                values[i] = Boolean.TRUE.equals(value[i]) ? (byte) 1 : (byte) 0;
            }
            set(values);
        } else {
            byte[] values = new byte[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o == null ? 0 : ((Number) o).byteValue();
            }
            set(values);
        }

        return this;
    }

    @Override
    public ClickHouseValue updateUnknown(Object value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (value instanceof Number) {
            return set(new byte[] { ((Number) value).byteValue() });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ClickHouseByteArrayValue update(Object value) {
        if (value instanceof byte[]) {
            set((byte[]) value);
        } else {
            super.update(value);
        }
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return Arrays.equals(getValue(), ((ClickHouseByteArrayValue) obj).getValue());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getValue());
    }

    @Override
    public ClickHouseArraySequence allocate(int length, Class<?> clazz, int level) {
        if (length < 1) {
            resetToDefault();
        } else {
            set(new byte[length]);
        }
        return this;
    }

    @Override
    public int length() {
        return isNullOrEmpty() ? 0 : getValue().length;
    }

    @Override
    public <V extends ClickHouseValue> V getValue(int index, V value) {
        value.update(getValue()[index]);
        return value;
    }

    @Override
    public ClickHouseArraySequence setValue(int index, ClickHouseValue value) {
        getValue()[index] = value.asByte();
        return this;
    }
}
