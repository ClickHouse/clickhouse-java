package com.clickhouse.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * Wraper class of enum.
 */
public class ClickHouseEnumValue<T extends Enum<T>> extends ClickHouseObjectValue<T> {
    static final String ERROR_NO_ENUM_TYPE = "Failed to convert value due to lack of enum type: ";

    /**
     * Create a new instance representing null value.
     *
     * @param <T> enum type
     * @return new instance representing null value
     */
    public static <T extends Enum<T>> ClickHouseEnumValue<T> ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param <T> enum type
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    @SuppressWarnings("unchecked")
    public static <T extends Enum<T>> ClickHouseEnumValue<T> ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseEnumValue ? (ClickHouseEnumValue<T>) ((ClickHouseEnumValue<T>) ref).set(null)
                : new ClickHouseEnumValue<>(null);
    }

    /**
     * Wrap the given value.
     *
     * @param <T>   enum type
     * @param value value
     * @return object representing the value
     */
    public static <T extends Enum<T>> ClickHouseEnumValue<T> of(T value) {
        return of(null, value);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param <T>   enum type
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    @SuppressWarnings("unchecked")
    public static <T extends Enum<T>> ClickHouseEnumValue<T> of(ClickHouseValue ref, T value) {
        return ref instanceof ClickHouseEnumValue
                ? (ClickHouseEnumValue<T>) ((ClickHouseEnumValue<T>) ref).update(value)
                : new ClickHouseEnumValue<>(value);
    }

    protected ClickHouseEnumValue(T value) {
        super(value);
    }

    @Override
    public ClickHouseEnumValue<T> copy(boolean deep) {
        return new ClickHouseEnumValue<>(getValue());
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : (byte) getValue().ordinal();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : (short) getValue().ordinal();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : getValue().ordinal();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().ordinal();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().ordinal());
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : getValue().ordinal();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : getValue().ordinal();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : BigDecimal.valueOf(getValue().ordinal(), scale);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends Enum<E>> E asEnum(Class<E> enumType) {
        return (E) getValue();
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    @Override
    public String asString(int length, Charset charset) {
        if (isNullOrEmpty()) {
            return null;
        }

        String str = String.valueOf(getValue().name());
        if (length > 0) {
            ClickHouseChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public String toSqlExpression() {
        return ClickHouseValues.convertToQuotedString(asString(0, null));
    }

    @Override
    public ClickHouseEnumValue<T> update(boolean value) {
        return update(value ? 1 : 0);
    }

    @Override
    public ClickHouseEnumValue<T> update(char value) {
        return update((int) value);
    }

    @Override
    public ClickHouseEnumValue<T> update(byte value) {
        return update((int) value);
    }

    @Override
    public ClickHouseEnumValue<T> update(short value) {
        return update((int) value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseEnumValue<T> update(int value) {
        if (isNullOrEmpty()) {
            throw new IllegalArgumentException(ERROR_NO_ENUM_TYPE + value);
        }

        Class<T> clazz = (Class<T>) getValue().getClass();
        for (T t : clazz.getEnumConstants()) {
            if (t.ordinal() == value) {
                return update(t);
            }
        }

        throw new IllegalArgumentException();
    }

    @Override
    public ClickHouseEnumValue<T> update(long value) {
        return update((int) value);
    }

    @Override
    public ClickHouseEnumValue<T> update(float value) {
        return update((int) value);
    }

    @Override
    public ClickHouseEnumValue<T> update(double value) {
        return update((int) value);
    }

    @Override
    public ClickHouseEnumValue<T> update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
            return this;
        }

        return update(value.intValueExact());
    }

    @Override
    public ClickHouseEnumValue<T> update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
            return this;
        }

        return update(value.intValueExact());
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseEnumValue<T> update(Enum<?> value) {
        set((T) value);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseEnumValue<T> update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else if (value instanceof ClickHouseEnumValue) {
            set(((ClickHouseEnumValue<T>) value).getValue());
        } else if (isNullOrEmpty()) {
            throw new IllegalArgumentException(ERROR_NO_ENUM_TYPE + value);
        } else {
            set(value.asEnum(isNullOrEmpty() ? null : (Class<T>) getValue().getClass()));
        }
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseEnumValue<T> update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (isNullOrEmpty()) {
            throw new IllegalArgumentException(ERROR_NO_ENUM_TYPE + value);
        } else {
            set((T) Enum.valueOf(getValue().getClass(), value));
        }

        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseEnumValue<T> update(Object value) {
        if (value instanceof Enum) {
            set((T) value);
            return this;
        } else if (value instanceof ClickHouseEnumValue) {
            set(((ClickHouseEnumValue<T>) value).getValue());
            return this;
        }

        super.update(value);
        return this;
    }
}
