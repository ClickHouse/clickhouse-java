package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import com.clickhouse.data.ClickHouseEnum;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@code enum}.
 */
@Deprecated
public class ClickHouseEnumValue implements ClickHouseValue {
    /**
     * Create a new instance representing null value.
     *
     * @param clazz enum class
     * @return new instance representing null value
     */
    public static ClickHouseEnumValue ofNull(Class<? extends Enum> clazz) {
        return ofNull(null, ClickHouseEnum.of(clazz));
    }

    /**
     * Create a new instance representing null value.
     *
     * @param type enum type, null is same as {@link ClickHouseEnum#EMPTY}
     * @return new instance representing null value
     */
    public static ClickHouseEnumValue ofNull(ClickHouseEnum type) {
        return ofNull(null, type);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref   object to update, could be null
     * @param clazz enum class
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseEnumValue ofNull(ClickHouseValue ref, Class<? extends Enum> clazz) {
        return ref instanceof ClickHouseEnumValue ? ((ClickHouseEnumValue) ref).set(true, 0)
                : new ClickHouseEnumValue(ClickHouseEnum.of(clazz), true, 0);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref  object to update, could be null
     * @param type enum type, null is same as {@link ClickHouseEnum#EMPTY}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseEnumValue ofNull(ClickHouseValue ref, ClickHouseEnum type) {
        return ref instanceof ClickHouseEnumValue ? ((ClickHouseEnumValue) ref).set(true, 0)
                : new ClickHouseEnumValue(type, true, 0);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseEnumValue of(Enum<?> value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @param type  enum type
     * @return object representing the value
     */
    public static ClickHouseEnumValue of(ClickHouseEnum type, int value) {
        return of(null, type, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @param type  enum type
     * @return object representing the value
     */
    public static ClickHouseEnumValue of(ClickHouseEnum type, Number value) {
        return value == null ? ofNull(null, type) : of(null, type, value.intValue());
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseEnumValue of(ClickHouseValue ref, Enum<?> value) {
        ClickHouseEnumValue v;
        if (ref instanceof ClickHouseEnumValue) {
            v = (ClickHouseEnumValue) ref;
            if (value != null) {
                v.set(false, value.ordinal());
            } else {
                v.resetToNullOrEmpty();
            }
        } else {
            if (value != null) {
                v = new ClickHouseEnumValue(ClickHouseEnum.of(value.getClass()), false, value.ordinal());
            } else {
                v = new ClickHouseEnumValue(ClickHouseEnum.EMPTY, true, 0);
            }
        }
        return v;
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param type  enum type, null is same as {@link ClickHouseEnum#EMPTY}
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseEnumValue of(ClickHouseValue ref, ClickHouseEnum type, int value) {
        return ref instanceof ClickHouseEnumValue ? ((ClickHouseEnumValue) ref).set(false, value)
                : new ClickHouseEnumValue(type, false, value);
    }

    private final ClickHouseEnum type;

    private boolean isNull;
    private int value;

    protected ClickHouseEnumValue(ClickHouseEnum type, boolean isNull, int value) {
        this.type = type != null ? type : ClickHouseEnum.EMPTY;

        set(isNull, value);
    }

    protected ClickHouseEnumValue set(boolean isNull, int value) {
        this.isNull = isNull;
        this.value = isNull ? 0 : type.validate(value);
        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public int getValue() {
        return value;
    }

    @Override
    public ClickHouseEnumValue copy(boolean deep) {
        return new ClickHouseEnumValue(type, isNull, value);
    }

    @Override
    public boolean isNullOrEmpty() {
        return isNull;
    }

    @Override
    public byte asByte() {
        return (byte) value;
    }

    @Override
    public short asShort() {
        return (short) value;
    }

    @Override
    public int asInteger() {
        return value;
    }

    @Override
    public long asLong() {
        return value;
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull ? null : BigInteger.valueOf(value);
    }

    @Override
    public float asFloat() {
        return value;
    }

    @Override
    public double asDouble() {
        return value;
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNull ? null : BigDecimal.valueOf(value, scale);
    }

    @Override
    public Object asObject() {
        return isNull ? null : type.name(value);
    }

    @Override
    public String asString() {
        if (isNull) {
            return null;
        }

        return type.name(value);
    }

    @Override
    public ClickHouseEnumValue resetToDefault() {
        return set(false, (byte) 0);
    }

    @Override
    public ClickHouseEnumValue resetToNullOrEmpty() {
        return set(true, (byte) 0);
    }

    @Override
    public String toSqlExpression() {
        return isNull ? ClickHouseValues.NULL_EXPR : Integer.toString(value);
    }

    @Override
    public ClickHouseEnumValue update(char value) {
        return set(false, value);
    }

    @Override
    public ClickHouseEnumValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ClickHouseEnumValue update(short value) {
        return set(false, value);
    }

    @Override
    public ClickHouseEnumValue update(int value) {
        return set(false, value);
    }

    @Override
    public ClickHouseEnumValue update(long value) {
        return set(false, (int) value);
    }

    @Override
    public ClickHouseEnumValue update(float value) {
        return set(false, (int) value);
    }

    @Override
    public ClickHouseEnumValue update(double value) {
        return set(false, (int) value);
    }

    @Override
    public ClickHouseEnumValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.intValueExact());
    }

    @Override
    public ClickHouseEnumValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.intValueExact());
    }

    @Override
    public ClickHouseEnumValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.ordinal());
    }

    @Override
    public ClickHouseEnumValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(false, type.value(value));
        }
        return this;
    }

    @Override
    public ClickHouseEnumValue update(ClickHouseValue value) {
        return value == null || value.isNullOrEmpty() ? resetToNullOrEmpty() : set(false, value.asInteger());
    }

    @Override
    public ClickHouseEnumValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).intValue());
        } else if (value instanceof String) {
            return set(false, type.value((String) value));
        } else if (value instanceof ClickHouseValue) {
            return set(false, ((ClickHouseValue) value).asInteger());
        }

        ClickHouseValue.super.update(value);
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseEnumValue v = (ClickHouseEnumValue) obj;
        return isNull == v.isNull && value == v.value && type.equals(v.type);
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        final int prime = 31;
        int result = prime + (isNull ? 1231 : 1237);
        result = prime * result + value;
        result = prime * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return ClickHouseValues.convertToString(this);
    }
}
