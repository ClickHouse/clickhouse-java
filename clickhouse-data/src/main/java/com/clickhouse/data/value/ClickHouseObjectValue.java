package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

public abstract class ClickHouseObjectValue<T> implements ClickHouseValue {
    // a nested structure like Map might not be always serializable
    @SuppressWarnings("squid:S1948")
    private T value;

    protected ClickHouseObjectValue(T value) {
        set(value);
    }

    protected ClickHouseObjectValue<T> set(T value) {
        this.value = value;
        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public final T getValue() {
        return value;
    }

    @Override
    public boolean isNullOrEmpty() {
        return value == null;
    }

    @Override
    public byte asByte() {
        if (isNullOrEmpty()) {
            return (byte) 0;
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_OBJECT, ClickHouseValues.TYPE_BYTE);
    }

    @Override
    public short asShort() {
        if (isNullOrEmpty()) {
            return (short) 0;
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_OBJECT, ClickHouseValues.TYPE_SHORT);
    }

    @Override
    public int asInteger() {
        if (isNullOrEmpty()) {
            return 0;
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_OBJECT, ClickHouseValues.TYPE_INT);
    }

    @Override
    public long asLong() {
        if (isNullOrEmpty()) {
            return 0L;
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_OBJECT, ClickHouseValues.TYPE_LONG);
    }

    @Override
    public BigInteger asBigInteger() {
        if (isNullOrEmpty()) {
            return null;
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_OBJECT, ClickHouseValues.TYPE_BIG_INTEGER);
    }

    @Override
    public float asFloat() {
        if (isNullOrEmpty()) {
            return 0F;
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_OBJECT, ClickHouseValues.TYPE_FLOAT);
    }

    @Override
    public double asDouble() {
        if (isNullOrEmpty()) {
            return 0D;
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_OBJECT, ClickHouseValues.TYPE_DOUBLE);
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_OBJECT, ClickHouseValues.TYPE_BIG_DECIMAL);
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    @Override
    public String asString() {
        if (isNullOrEmpty()) {
            return null;
        }

        return value.toString();
    }

    @Override
    public ClickHouseObjectValue<T> resetToNullOrEmpty() {
        return set(null);
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ClickHouseValues.NULL_EXPR : asString();
    }

    @Override
    public ClickHouseValue update(Object value) {
        return ClickHouseValue.super.update(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseObjectValue<?> v = (ClickHouseObjectValue<?>) obj;
        return value == v.value || (value != null && value.equals(v.value));
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return ClickHouseValues.convertToString(this);
    }
}
