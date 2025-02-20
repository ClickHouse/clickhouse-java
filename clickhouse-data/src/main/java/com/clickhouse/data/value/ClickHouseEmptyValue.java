package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of Nothing.
 */
@Deprecated
public final class ClickHouseEmptyValue implements ClickHouseValue {
    /**
     * Singleton.
     */
    public static final ClickHouseEmptyValue INSTANCE = new ClickHouseEmptyValue();

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return null;
    }

    @Override
    public BigInteger asBigInteger() {
        return null;
    }

    @Override
    public byte asByte() {
        return (byte) 0;
    }

    @Override
    public double asDouble() {
        return 0D;
    }

    @Override
    public float asFloat() {
        return 0F;
    }

    @Override
    public int asInteger() {
        return 0;
    }

    @Override
    public long asLong() {
        return 0L;
    }

    @Override
    public Object asObject() {
        return null;
    }

    @Override
    public short asShort() {
        return (short) 0;
    }

    @Override
    public ClickHouseValue copy(boolean deep) {
        return INSTANCE;
    }

    @Override
    public boolean isNullOrEmpty() {
        return true;
    }

    @Override
    public ClickHouseValue resetToDefault() {
        return INSTANCE;
    }

    @Override
    public ClickHouseValue resetToNullOrEmpty() {
        return INSTANCE;
    }

    @Override
    public String toSqlExpression() {
        return ClickHouseValues.NULL_EXPR;
    }

    @Override
    public ClickHouseValue update(byte value) {
        return INSTANCE;
    }

    @Override
    public ClickHouseValue update(short value) {
        return INSTANCE;
    }

    @Override
    public ClickHouseValue update(int value) {
        return INSTANCE;
    }

    @Override
    public ClickHouseValue update(long value) {
        return INSTANCE;
    }

    @Override
    public ClickHouseValue update(float value) {
        return INSTANCE;
    }

    @Override
    public ClickHouseValue update(double value) {
        return INSTANCE;
    }

    @Override
    public ClickHouseValue update(BigInteger value) {
        return INSTANCE;
    }

    @Override
    public ClickHouseValue update(BigDecimal value) {
        return INSTANCE;
    }

    @Override
    public ClickHouseValue update(String value) {
        return INSTANCE;
    }

    @Override
    public ClickHouseValue update(ClickHouseValue value) {
        return INSTANCE;
    }

    @Override
    public String toString() {
        return "";
    }

    private ClickHouseEmptyValue() {
    }
}
