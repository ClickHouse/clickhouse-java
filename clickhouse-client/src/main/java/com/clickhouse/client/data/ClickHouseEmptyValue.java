package com.clickhouse.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.clickhouse.client.ClickHouseValue;

/**
 * Wrapper class of Nothing.
 */
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
        throw new IllegalStateException("Empty value cannot be converted to byte");
    }

    @Override
    public double asDouble() {
        throw new IllegalStateException("Empty value cannot be converted to double");
    }

    @Override
    public float asFloat() {
        throw new IllegalStateException("Empty value cannot be converted to float");
    }

    @Override
    public int asInteger() {
        throw new IllegalStateException("Empty value cannot be converted to int");
    }

    @Override
    public long asLong() {
        throw new IllegalStateException("Empty value cannot be converted to long");
    }

    @Override
    public Object asObject() {
        return null;
    }

    @Override
    public short asShort() {
        throw new IllegalStateException("Empty value cannot be converted to short");
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
    public ClickHouseValue resetToNullOrEmpty() {
        return INSTANCE;
    }

    @Override
    public String toSqlExpression() {
        return toString();
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
