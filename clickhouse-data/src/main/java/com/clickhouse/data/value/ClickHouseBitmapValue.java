package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@code Bitmap}.
 */
@Deprecated
public class ClickHouseBitmapValue extends ClickHouseObjectValue<ClickHouseBitmap> {
    /**
     * Create a new instance representing empty value.
     *
     * @param valueType value type, must be native integer
     * @return new instance representing empty value
     */
    public static ClickHouseBitmapValue ofEmpty(ClickHouseDataType valueType) {
        return ofEmpty(null, valueType);
    }

    /**
     * Update given value to empty or create a new instance if {@code ref} is null.
     * 
     * @param ref       object to update, could be null
     * @param valueType value type, must be native integer
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseBitmapValue ofEmpty(ClickHouseValue ref, ClickHouseDataType valueType) {
        ClickHouseBitmap v = ClickHouseBitmap.empty(valueType);
        return ref instanceof ClickHouseBitmapValue ? (ClickHouseBitmapValue) ((ClickHouseBitmapValue) ref).set(v)
                : new ClickHouseBitmapValue(v);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseBitmapValue of(ClickHouseBitmap value) {
        return of(null, value);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseBitmapValue of(ClickHouseValue ref, ClickHouseBitmap value) {
        if (value == null) {
            value = ClickHouseBitmap.empty();
        }

        return ref instanceof ClickHouseBitmapValue ? (ClickHouseBitmapValue) ((ClickHouseBitmapValue) ref).set(value)
                : new ClickHouseBitmapValue(value);
    }

    protected ClickHouseBitmapValue(ClickHouseBitmap value) {
        super(value);
    }

    @Override
    public ClickHouseBitmapValue copy(boolean deep) {
        return new ClickHouseBitmapValue(getValue());
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().isEmpty();
    }

    @Override
    public byte asByte() {
        if (isNullOrEmpty()) {
            return (byte) 0;
        }

        ClickHouseBitmap v = getValue();
        if (v.getCardinality() != 1) {
            throw new IllegalArgumentException(
                    ClickHouseUtils.format("Expect only one element but we got %d", v.getLongCardinality()));
        }
        return v.innerType.getByteLength() > 4 ? (byte) v.toLongArray()[0] : (byte) v.toIntArray()[0];
    }

    @Override
    public short asShort() {
        if (isNullOrEmpty()) {
            return (short) 0;
        }

        ClickHouseBitmap v = getValue();
        if (v.getCardinality() != 1) {
            throw new IllegalArgumentException(
                    ClickHouseUtils.format("Expect only one element but we got %d", v.getLongCardinality()));
        }
        return v.innerType.getByteLength() > 4 ? (short) v.toLongArray()[0] : (short) v.toIntArray()[0];
    }

    @Override
    public int asInteger() {
        if (isNullOrEmpty()) {
            return 0;
        }

        ClickHouseBitmap v = getValue();
        if (v.getCardinality() != 1) {
            throw new IllegalArgumentException(
                    ClickHouseUtils.format("Expect only one element but we got %d", v.getLongCardinality()));
        }
        return v.innerType.getByteLength() > 4 ? (int) v.toLongArray()[0] : v.toIntArray()[0];
    }

    @Override
    public long asLong() {
        if (isNullOrEmpty()) {
            return 0;
        }

        ClickHouseBitmap v = getValue();
        if (v.getCardinality() != 1) {
            throw new IllegalArgumentException(
                    ClickHouseUtils.format("Expect only one element but we got %d", v.getLongCardinality()));
        }
        return v.innerType.getByteLength() > 4 ? v.toLongArray()[0] : v.toIntArray()[0];
    }

    @Override
    public BigInteger asBigInteger() {
        if (isNullOrEmpty()) {
            return null;
        }

        return BigInteger.valueOf(asLong());
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : (float) asInteger();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : (double) asLong();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(asBigInteger()).setScale(scale);
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    public long getCardinality() {
        return isNullOrEmpty() ? 0L : getValue().getLongCardinality();
    }

    @Override
    public ClickHouseBitmapValue resetToDefault() {
        set(ClickHouseBitmap.empty());
        return this;
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ClickHouseValues.NULL_EXPR : String.valueOf(getValue());
    }

    @Override
    public ClickHouseBitmapValue update(boolean value) {
        set(ClickHouseBitmap.wrap(value ? (byte) 1 : (byte) 0));
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(char value) {
        set(ClickHouseBitmap.wrap((short) value));
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(byte value) {
        set(ClickHouseBitmap.wrap(value));
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(short value) {
        set(ClickHouseBitmap.wrap(value));
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(int value) {
        set(ClickHouseBitmap.wrap(value));
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(long value) {
        set(ClickHouseBitmap.wrap(value));
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(float value) {
        set(ClickHouseBitmap.wrap((int) value));
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(double value) {
        set(ClickHouseBitmap.wrap((long) value));
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseBitmap.wrap(value.longValue()));
        }
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseBitmap.wrap(value.longValue()));
        }
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseBitmap.wrap(value.ordinal()));
        }
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(Inet4Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseBitmap.wrap(ClickHouseValues.convertToBigInteger(value).longValue()));
        }
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(Inet6Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseBitmap.wrap(ClickHouseValues.convertToBigInteger(value).longValue()));
        }
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseBitmap.wrap(value.toEpochDay()));
        }
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseBitmap.wrap(value.toSecondOfDay()));
        }
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseBitmap.wrap(value.toEpochSecond(ZoneOffset.UTC)));
        }
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(ClickHouseBitmap.wrap(Long.parseLong(value)));
        }
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(UUID value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseBitmap.wrap(ClickHouseValues.convertToBigInteger(value).longValue()));
        }
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else if (value instanceof ClickHouseBitmapValue) {
            set(((ClickHouseBitmapValue) value).getValue());
        } else {
            set(ClickHouseBitmap.wrap(value.asInteger()));
        }
        return this;
    }

    @Override
    public ClickHouseBitmapValue update(Object value) {
        if (value instanceof ClickHouseBitmap) {
            set((ClickHouseBitmap) value);
            return this;
        }

        super.update(value);
        return this;
    }
}
