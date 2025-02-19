package com.clickhouse.data;

/**
 * This interface represents a generic array value.
 */
@Deprecated
public interface ClickHouseArraySequence extends ClickHouseValue {
    /**
     * Allocates an array according to given length. Same as
     * {@code allocate(length, Object.class, 1)}.
     *
     * @param length length of the array
     * @return this value
     */
    default ClickHouseArraySequence allocate(int length) {
        return allocate(length, Object.class, 1);
    }

    /**
     * Allocates an array according to given arguments. Same as
     * {@code allocate(length, clazz, 1)}.
     *
     * @param length length of the array
     * @param clazz  optional value type, null means {@code Object.class}
     * @return this value
     */
    default ClickHouseArraySequence allocate(int length, Class<?> clazz) {
        return allocate(length, clazz, 1);
    }

    /**
     * Allocates an array according to given arguments. Pay attention that this will
     * will not create new array but reuse existing one, when {@code length} are
     * {@code clazz} not changed.
     *
     * @param length length of the array
     * @param clazz  optional value type, null means {@code Object.class}
     * @param level  level of the array, zero or negative number is treated as
     *               {@code 1}
     * @return this value
     */
    ClickHouseArraySequence allocate(int length, Class<?> clazz, int level);

    /**
     * Gets length of this array.
     *
     * @return length of this array
     */
    int length();

    /**
     * Gets value at the specified position in this array.
     *
     * @param <V>   type of the value
     * @param index index which is greater than or equal to zero and it's always
     *              smaller than {@link #length()}
     * @param value non-null template object to retrieve the value
     * @return non-null value which is same as {@code value}
     */
    <V extends ClickHouseValue> V getValue(int index, V value);

    /**
     * Sets value to the specified position in this array.
     *
     * @param index index which is greater than or equal to zero and it's always
     *              smaller than {@link #length()}
     * @param value non-null container of the value
     * @return this value
     */
    ClickHouseArraySequence setValue(int index, ClickHouseValue value);

    @Override
    default ClickHouseArraySequence copy() {
        return copy(false);
    }

    @Override
    ClickHouseArraySequence copy(boolean deep);
}
