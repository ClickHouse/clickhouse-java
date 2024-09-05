package com.clickhouse.client.api.query;


/**
 * Class used to set value for individual fields in a POJO.
 * Implementation will have reference to a specific POJO property.
 * Caller will use this class to set value for the property.
 * Methods are overloaded to support primitive types and avoid boxing.
 */
public interface POJOSetter<T> {

    default void setValue(T obj, boolean value) {
        throw new UnsupportedOperationException("Unsupported type: boolean");
    };

    default  void setValue(T obj, byte value) {
        throw new UnsupportedOperationException("Unsupported type: byte");
    };

    default void setValue(T obj, char value) {
        throw new UnsupportedOperationException("Unsupported type: char");
    };

    default void setValue(T obj, short value) {
        throw new UnsupportedOperationException("Unsupported type: short");
    };

    default void setValue(T obj, int value) {
        throw new UnsupportedOperationException("Unsupported type: int");
    };

    default void setValue(T obj, long value) {
        throw new UnsupportedOperationException("Unsupported type: long");
    };

    default void setValue(T obj, float value) {
        throw new UnsupportedOperationException("Unsupported type: float");
    };

    default void setValue(T obj, double value) {
        throw new UnsupportedOperationException("Unsupported type: double");
    };

    void setValue(T obj, Object value);
}
