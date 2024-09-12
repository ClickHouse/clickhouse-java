package com.clickhouse.client.api.query;


/**
 * Class used to set value for individual fields in a POJO.
 * Implementation will have reference to a specific POJO property.
 * Caller will use this class to set value for the property.
 * Methods are overloaded to support primitive types and avoid boxing.
 */
public interface POJOSetter {

    default void setValue(Object obj, boolean value) {
        throw new UnsupportedOperationException("Unsupported type: boolean");
    };

    default  void setValue(Object obj, byte value) {
        throw new UnsupportedOperationException("Unsupported type: byte");
    };

    default void setValue(Object obj, char value) {
        throw new UnsupportedOperationException("Unsupported type: char");
    };

    default void setValue(Object obj, short value) {
        throw new UnsupportedOperationException("Unsupported type: short");
    };

    default void setValue(Object obj, int value) {
        throw new UnsupportedOperationException("Unsupported type: int");
    };

    default void setValue(Object obj, long value) {
        throw new UnsupportedOperationException("Unsupported type: long");
    };

    default void setValue(Object obj, float value) {
        throw new UnsupportedOperationException("Unsupported type: float");
    };

    default void setValue(Object obj, double value) {
        throw new UnsupportedOperationException("Unsupported type: double");
    };

    void setValue(Object obj, Object value);
}
