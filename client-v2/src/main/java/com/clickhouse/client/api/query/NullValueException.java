package com.clickhouse.client.api.query;

import com.clickhouse.client.api.ClientException;

/**
 * Throw when a null value cannot be returned because of data type.
 * For example when a primitive type is expected but a null value is received.
 *
 */
public class NullValueException extends ClientException {
    public NullValueException(String message) {
        super(message);
    }

    public NullValueException(String message, Throwable cause) {
        super(message, cause);
    }
}
