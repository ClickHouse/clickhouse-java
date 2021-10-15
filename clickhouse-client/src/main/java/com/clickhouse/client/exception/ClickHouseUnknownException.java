package com.clickhouse.client.exception;

import com.clickhouse.client.ClickHouseNode;

public class ClickHouseUnknownException extends ClickHouseException {
    /**
     * Generated ID.
     */
    private static final long serialVersionUID = -1724790228244438601L;

    public ClickHouseUnknownException(Throwable cause, ClickHouseNode server) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION.code, cause, server);
    }

    public ClickHouseUnknownException(String message, Throwable cause, ClickHouseNode server) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION.code, message, cause, server);
    }

    public ClickHouseUnknownException(String message, Throwable cause) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION.code, message, cause);
    }

    public ClickHouseUnknownException(Integer code, Throwable cause, ClickHouseNode server) {
        super(code, cause, server);
    }
}
