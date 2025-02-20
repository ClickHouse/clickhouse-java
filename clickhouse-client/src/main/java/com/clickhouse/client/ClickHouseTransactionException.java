package com.clickhouse.client;

@Deprecated
public class ClickHouseTransactionException extends ClickHouseException {
    public static final int ERROR_INVALID_TRANSACTION = 649;
    public static final int ERROR_UNKNOWN_STATUS_OF_TRANSACTION = 659;

    private final ClickHouseTransaction tx;

    public ClickHouseTransactionException(String message, ClickHouseTransaction tx) {
        this(ERROR_INVALID_TRANSACTION, message, tx);
    }

    public ClickHouseTransactionException(String message, Throwable cause, ClickHouseTransaction tx) {
        this(ERROR_INVALID_TRANSACTION, message, cause, tx);
    }

    public ClickHouseTransactionException(int code, String message, Throwable cause, ClickHouseTransaction tx) {
        super(code, message, cause);
        this.tx = tx;
    }

    public ClickHouseTransactionException(int code, String message, ClickHouseTransaction tx) {
        super(code, message, tx.getServer());
        this.tx = tx;
    }

    public ClickHouseTransaction getTransaction() {
        return tx;
    }
}
