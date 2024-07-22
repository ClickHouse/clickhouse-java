package com.clickhouse.client.api;

public class ServerException extends RuntimeException {

    public static final int CODE_UNKNOWN = 0;

    public static final int TABLE_NOT_FOUND = 60;

    private final int code;
    public ServerException(int code, String message) {
        super(message);
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
