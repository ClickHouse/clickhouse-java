package com.clickhouse.client.api;

public class ServerException extends RuntimeException {

    public static final int CODE_UNKNOWN = 0;

    public static final int TABLE_NOT_FOUND = 60;

    private final int code;

    private final int transportProtocolCode;

    public ServerException(int code, String message) {
        this(code, message, 500);
    }

    public ServerException(int code, String message, int transportProtocolCode) {
        super(message);
        this.code = code;
        this.transportProtocolCode = transportProtocolCode;
    }

    /**
     * Returns CH server error code. May return 0 if code is unknown.
     * @return - error code from server response
     */
    public int getCode() {
        return code;
    }

    /**
     * Returns error code of underlying transport protocol. For example, HTTP status.
     * By default, will return {@code 500 } what is derived from HTTP Server Internal Error.
     *
     * @return - transport status code
     */
    public int getTransportProtocolCode() {
        return transportProtocolCode;
    }
}
