package com.clickhouse.client.api;

public class ServerException extends ClickHouseException {

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
        this.isRetryable = discoverIsRetryable(code, message, transportProtocolCode);
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

    public boolean isRetryable() {
        return isRetryable;
    }

    private boolean discoverIsRetryable(int code, String message, int transportProtocolCode) {
        //Let's check if we have a ServerException to reference the error code
        //https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
        switch (code) { // UNEXPECTED_END_OF_FILE
            case 3: // UNEXPECTED_END_OF_FILE
            case 107: // FILE_DOESNT_EXIST
            case 159: // TIMEOUT_EXCEEDED
            case 164: // READONLY
            case 202: // TOO_MANY_SIMULTANEOUS_QUERIES
            case 203: // NO_FREE_CONNECTION
            case 209: // SOCKET_TIMEOUT
            case 210: // NETWORK_ERROR
            case 241: // MEMORY_LIMIT_EXCEEDED
            case 242: // TABLE_IS_READ_ONLY
            case 252: // TOO_MANY_PARTS
            case 285: // TOO_FEW_LIVE_REPLICAS
            case 319: // UNKNOWN_STATUS_OF_INSERT
            case 425: // SYSTEM_ERROR
            case 999: // KEEPER_EXCEPTION
                return true;
        };
        return false;
    }
}
