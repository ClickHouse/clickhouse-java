package com.clickhouse.client.api.exception;

public class Utils {

    public static boolean isRetryable(int code, String message, int transportProtocolCode) {
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
            case 0:
                switch (transportProtocolCode) { // Bad request
                    case 400: // Bad request
                    case 500: // Internal server error
                        return true;
                };
        };
        return false;
    }
}
