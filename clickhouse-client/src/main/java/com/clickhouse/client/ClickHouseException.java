package com.clickhouse.client;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Locale;
import java.util.concurrent.TimeoutException;

/**
 * Exception thrown from ClickHouse server. See full list at
 * https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp.
 */
public class ClickHouseException extends Exception {
    /**
     * Generated ID.
     */
    private static final long serialVersionUID = -2417038200885554382L;

    public static final int ERROR_ABORTED = 236;
    public static final int ERROR_CANCELLED = 394;
    public static final int ERROR_NETWORK = 210;
    public static final int ERROR_POCO = 1000;
    public static final int ERROR_TIMEOUT = 159;
    public static final int ERROR_UNKNOWN = 1002;

    static final String MSG_CONNECT_TIMED_OUT = "connect timed out";

    private final int errorCode;

    private static String buildErrorMessage(int code, Throwable cause, ClickHouseNode server) {
        return buildErrorMessage(code, cause != null ? cause.getMessage() : null, server);
    }

    private static String buildErrorMessage(int code, String message, ClickHouseNode server) {
        StringBuilder builder = new StringBuilder();

        if (message != null && !message.isEmpty()) {
            builder.append(message);
        } else {
            builder.append("Unknown error ").append(code);
        }

        if (server != null) {
            builder.append(", server ").append(server);
        }

        return builder.toString();
    }

    private static int extractErrorCode(String errorMessage) {
        if (errorMessage == null || errorMessage.isEmpty()) {
            return ERROR_UNKNOWN;
        } else if (errorMessage.startsWith("Poco::Exception. Code: 1000, ")) {
            return ERROR_POCO;
        }

        int startIndex = errorMessage.indexOf(' ');
        if (startIndex >= 0) {
            for (int i = ++startIndex, len = errorMessage.length(); i < len; i++) {
                char ch = errorMessage.charAt(i);
                if (ch == '.' || ch == ',' || Character.isWhitespace(ch)) {
                    try {
                        return Integer.parseInt(errorMessage.substring(startIndex, i));
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                    break;
                }
            }
        }

        // this is confusing as usually it's a client-side exception
        return ERROR_UNKNOWN;
    }

    /**
     * Creates an exception for cancellation.
     *
     * @param e      exception
     * @param server server
     * @return ClickHouseException
     */
    public static ClickHouseException forCancellation(Exception e, ClickHouseNode server) {
        Throwable cause = e.getCause();
        if (cause == null) {
            cause = e;
        }

        return new ClickHouseException(ERROR_ABORTED, cause, server);
    }

    /**
     * Checks if the given exception represents connection timeout error.
     *
     * @param t exception
     * @return true if the exception represents connection timeout error; false
     *         otherwise
     */
    public static boolean isConnectTimedOut(Throwable t) {
        if (t instanceof SocketTimeoutException || t instanceof TimeoutException) {
            String msg = t.getMessage();
            if (msg != null && msg.length() >= MSG_CONNECT_TIMED_OUT.length()) {
                msg = msg.substring(0, MSG_CONNECT_TIMED_OUT.length()).toLowerCase(Locale.ROOT);
            }
            return MSG_CONNECT_TIMED_OUT.equals(msg);
        }

        return false;
    }

    /**
     * Creates an exception to encapsulate cause of the given exception.
     *
     * @param e      exception
     * @param server server
     * @return ClickHouseException
     */
    public static ClickHouseException of(Throwable e, ClickHouseNode server) {
        if (e instanceof ClickHouseException) {
            return (ClickHouseException) e;
        }

        Throwable cause = e != null ? e.getCause() : e;
        if (cause instanceof ClickHouseException) {
            return (ClickHouseException) cause;
        } else if (cause == null) {
            cause = e;
        }

        ClickHouseException exp;
        // If we've got SocketTimeoutException, we'll say that the query is not good.
        // This is not the same as SOCKET_TIMEOUT of clickhouse but it actually could be
        // a failing ClickHouse
        if (cause instanceof SocketTimeoutException || cause instanceof TimeoutException) {
            exp = new ClickHouseException(ERROR_TIMEOUT, cause, server);
        } else if (cause instanceof ConnectException) {
            exp = new ClickHouseException(ERROR_NETWORK, cause, server);
        } else {
            exp = new ClickHouseException(extractErrorCode(cause != null ? cause.getMessage() : null), cause, server);
        }

        return exp;
    }

    /**
     * Creates an exception to encapsulate the given error message.
     *
     * @param message error message
     * @param server  server
     * @return ClickHouseException
     */
    public static ClickHouseException of(String message, ClickHouseNode server) {
        return new ClickHouseException(extractErrorCode(message), message, server);
    }

    /**
     * Constructs an exception with cause.
     *
     * @param code   error code
     * @param cause  cause of the exception
     * @param server server
     */
    public ClickHouseException(int code, Throwable cause, ClickHouseNode server) {
        super(buildErrorMessage(code, cause, server), cause);

        errorCode = code;
    }

    /**
     * Constructs an exception without cause.
     *
     * @param code    error code
     * @param message error message
     * @param server  server
     */
    public ClickHouseException(int code, String message, ClickHouseNode server) {
        super(buildErrorMessage(code, message, server), null);

        errorCode = code;
    }

    /**
     * Gets error code.
     *
     * @return error code
     */
    public int getErrorCode() {
        return errorCode;
    }
}
