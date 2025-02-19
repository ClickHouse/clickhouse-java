package com.clickhouse.client;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Exception thrown from ClickHouse server. See full list at
 * https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp.
 */
@Deprecated
public class ClickHouseException extends Exception {
    /**
     * Generated ID.
     */
    private static final long serialVersionUID = -2417038200885554382L;

    public static final int ERROR_UNKNOWN_SETTING = 115;
    public static final int ERROR_ABORTED = 236;
    public static final int ERROR_CANCELLED = 394;
    public static final int ERROR_NETWORK = 210;
    public static final int ERROR_SESSION_NOT_FOUND = 372;
    public static final int ERROR_SESSION_IS_LOCKED = 373;
    public static final int ERROR_POCO = 1000;
    public static final int ERROR_TIMEOUT = 159;
    public static final int ERROR_UNKNOWN = 1002;
    public static final int ERROR_SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY = 455;

    static final String MSG_CODE = "Code: ";
    static final String MSG_CONNECT_TIMED_OUT = "connect timed out";
    static final Pattern ERROR_CODE_PATTERN = Pattern.compile("Code:[ ]*(\\d+)");

    private final int errorCode;

    private final ClickHouseNode server;

    private static String buildErrorMessageImpl(int code, Throwable cause) {
        return buildErrorMessageImpl(code, cause != null ? cause.getMessage() : null);
    }

    private static String buildErrorMessageImpl(int code, String message) {
        StringBuilder builder = new StringBuilder();

        if (message != null && !message.isEmpty()) {
            builder.append(message);
        } else if (code == ERROR_ABORTED) {
            builder.append(MSG_CODE).append(code).append(". Execution aborted");
        } else if (code == ERROR_CANCELLED) {
            builder.append(MSG_CODE).append(code).append(". Execution cancelled");
        } else if (code == ERROR_TIMEOUT) {
            builder.append(MSG_CODE).append(code).append(". Execution timed out");
        } else {
            builder.append("Unknown error ").append(code);
        }

        return builder.toString();
    }

    private static int extractErrorCode(String errorMessage) {
        if (errorMessage == null || errorMessage.isEmpty()) {
            return ERROR_UNKNOWN;
        }
        Matcher matcher = ERROR_CODE_PATTERN.matcher(errorMessage);
        // when not match, this is confusing as usually it's a client-side exception
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : ERROR_UNKNOWN;
    }

    static Throwable getRootCause(Throwable t) {
        if (t == null) {
            return t;
        }

        Throwable rootCause = t;
        while (!(rootCause instanceof ClickHouseException) && rootCause.getCause() != null
                && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }

    /**
     * Builds error message like {@code "Code: <code>, <detail>"}.
     *
     * @param code   error code
     * @param detail detail of the error
     * @return non-null error message
     */
    public static String buildErrorMessage(int code, String detail) {
        return new StringBuilder().append(MSG_CODE).append(code).append(", ").append(detail).toString();
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
            return msg != null && msg.toLowerCase().contains(MSG_CONNECT_TIMED_OUT);
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

        Throwable cause = getRootCause(e);
        ClickHouseException exp;
        if (cause instanceof ClickHouseException) {
            exp = (ClickHouseException) cause;
        } else if (cause instanceof SocketTimeoutException || cause instanceof TimeoutException) {
            // If we've got SocketTimeoutException, we'll say that the query is not good.
            // This is not the same as SOCKET_TIMEOUT of clickhouse but it actually could be
            // a failing ClickHouse
            exp = new ClickHouseException(ERROR_TIMEOUT, cause, server);
        } else if (cause instanceof ConnectException || cause instanceof UnknownHostException) {
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
        super(buildErrorMessageImpl(code, cause), cause);
        this.server = server;
        this.errorCode = code;
    }

    /**
     * Constructs an exception without cause.
     *
     * @param code    error code
     * @param message error message
     * @param server  server
     */
    public ClickHouseException(int code, String message, ClickHouseNode server) {
        super(buildErrorMessageImpl(code, message), null);

        this.server = server;
        this.errorCode = code;
    }

    /**
     * Constructs an exception.
     *
     * @param code    error code
     * @param message error message
     * @param cause   cause
     */
    protected ClickHouseException(int code, String message, Throwable cause) {
        super(message, cause);

        this.server = null;
        this.errorCode = code;
    }

    /**
     * Gets error code.
     *
     * @return error code
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * Get the server that caused the exception.
     * If the exception is not caused by a server, this method will return null.
     * @return server
     */
    public ClickHouseNode getServer() {
        return server;
    }
}
