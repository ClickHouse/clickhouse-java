package com.clickhouse.client.exception;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutionException;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

/**
 * Specify ClickHouse exception to ClickHouseException and fill it with a vendor
 * code.
 */
public final class ClickHouseExceptionSpecifier {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseExceptionSpecifier.class);

    private ClickHouseExceptionSpecifier() {
    }

    /**
     * Handles ExecutionException.
     *
     * @param e      ExecutionException
     * @param server server
     * @return exception
     */
    public static ClickHouseException handle(ExecutionException e, ClickHouseNode server) {
        Throwable cause = e.getCause();
        if (cause == null) {
            cause = e;
        }
        return ClickHouseExceptionSpecifier.specify(cause, server);
    }

    public static ClickHouseException specify(Throwable cause, ClickHouseNode server) {
        return specify(cause != null ? cause.getMessage() : null, cause, server);
    }

    public static ClickHouseException specify(String clickHouseMessage, ClickHouseNode server) {
        return specify(clickHouseMessage, null, server);
    }

    public static ClickHouseException specify(String clickHouseMessage) {
        return specify(clickHouseMessage, null);
    }

    /**
     * Here we expect the ClickHouse error message to be of the following format:
     * "Code: 10, e.displayText() = DB::Exception: ...".
     */
    private static ClickHouseException specify(String clickHouseMessage, Throwable cause, ClickHouseNode server) {
        if (ClickHouseChecker.isNullOrEmpty(clickHouseMessage) && cause != null) {
            return getException(cause, server);
        }

        try {
            int code;
            if (clickHouseMessage.startsWith("Poco::Exception. Code: 1000, ")) {
                code = 1000;
            } else {
                // Code: 175, e.displayText() = DB::Exception:
                code = getErrorCode(clickHouseMessage);
            }
            // ошибку в изначальном виде все-таки укажем
            Throwable messageHolder = cause != null ? cause : new Throwable(clickHouseMessage);
            if (code == -1) {
                return getException(messageHolder, server);
            }

            return new ClickHouseException(code, messageHolder, server);
        } catch (Exception e) {
            log.error(
                    "Unsupported ClickHouse error format, please fix ClickHouseExceptionSpecifier, message: {}, error: {}",
                    clickHouseMessage, e.getMessage());
            return new ClickHouseUnknownException(clickHouseMessage, cause, server);
        }
    }

    private static int getErrorCode(String errorMessage) {
        int startIndex = errorMessage.indexOf(' ');
        int endIndex = startIndex == -1 ? -1 : errorMessage.indexOf(',', startIndex);

        if (startIndex == -1 || endIndex == -1) {
            return -1;
        }

        try {
            return Integer.parseInt(errorMessage.substring(startIndex + 1, endIndex));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static ClickHouseException getException(Throwable cause, ClickHouseNode server) {
        if (cause instanceof SocketTimeoutException)
        // if we've got SocketTimeoutException, we'll say that the query is not good.
        // This is not the same as SOCKET_TIMEOUT of clickhouse
        // but it actually could be a failing ClickHouse
        {
            return new ClickHouseException(ClickHouseErrorCode.TIMEOUT_EXCEEDED.code, cause, server);
        } else if (cause instanceof ConnectException)
        // couldn't connect to ClickHouse during connectTimeout
        {
            return new ClickHouseException(ClickHouseErrorCode.NETWORK_ERROR.code, cause, server);
        } else {
            return new ClickHouseUnknownException(cause, server);
        }
    }

}
