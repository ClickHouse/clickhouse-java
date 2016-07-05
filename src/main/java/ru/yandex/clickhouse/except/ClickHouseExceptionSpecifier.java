package ru.yandex.clickhouse.except;

import org.apache.http.conn.ConnectTimeoutException;
import ru.yandex.clickhouse.util.Logger;
import ru.yandex.clickhouse.util.apache.StringUtils;

import java.net.ConnectException;
import java.net.SocketTimeoutException;


/**
 * @author lemmsh
 * @since 7/17/14
 *
 * Specify clickhouse exception to ClickHouseException and fill it with a vendor code.
 */

public final class ClickHouseExceptionSpecifier {

    private static final Logger log = Logger.of(ClickHouseExceptionSpecifier.class);

    private ClickHouseExceptionSpecifier() {
    }

    public static ClickHouseException specify(Throwable cause, String host, int port) {
        return specify(null, cause, host, port);
    }

    public static ClickHouseException specify(String clickhouseMessage, String host, int port) {
        return specify(clickhouseMessage, null, host, port);
    }

    /**
     * Here we expect the clickhouse error message to be of the following format:
     * "Code: 10, e.displayText() = DB::Exception: ...".
     */
    public static ClickHouseException specify(String clickhouseMessage, Throwable cause, String host, int port) {
        if (StringUtils.isEmpty(clickhouseMessage) && cause != null) {
            if (cause instanceof SocketTimeoutException)
                // if we've got SocketTimeoutException, we'll say that the query is not good. This is not the same as SOCKET_TIMEOUT of clickhouse
                // but it actually could be a failing clickhouse
                return new ClickHouseException(ClickHouseErrorCode.TIMEOUT_EXCEEDED.code, cause, host, port);
            else if (cause instanceof ConnectTimeoutException || cause instanceof ConnectException)
                // couldn't connect to clickhouse during connectTimeout
                return new ClickHouseException(ClickHouseErrorCode.NETWORK_ERROR.code, cause, host, port);
            else
                return new ClickHouseUnknownException(cause, host, port);
        }
        try {
            int code;
            if(clickhouseMessage.startsWith("Poco::Exception. Code: 1000, ")) {
                code = 1000;
            } else {
                // Code: 175, e.displayText() = DB::Exception:
                code = Integer.parseInt(clickhouseMessage.substring(clickhouseMessage.indexOf(' ') + 1, clickhouseMessage.indexOf(',')));
            }
            // ошибку в изначальном виде все-таки укажем
            Throwable messageHolder = cause != null ? cause : new Throwable(clickhouseMessage);
            return new ClickHouseException(code, messageHolder, host, port);
        } catch (Exception e) {
            log.error("Unsupported clickhouse error format, please fix ClickhouseExceptionSpecifier, message: "
                            + clickhouseMessage + ", error: " + e.getMessage());
            return new ClickHouseUnknownException(clickhouseMessage, cause, host, port);
        }
    }

    private interface ClickhouseExceptionFactory {
        ClickHouseException create(Integer code, Throwable cause, String host, int port);
    }

}