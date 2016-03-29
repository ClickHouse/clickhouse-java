package ru.yandex.metrika.clickhouse.except;

import org.apache.http.conn.ConnectTimeoutException;
import ru.yandex.metrika.clickhouse.CHException;
import ru.yandex.metrika.clickhouse.util.CopypasteUtils;
import ru.yandex.metrika.clickhouse.util.Logger;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * @author lemmsh
 * @since 7/17/14
 *
 *  * Исключения, возбуждаемые плохим ответом от кликхауса, будут рождаться здесь. Разделяем их на 4 категории:
 * 1. ClickhouseApiException - пришел ответ с ошибкой, характерной для неверно сгенерированного запроса, например,
 * неизвестный столбец, токен, несовместимость типов и т.п., т.е. виновато API, следует залогировать это с более
 * высоким приоритетом и чинить первопричину;
 * 2. ClickhouseDBException - пришел ответ с ошибкой, характерной для внутренней ошибки СУБД, например, закончились
 * коннекты к реплике, закончилось место и т.п.;
 * 3. ClickhouseQueryException - запрос либо слишком сложный и не смог уложиться в отведенные квоты по памяти/времени,
 * либо это запрос-неудачник, пришедший в неудачное время, когда нода кликхауса была перегружена. В этом случае отдаем
 * пользователю совет сузить запрос (включить/уменьшить семплирование), попробовать запрос позже и т.п.;
 * 4. ClickhouseUnhandledException - мир не идеален, а в не идеальном мире не всегда удается сразу разделить
 * ошибки кликхауса на 3 категории, указанные выше, вот за этим создана четвертая. Сюда попадают впервые появившиеся в
 * логах ошибки, чтобы в наиближайшем будущем превратиться в Api, DB или Query вариант ClickhouseException.
 *
 * https://github.yandex-team.ru/Metrika/metrika-core/blob/master/metrica/src/dbms/include/DB/Core/ErrorCodes.h
 */

public final class ClickhouseExceptionSpecifier {

    private static final Logger log = Logger.of(ClickhouseExceptionSpecifier.class);

    private ClickhouseExceptionSpecifier() {
    }

    public static CHException specify(Throwable cause, String host, int port) {
        return specify(null, cause, host, port);
    }

    public static CHException specify(String clickhouseMessage, String host, int port) {
        return specify(clickhouseMessage, null, host, port);
    }

    /**
     * Очень надеемся, что формат сообщения от кликхауса имеет вид "Code: 10, e.displayText() = DB::Exception: ...".
     * Очень надеемся, что кто-то будет это проверять в тестах.
     * Очень надеемся, что кто-то будет в эти тесты смотреть и актуализировать парсинг.
     */
    public static CHException specify(String clickhouseMessage, Throwable cause, String host, int port) {
        if (CopypasteUtils.isEmpty(clickhouseMessage) && cause != null) {
            if (cause instanceof SocketTimeoutException)
                // если приехал STE, то скажем, что это запрос плохой, это не то же самое, что SOCKET_TIMEOUT от кликхауса
                // хотя это также может значить падающий кликхаус, посмотрим что выглядит правдоподобнее
                return new CHException(ClickhouseErrorCode.TIMEOUT_EXCEEDED.code, cause, host, port);
            else if (cause instanceof ConnectTimeoutException || cause instanceof ConnectException)
                // не смогли соединиться с кликхаусом за connectTimeout - в принципе, может быть никто не виноват
                // среди наших сущностей (query/api/db), но обвинить кого-то надо, и это будет db
                return new CHException(ClickhouseErrorCode.NETWORK_ERROR.code, cause, host, port);
            else
                return new ClickhouseUnhandledException(cause, host, port);
        }
        try {
            int code;
            if(clickhouseMessage.startsWith("Poco::Exception. Code: 1000, ")) {
                code = 1000;
            } else {
                // быстро и опасно  Code: 175, e.displayText() = DB::Exception:
                code = Integer.parseInt(clickhouseMessage.substring(clickhouseMessage.indexOf(' ') + 1, clickhouseMessage.indexOf(',')));
            }
            // ошибку в изначальном виде все-таки укажем
            Throwable messageHolder = cause != null ? cause : new Throwable(clickhouseMessage);
            return new CHException(code, messageHolder, host, port);
        } catch (Exception e) {
            log.error("Unsupported clickhouse error format, please fix ClickhouseExceptionSpecifier, message: "
                            + clickhouseMessage + ", error: " + e.getMessage());
            return new ClickhouseUnhandledException(clickhouseMessage, cause, host, port);
        }
    }

    private interface ClickhouseExceptionFactory {
        CHException create(Integer code, Throwable cause, String host, int port);
    }

}