package ru.yandex.clickhouse.util;

import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 11/05/2017
 */
public interface ClickHouseStreamCallback {
    void writeTo(ClickHouseRawBinaryStream stream) throws IOException;
}
