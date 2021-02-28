package ru.yandex.clickhouse.util;

import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public interface ClickHouseStreamCallback {
    void writeTo(ClickHouseRowBinaryStream stream) throws IOException;
}
