package ru.yandex.clickhouse.util;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public enum ClickHouseFormat {
    TabSeparated,
    TabSeparatedWithNamesAndTypes,
    JSONCompact,
    RowBinary,
    Native
}
