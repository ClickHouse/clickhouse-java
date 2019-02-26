package ru.yandex.clickhouse.util;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public enum ClickHouseFormat {

    TabSeparatedWithNamesAndTypes("TabSeparatedWithNamesAndTypes"),
    TabSeparated("TabSeparated"),
    RowBinary("RowBinary"),
    JSONCompact("JSONCompact"),
    Native("Native"),
    CSVWithNames("CSVWithNames");

    public final String name;

    ClickHouseFormat(String name) {
        this.name = name;
    }

}
