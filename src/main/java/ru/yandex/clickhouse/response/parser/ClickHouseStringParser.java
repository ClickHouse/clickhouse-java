package ru.yandex.clickhouse.response.parser;

import java.sql.SQLException;
import java.util.TimeZone;

import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseStringParser extends ClickHouseValueParser<String> {

    private static ClickHouseStringParser instance;

    static ClickHouseStringParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseStringParser();
        }
        return instance;
    }

    private ClickHouseStringParser() {
        // prevent regular instantiation
    }

    @Override
    public String parse(ByteFragment value, ClickHouseColumnInfo columnInfo,
        TimeZone resultTimeZone) throws SQLException
    {
        return value.asString(true);
    }

}
