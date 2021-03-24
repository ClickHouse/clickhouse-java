package ru.yandex.clickhouse.response.parser;

import java.sql.Array;
import java.sql.SQLException;
import java.util.TimeZone;

import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;
import ru.yandex.clickhouse.util.ClickHouseArrayUtil;

final class ClickHouseArrayParser extends ClickHouseValueParser<Array> {

    private static ClickHouseArrayParser instance;

    static ClickHouseArrayParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseArrayParser();
        }
        return instance;
    }

    private ClickHouseArrayParser() {
        // prevent instantiation
    }

    @Override
    public Array parse(ByteFragment value, ClickHouseColumnInfo columnInfo, TimeZone resultTimeZone)
            throws SQLException {
        if (columnInfo.getClickHouseDataType() != ClickHouseDataType.Array) {
            throw new SQLException("Column not an array");
        }

        if (value.isNull()) {
            return null;
        }

        final Object array;
        switch (columnInfo.getArrayBaseType()) {
            case Date:
                // FIXME: properties.isUseObjectsInArrays()
                array = ClickHouseArrayUtil.parseArray(value, false, resultTimeZone, columnInfo);
                break;
            default:
                // properties.isUseObjectsInArrays()
                TimeZone timeZone = columnInfo.getTimeZone() != null ? columnInfo.getTimeZone() : resultTimeZone;
                array = ClickHouseArrayUtil.parseArray(value, false, timeZone, columnInfo);
                break;
        }

        return new ClickHouseArray(columnInfo.getArrayBaseType(), array);
    }

    @Override
    protected Array getDefaultValue() {
        return null;
    }
}
