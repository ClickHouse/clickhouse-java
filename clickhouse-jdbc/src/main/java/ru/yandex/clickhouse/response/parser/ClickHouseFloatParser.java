package ru.yandex.clickhouse.response.parser;

import java.sql.SQLException;
import java.util.TimeZone;

import ru.yandex.clickhouse.except.ClickHouseUnknownException;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseFloatParser extends ClickHouseValueParser<Float> {

    private static ClickHouseFloatParser instance;

    static ClickHouseFloatParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseFloatParser();
        }
        return instance;
    }

    private ClickHouseFloatParser() {
        // prevent instantiation
    }

    @Override
    public Float parse(ByteFragment value, ClickHouseColumnInfo columnInfo,
        TimeZone resultTimeZone) throws SQLException
    {
        if (value.isNull()) {
            return null;
        }
        if (value.isNaN()) {
            return Float.valueOf(Float.NaN);
        }
        String s = value.asString();
        switch (s) {
            case "+inf":
            case "inf":
                return Float.valueOf(Float.POSITIVE_INFINITY);
            case "-inf":
                return Float.valueOf(Float.NEGATIVE_INFINITY);
            default:
                try {
                    return Float.valueOf(s);
                } catch (NumberFormatException nfe) {
                    throw new ClickHouseUnknownException(
                        "Error parsing '" + s + "' as Float",
                        nfe);
                }
        }
    }

    @Override
    protected Float getDefaultValue() {
        return Float.valueOf(0);
    }

}
