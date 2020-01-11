package ru.yandex.clickhouse.response.parser;

import java.sql.SQLException;
import java.util.TimeZone;

import ru.yandex.clickhouse.except.ClickHouseUnknownException;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseDoubleParser extends ClickHouseValueParser<Double> {

    private static ClickHouseDoubleParser instance;

    static ClickHouseDoubleParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseDoubleParser();
        }
        return instance;
    }

    private ClickHouseDoubleParser() {
        // prevent instantiation
    }

    @Override
    public Double parse(ByteFragment value, ClickHouseColumnInfo columnInfo,
        TimeZone resultTimeZone) throws SQLException
    {
        if (value.isNull()) {
            return null;
        }
        if (value.isNaN()) {
            return Double.valueOf(Double.NaN);
        }
        String s = value.asString();
        switch (s) {
            case "+inf":
            case "inf":
                return Double.valueOf(Double.POSITIVE_INFINITY);
            case "-inf":
                return Double.valueOf(Double.NEGATIVE_INFINITY);
            default:
                try {
                    return Double.valueOf(s);
                } catch (NumberFormatException nfe) {
                    throw new ClickHouseUnknownException(
                        "Error parsing '" + s + "' as Double",
                        nfe);
                }
        }
    }

    @Override
    protected Double getDefaultValue() {
        return Double.valueOf(0);
    }

}
