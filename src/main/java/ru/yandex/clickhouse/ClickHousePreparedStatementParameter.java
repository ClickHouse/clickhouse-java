package ru.yandex.clickhouse;

import java.util.TimeZone;

import ru.yandex.clickhouse.util.ClickHouseValueFormatter;

public final class ClickHousePreparedStatementParameter {

    private final String stringValue;
    private final boolean quoteNeeded;

    public static ClickHousePreparedStatementParameter fromObject(Object x,
        TimeZone dateTimeZone, TimeZone dateTimeTimeZone)
    {
        if (x == null) {
            return new ClickHousePreparedStatementParameter(null, false);
        }
        return new ClickHousePreparedStatementParameter(
            ClickHouseValueFormatter.formatObject(x, dateTimeZone, dateTimeTimeZone),
            ClickHouseValueFormatter.needsQuoting(x));
    }

    public ClickHousePreparedStatementParameter(String stringValue,
        boolean quoteNeeded)
    {
        this.stringValue = stringValue == null
            ? ClickHouseValueFormatter.NULL_MARKER
            : stringValue;
        this.quoteNeeded = quoteNeeded;
    }

    String getRegularValue() {
        return !ClickHouseValueFormatter.NULL_MARKER.equals(stringValue)
            ? quoteNeeded
                ? "'" + stringValue + "'"
                : stringValue
            : "null";
    }

    String getBatchValue() {
        return stringValue;
    }

    @Override
    public String toString() {
        return stringValue;
    }

}