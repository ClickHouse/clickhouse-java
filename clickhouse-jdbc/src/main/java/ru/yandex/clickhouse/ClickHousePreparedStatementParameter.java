package ru.yandex.clickhouse;

import ru.yandex.clickhouse.util.ClickHouseValueFormatter;

import java.util.TimeZone;

public final class ClickHousePreparedStatementParameter {

    private static final String EMPTY_STR = "";

    private static final ClickHousePreparedStatementParameter NULL_PARAM =
        new ClickHousePreparedStatementParameter(null, false);

    private static final ClickHousePreparedStatementParameter TRUE_PARAM =
            new ClickHousePreparedStatementParameter("1", false);

    private static final ClickHousePreparedStatementParameter FALSE_PARAM =
            new ClickHousePreparedStatementParameter("0", false);

    private final String stringValue;
    private final boolean quoteNeeded;

    public static ClickHousePreparedStatementParameter fromObject(Object x,
        TimeZone dateTimeZone, TimeZone dateTimeTimeZone)
    {
        if (x == null) {
            return NULL_PARAM;
        }
        return new ClickHousePreparedStatementParameter(
            ClickHouseValueFormatter.formatObject(x, dateTimeZone, dateTimeTimeZone),
            ClickHouseValueFormatter.needsQuoting(x));
    }

    public static ClickHousePreparedStatementParameter nullParameter() {
        return NULL_PARAM;
    }

    public static ClickHousePreparedStatementParameter boolParameter(boolean value) {
        return value ? TRUE_PARAM : FALSE_PARAM;
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

    String getRegularParam(int paramIndex) {
        if (quoteNeeded && EMPTY_STR.equals(getBatchValue())) {
            return "''";
        } else {
            if (ClickHouseValueFormatter.NULL_MARKER.equals(stringValue)) {
                return "null";
            } else {
                if (quoteNeeded) {
                    return "{param" + paramIndex + ":String}";
                } else {
                    if (stringValue.contains(".")) {
                        return "{param" + paramIndex + ":Float64}";
                    } else {
                        return "{param" + paramIndex + ":Int64}";
                    }
                }
            }
        }
    }

    String getBatchValue() {
        return stringValue;
    }


    @Override
    public String toString() {
        return stringValue;
    }

}