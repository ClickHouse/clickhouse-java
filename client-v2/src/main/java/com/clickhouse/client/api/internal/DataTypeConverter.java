package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;

import java.net.InetAddress;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Date;

public class DataTypeConverter {

    public static final DataTypeConverter INSTANCE = new DataTypeConverter();

    public String convertToString(Object value, ClickHouseColumn column) {
        if (value == null) {
            return null;
        }

        switch (column.getDataType()) {
            case String:
                return stringToString(value, column);
            case Date:
            case Date32:
                return dateToString(value, column);
            case Time:
            case Time64:
                return timeToString(value, column);
            case DateTime:
            case DateTime32:
            case DateTime64:
                return dateTimeToString(value, column);
            case Enum8:
            case Enum16:
            case Enum:
                return enumToString(value, column);
            case IPv4:
            case IPv6:
                return ipvToString(value, column);
            case Array:
                return  arrayToString(value, column);
            case Variant:
            case Dynamic:
                return variantOrDynamicToString(value, column);
            default:
                return value.toString();
        }
    }

    public String stringToString(Object bytesOrString, ClickHouseColumn column) {
        return bytesOrString instanceof byte[] ? new String((byte[]) bytesOrString) : (String) bytesOrString;
    }

    public String dateToString(Object value, ClickHouseColumn column) {
        DateTimeFormatter formatter = DataTypeUtils.DATE_FORMATTER;

        if (value instanceof ZonedDateTime || value instanceof LocalDateTime) {
            TemporalAccessor dateTime = (TemporalAccessor) value;
            return formatter.format(dateTime);
        } else if (value instanceof LocalDate) {
            return formatter.format(((LocalDate)value));
        } else if (value instanceof Date) {
            return DataTypeUtils.OLD_DATE_FORMATTER.format(((Date)value));
        }
        return value.toString();
    }

    public String timeToString(Object value, ClickHouseColumn column) {
        DateTimeFormatter formatter;
        switch (column.getDataType()) {
            case Time64:
                formatter = DataTypeUtils.TIME_WITH_NANOS_FORMATTER;
                break;
            default:
                formatter = DataTypeUtils.TIME_FORMATTER;
        }

        if (value instanceof ZonedDateTime || value instanceof LocalDateTime) {
            TemporalAccessor dateTime = (TemporalAccessor) value;
            return formatter.format(dateTime);
        } else if (value instanceof LocalTime) {
            return formatter.format(((LocalTime)value));
        } else if (value instanceof Date) {
            return DataTypeUtils.OLD_TIME_FORMATTER.format(((Date)value));
        }
        return value.toString();
    }

    public String dateTimeToString(Object value, ClickHouseColumn column) {
        DateTimeFormatter formatter;
        switch (column.getDataType()) {
            case DateTime64:
                formatter = DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER;
                break;
            default:
                formatter = DataTypeUtils.DATETIME_FORMATTER;
        }

        if (value instanceof ZonedDateTime || value instanceof LocalDateTime) {
            TemporalAccessor dateTime = (TemporalAccessor) value;
            return formatter.format(dateTime);
        } else if (value instanceof LocalDate) {
            return formatter.format(((LocalDate)value).atStartOfDay());
        } else if (value instanceof LocalTime) {
            return formatter.format(((LocalTime)value).atDate(LocalDate.now()));
        } else if (value instanceof Date) {
            return DataTypeUtils.OLD_DATE_TIME_FORMATTER.format(((Date)value));
        }
        return value.toString();
    }

    public String enumToString(Object value, ClickHouseColumn column) {
        if (value instanceof BinaryStreamReader.EnumValue) {
            return ((BinaryStreamReader.EnumValue)value).name;
        } else if (value instanceof Number ) {
            int num = ((Number) value).intValue();
            switch (column.getDataType()) {
                case Variant:
                    for (ClickHouseColumn c : column.getNestedColumns()) {
                        // TODO: will work only if single enum listed as variant
                        if (c.getDataType() == ClickHouseDataType.Enum8 || c.getDataType() == ClickHouseDataType.Enum16) {
                            return c.getEnumConstants().name(num);
                        }
                    }
                    return String.valueOf(num); // fail-safe
                case Enum8:
                case Enum16:
                case Enum:
                    return column.getEnumConstants().name(num);
            }
        }
        return value.toString();
    }

    public String ipvToString(Object value, ClickHouseColumn column) {
        if (value instanceof InetAddress) {
            return ((InetAddress) value).getHostAddress();
        }
        return value.toString();
    }

    public String arrayToString(Object value, ClickHouseColumn column) {
        if (value instanceof BinaryStreamReader.ArrayValue) {
            return ((BinaryStreamReader.ArrayValue) value).asList().toString();
        } else if (value instanceof byte[]) {
            return Arrays.toString((byte[]) value);
        } else if (value instanceof short[]) {
            return Arrays.toString((short[]) value);
        } else if (value instanceof int[]) {
            return Arrays.toString((int[]) value);
        } else if (value instanceof long[]) {
            return Arrays.toString((long[]) value);
        } else if (value instanceof float[]) {
            return Arrays.toString((float[]) value);
        } else if (value instanceof double[]) {
            return Arrays.toString((double[]) value);
        } else if (value instanceof boolean[]) {
            return Arrays.toString((boolean[]) value);
        } else if (value instanceof char[]) {
            return Arrays.toString((char[]) value);
        } else if (value instanceof Object[]) {
            return Arrays.deepToString((Object[]) value);
        }
        return value.toString();
    }

    public String variantOrDynamicToString(Object value, ClickHouseColumn column) {
        if (value instanceof BinaryStreamReader.ArrayValue) {
            return arrayToString(value, column);
        }
        return value.toString();
    }
}
