package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClickHouseException;
import com.clickhouse.client.api.DataTypeUtils;
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
import java.util.Date;
import java.util.List;

/**
 * Class designed to convert different data types to Java objects.
 * First use-case is to convert ClickHouse data types to String representation.
 * Note: class is not thread-safe to avoid extra object creation.
 */
public class DataTypeConverter {

    private static final char QUOTE = '\'';

    private static final String NULL = "NULL";

    public static final DataTypeConverter INSTANCE = new DataTypeConverter();

    private final ListAsStringWriter listAsStringWriter = new ListAsStringWriter();

    private final ArrayAsStringWriter arrayAsStringWriter = new ArrayAsStringWriter();

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
        StringBuilder sb = new StringBuilder();
        if (column.isArray()) {
            sb.append(QUOTE);
        }
        if (bytesOrString instanceof CharSequence) {
            sb.append(((CharSequence) bytesOrString));
        } else if (bytesOrString instanceof byte[]) {
            sb.append(bytesOrString);
        } else {
            sb.append(bytesOrString);
        }
        if (column.isArray()) {
            sb.append(QUOTE);
        }
        return sb.toString();
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
            return listAsStringWriter.convertAndReset(((BinaryStreamReader.ArrayValue) value).asList(), new StringBuilder(), column);
        } else if (value.getClass().isArray()) {
            return arrayAsStringWriter.convertAndReset(value, new StringBuilder(), column);
        } else if (value instanceof List<?>) {
            return listAsStringWriter.convertAndReset((List<?>) value, new StringBuilder(), column);
        }
        return value.toString();
    }

    /**
     *
     * @param value not null object value to convert
     * @param column column describing the DB value
     * @return String representing the value
     */
    public String variantOrDynamicToString(Object value, ClickHouseColumn column) {
        if (value instanceof BinaryStreamReader.ArrayValue) {
            return arrayToString(value, column);
        }
        return value.toString();
    }

    private final class ArrayAsStringWriter extends BaseCollectionConverter.BaseArrayWriter {
        private ClickHouseColumn column;

        ArrayAsStringWriter() {
            super();
        }

        public void setColumn(ClickHouseColumn column) {
            this.column = column;
        }


        @Override
        protected void onItem(Object item, ListConversionState<Object> state) {
            if (item == null) {
                append(NULL);
                return;
            }
            String str = DataTypeConverter.this.convertToString(item, column.getArrayBaseColumn() == null ? column : column.getArrayBaseColumn());
            try {
                if (column.getArrayBaseColumn().getDataType() == ClickHouseDataType.String) {
                    appendable.append(QUOTE).append(str).append(QUOTE);
                } else {
                    appendable.append(str);
                }
            } catch (Exception ex) {
                throw new ClickHouseException(ex.getMessage(), ex);
            }
        }

        public String convertAndReset(Object list, Appendable acc, ClickHouseColumn column) {
            try {
                setColumn(column);
                return super.convert(list, acc);
            } finally {
                this.column = null;
                setAccumulator(null);
            }
        }
    }

    private final class ListAsStringWriter extends BaseCollectionConverter.BaseListWriter {

        private ClickHouseColumn column;

        public void setColumn(ClickHouseColumn column) {
            this.column = column;
        }

        @Override
        protected void onItem(Object item, ListConversionState<List<?>> state) {
            if (item == null) {
                append(NULL);
                return;
            }
            append(DataTypeConverter.this.convertToString(item, column.getArrayBaseColumn() == null ? column : column.getArrayBaseColumn()));
        }

        public String convertAndReset(List<?> list, Appendable acc, ClickHouseColumn column) {
            try {
                setColumn(column);
                return super.convert(list, acc);
            } finally {
                this.column = null;
                setAccumulator(null);
            }
        }
    }
}
