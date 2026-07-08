package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClickHouseException;
import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseValues;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.List;
import java.util.Map;

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
            case Point:
            case Ring:
            case LineString:
            case Polygon:
            case MultiLineString:
            case MultiPolygon:
            case Geometry:
                return geoToString(value, column);
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
            sb.append(new String((byte[]) bytesOrString));
        } else {
            sb.append(bytesOrString);
        }
        if (column.isArray()) {
            sb.append(QUOTE);
        }
        return sb.toString();
    }

    public static ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    public static final ZonedDateTime EPOCH_START_OF_THE_DAY =
            ZonedDateTime.ofInstant(Instant.EPOCH, UTC_ZONE_ID);

    public static final LocalDate EPOCH_DATE = LocalDate.of(1970, 1, 1);

    public String dateToString(Object value, ClickHouseColumn column) {
        DateTimeFormatter formatter = DataTypeUtils.DATE_FORMATTER;

        if (value instanceof ZonedDateTime || value instanceof LocalDateTime) {
            TemporalAccessor dateTime = (TemporalAccessor) value;
            return formatter.format(dateTime);
        } else if (value instanceof LocalDate) {
            return formatter.format(((LocalDate)value));
        } else if (value instanceof java.sql.Date) {
            java.sql.Date date = (java.sql.Date) value;
            return formatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), UTC_ZONE_ID));
        } else if (value instanceof  java.sql.Time) {
            return formatter.format(EPOCH_START_OF_THE_DAY);
        } else if (value instanceof Date) {
            return formatter.format(((Date)value).toInstant().atZone(UTC_ZONE_ID));
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
        } else if (value instanceof java.sql.Date) {
            return formatter.format(EPOCH_START_OF_THE_DAY);
        } else if (value instanceof java.sql.Time) {
            java.sql.Time time = (java.sql.Time) value;
            LocalTime lt = time.toLocalTime();
            return formatter.format(lt);
        } else if (value instanceof Date) {
            return formatter.format(((Date)value).toInstant().atZone(UTC_ZONE_ID));
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
            return formatter.format(((LocalTime) value).atDate(LocalDate.now()));
        } else if (value instanceof java.sql.Date) {
            return formatter.format(EPOCH_START_OF_THE_DAY);

        } else if (value instanceof java.sql.Time) {
            java.sql.Time time = (java.sql.Time) value;
            LocalTime lt = time.toLocalTime();
            return formatter.format(lt.atDate(EPOCH_DATE));
        } else if (value instanceof Date) {
            return formatter.format(((Date)value).toInstant().atZone(UTC_ZONE_ID));
        }
        return value.toString();
    }

    public String enumToString(Object value, ClickHouseColumn column) {
        if (value instanceof BinaryStreamReader.EnumValue) {
            return ((BinaryStreamReader.EnumValue)value).name;
        } else if (value instanceof Number ) {
            int num = ((Number) value).intValue();
            switch (column.getDataType()) {
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

    public String arrayToString(Object value, String columnDef) {
        ClickHouseColumn column = ClickHouseColumn.of("v", columnDef);
        return arrayToString(value, column);
    }

    /**
     * Converts a query-parameter value into the text form expected by ClickHouse's HTTP
     * {@code param_<name>} interface, allowing callers to pass raw Java values - including
     * {@link Collection}, array (object or primitive) and {@link Map} values - for
     * {@code Array}/{@code Map} placeholders without pre-formatting them.
     *
     * <p>A top-level scalar is returned in its bare, unquoted text form, which is what the server
     * expects for a scalar {@code {name:Type}} placeholder (e.g. a {@code Date} is sent as
     * {@code 2026-05-13}, not {@code '2026-05-13'}). A container is rendered as a ClickHouse
     * {@code Array} ({@code [..]}) or {@code Map} ({@code {..}}) text literal in which
     * {@code String}/temporal leaves are single-quoted (and escaped) while numeric/boolean leaves
     * are left unquoted, as required by the server's array/map text parser.</p>
     *
     * @param value parameter value, may be {@code null}
     * @return the formatted {@code param_<name>} value
     */
    public String convertParameterToString(Object value) {
        if (isParameterContainer(value)) {
            return convertParameterContainer(value);
        }
        // Scalars (and null) are passed through unquoted: the server reads a scalar parameter value
        // verbatim, so quoting it here would break parsing (e.g. Date, numbers, Identifier).
        return String.valueOf(value);
    }

    private boolean isParameterContainer(Object value) {
        return value instanceof Collection || value instanceof Map
                || (value != null && value.getClass().isArray());
    }

    /**
     * Renders a {@code Collection}/array/{@code Map} parameter value into ClickHouse
     * {@code Array} ({@code [e1,e2,...]}) or {@code Map} ({@code {k:v,...}}) text using an explicit
     * work stack rather than recursion, so that arbitrarily deep nesting cannot overflow the call
     * stack. Work items are pushed in reverse so they pop in left-to-right output order: a container
     * expands into its punctuation and child values, while a leaf is formatted with
     * {@link ClickHouseValues#convertToSqlExpression(Object)} (String/temporal single-quoted,
     * numeric/boolean unquoted, {@code null} -> {@code NULL}) - exactly what the server's array/map
     * text parser expects for nested elements.
     */
    private String convertParameterContainer(Object value) {
        StringBuilder sb = new StringBuilder();
        Deque<Object> stack = new ArrayDeque<>();
        stack.push(value);
        while (!stack.isEmpty()) {
            Object item = stack.pop();
            if (item instanceof Literal) {
                sb.append(((Literal) item).text);
            } else if (isParameterContainer(item)) {
                pushContainer(stack, item);
            } else {
                sb.append(ClickHouseValues.convertToSqlExpression(item));
            }
        }
        return sb.toString();
    }

    /** Expands one container onto the work {@code stack}, pushed in reverse so it pops in output order. */
    private void pushContainer(Deque<Object> stack, Object container) {
        if (container instanceof Map) {
            stack.push(MAP_CLOSE);
            Object[] entries = ((Map<?, ?>) container).entrySet().toArray();
            for (int i = entries.length - 1; i >= 0; i--) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) entries[i];
                pushValue(stack, entry.getValue());
                stack.push(COLON);
                pushValue(stack, entry.getKey());
                if (i > 0) {
                    stack.push(COMMA);
                }
            }
            stack.push(MAP_OPEN);
        } else if (container instanceof Collection) {
            stack.push(ARRAY_CLOSE);
            Object[] elements = ((Collection<?>) container).toArray();
            for (int i = elements.length - 1; i >= 0; i--) {
                pushValue(stack, elements[i]);
                if (i > 0) {
                    stack.push(COMMA);
                }
            }
            stack.push(ARRAY_OPEN);
        } else {
            // Reflection handles both Object[] and primitive arrays (int[], long[], ...);
            // Array.get autoboxes primitive elements so they render as unquoted numbers/booleans.
            stack.push(ARRAY_CLOSE);
            for (int i = Array.getLength(container) - 1; i >= 0; i--) {
                pushValue(stack, Array.get(container, i));
                if (i > 0) {
                    stack.push(COMMA);
                }
            }
            stack.push(ARRAY_OPEN);
        }
    }

    /**
     * Pushes a child value onto the work {@code stack}, substituting {@link #NULL_LITERAL} for a
     * {@code null} element because {@link ArrayDeque} does not permit {@code null} entries.
     */
    private static void pushValue(Deque<Object> stack, Object value) {
        stack.push(value == null ? NULL_LITERAL : value);
    }

    /** A pre-rendered literal on the work stack: structural punctuation, or the {@code null} leaf. */
    private static final class Literal {
        final String text;

        Literal(String text) {
            this.text = text;
        }
    }

    private static final Literal ARRAY_OPEN = new Literal("[");
    private static final Literal ARRAY_CLOSE = new Literal("]");
    private static final Literal MAP_OPEN = new Literal("{");
    private static final Literal MAP_CLOSE = new Literal("}");
    private static final Literal COMMA = new Literal(",");
    private static final Literal COLON = new Literal(":");
    private static final Literal NULL_LITERAL = new Literal(ClickHouseValues.convertToSqlExpression(null));

    public String geoToString(Object value, ClickHouseColumn column) {
        String geoValue = tryGeoToString(value, column);
        return geoValue != null ? geoValue : value.toString();
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
        String geoValue = tryGeoToString(value, column);
        if (geoValue != null) {
            return geoValue;
        }
        if (value.getClass().isArray()) {
            return arrayToString(value, column);
        }
        return value.toString();
    }

    private String tryGeoToString(Object value, ClickHouseColumn column) {
        if (value == null || !value.getClass().isArray()) {
            return null;
        }

        int dimensions = getArrayDimensions(value);
        if (dimensions < 1 || dimensions > 4 || !isGeoShape(value, dimensions)) {
            return null;
        }

        ClickHouseDataType dataType = column.getDataType();
        if (isGeoType(dataType)) {
            return geoArrayToString(value);
        }

        if (dataType == ClickHouseDataType.Variant && matchesGeoVariant(column, dimensions)) {
            return geoArrayToString(value);
        }

        if (dataType == ClickHouseDataType.Dynamic) {
            return geoArrayToString(value);
        }

        return null;
    }

    private boolean matchesGeoVariant(ClickHouseColumn column, int dimensions) {
        for (ClickHouseColumn nestedColumn : column.getNestedColumns()) {
            if (isGeoTypeForDimensions(nestedColumn.getDataType(), dimensions)) {
                return true;
            }
        }
        return false;
    }

    private boolean isGeoType(ClickHouseDataType dataType) {
        switch (dataType) {
            case Point:
            case Ring:
            case LineString:
            case Polygon:
            case MultiLineString:
            case MultiPolygon:
            case Geometry:
                return true;
            default:
                return false;
        }
    }

    private boolean isGeoTypeForDimensions(ClickHouseDataType dataType, int dimensions) {
        switch (dimensions) {
            case 1:
                return dataType == ClickHouseDataType.Point;
            case 2:
                return dataType == ClickHouseDataType.Ring || dataType == ClickHouseDataType.LineString;
            case 3:
                return dataType == ClickHouseDataType.Polygon || dataType == ClickHouseDataType.MultiLineString;
            case 4:
                return dataType == ClickHouseDataType.MultiPolygon;
            default:
                return false;
        }
    }

    private int getArrayDimensions(Object value) {
        int dimensions = 0;
        Class<?> arrayClass = value.getClass();
        while (arrayClass.isArray()) {
            dimensions++;
            arrayClass = arrayClass.getComponentType();
        }
        return dimensions;
    }

    private boolean isGeoShape(Object value, int dimensions) {
        if (dimensions == 1) {
            return Array.getLength(value) == 2
                    && Array.get(value, 0) instanceof Double
                    && Array.get(value, 1) instanceof Double;
        }

        for (int i = 0, len = Array.getLength(value); i < len; i++) {
            Object item = Array.get(value, i);
            if (item == null || !item.getClass().isArray() || !isGeoShape(item, dimensions - 1)) {
                return false;
            }
        }
        return true;
    }

    private String geoArrayToString(Object value) {
        int dimensions = getArrayDimensions(value);
        if (dimensions == 1) {
            return new StringBuilder()
                    .append('(')
                    .append(Array.get(value, 0))
                    .append(',')
                    .append(Array.get(value, 1))
                    .append(')')
                    .toString();
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (int i = 0, len = Array.getLength(value); i < len; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(geoArrayToString(Array.get(value, i)));
        }
        return builder.append(']').toString();
    }

    private static void appendEnquotedArrayElement(String value, ClickHouseColumn elementColumn, Appendable appendable) {
        try {
            if (elementColumn != null && elementColumn.getDataType() == ClickHouseDataType.String) {
                appendable.append(QUOTE).append(value).append(QUOTE);
            } else {
                appendable.append(value);
            }
        } catch (IOException e) {
            throw new ClickHouseException(e.getMessage(), e);
        }
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
            ClickHouseColumn elementColumn = column.getArrayBaseColumn() == null ? column : column.getArrayBaseColumn();
            String str = DataTypeConverter.this.convertToString(item, elementColumn);
            appendEnquotedArrayElement(str, elementColumn, appendable);
        }

        public String convertAndReset(Object list, Appendable acc, ClickHouseColumn column) {
            // Save current state to handle re-entrancy (nested array conversion)
            ClickHouseColumn prevColumn = this.column;
            Appendable prevAppendable = this.appendable;
            try {
                setColumn(column);
                return super.convert(list, acc);
            } finally {
                // Restore previous state for re-entrant calls
                this.column = prevColumn;
                setAccumulator(prevAppendable);
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
            ClickHouseColumn elementColumn = column.getArrayBaseColumn() == null ? column : column.getArrayBaseColumn();
            String str = DataTypeConverter.this.convertToString(item, elementColumn);
            appendEnquotedArrayElement(str, elementColumn, appendable);

        }

        public String convertAndReset(List<?> list, Appendable acc, ClickHouseColumn column) {
            // Save current state to handle re-entrancy (nested array conversion)
            ClickHouseColumn prevColumn = this.column;
            Appendable prevAppendable = this.appendable;
            try {
                setColumn(column);
                return super.convert(list, acc);
            } finally {
                // Restore previous state for re-entrant calls
                this.column = prevColumn;
                setAccumulator(prevAppendable);
            }
        }
    }
}
