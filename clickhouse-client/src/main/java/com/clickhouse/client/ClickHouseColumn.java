package com.clickhouse.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;

/**
 * This class represents a column defined in database.
 */
public final class ClickHouseColumn implements Serializable {
    private static final long serialVersionUID = 8228660689532259640L;

    private static final String ERROR_MISSING_NESTED_TYPE = "Missing nested data type";
    private static final String KEYWORD_NULLABLE = "Nullable";
    private static final String KEYWORD_LOW_CARDINALITY = "LowCardinality";
    private static final String KEYWORD_AGGREGATE_FUNCTION = ClickHouseDataType.AggregateFunction.name();
    private static final String KEYWORD_SIMPLE_AGGREGATE_FUNCTION = ClickHouseDataType.SimpleAggregateFunction.name();
    private static final String KEYWORD_ARRAY = ClickHouseDataType.Array.name();
    private static final String KEYWORD_TUPLE = ClickHouseDataType.Tuple.name();
    private static final String KEYWORD_MAP = ClickHouseDataType.Map.name();
    private static final String KEYWORD_NESTED = ClickHouseDataType.Nested.name();

    private String originalTypeName;
    private String columnName;

    private ClickHouseAggregateFunction aggFuncType;
    private ClickHouseDataType dataType;
    private boolean nullable;
    private boolean lowCardinality;
    private TimeZone timeZone;
    private int precision;
    private int scale;
    private List<ClickHouseColumn> nested;
    private List<String> parameters;
    private ClickHouseEnum enumConstants;

    private int arrayLevel;
    private ClickHouseColumn arrayBaseColumn;

    private static ClickHouseColumn update(ClickHouseColumn column) {
        column.enumConstants = ClickHouseEnum.EMPTY;
        int size = column.parameters.size();
        column.precision = column.dataType.getMaxPrecision();
        switch (column.dataType) {
            case Array:
                column.arrayLevel = 1;
                column.arrayBaseColumn = column.nested.get(0);
                while (column.arrayLevel < 255) {
                    if (column.arrayBaseColumn.dataType == ClickHouseDataType.Array) {
                        column.arrayLevel++;
                        column.arrayBaseColumn = column.arrayBaseColumn.nested.get(0);
                    } else {
                        break;
                    }
                }
                break;
            case Enum:
            case Enum8:
            case Enum16:
                column.enumConstants = new ClickHouseEnum(column.parameters);
                break;
            case DateTime:
                if (size >= 2) { // same as DateTime64
                    column.scale = Integer.parseInt(column.parameters.get(0));
                    column.timeZone = TimeZone.getTimeZone(column.parameters.get(1).replace("'", ""));
                } else if (size == 1) { // same as DateTime32
                    // unfortunately this will fall back to GMT if the time zone
                    // cannot be resolved
                    TimeZone tz = TimeZone.getTimeZone(column.parameters.get(0).replace("'", ""));
                    column.timeZone = tz;
                }
                break;
            case DateTime32:
                if (size > 0) {
                    // unfortunately this will fall back to GMT if the time zone
                    // cannot be resolved
                    TimeZone tz = TimeZone.getTimeZone(column.parameters.get(0).replace("'", ""));
                    column.timeZone = tz;
                }
                break;
            case DateTime64:
                if (size > 0) {
                    column.scale = Integer.parseInt(column.parameters.get(0));
                }
                if (size > 1) {
                    column.timeZone = TimeZone.getTimeZone(column.parameters.get(1).replace("'", ""));
                }
                break;
            case Decimal:
                if (size >= 2) {
                    column.precision = Integer.parseInt(column.parameters.get(0));
                    column.scale = Integer.parseInt(column.parameters.get(1));
                }
                break;
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
                column.scale = Integer.parseInt(column.parameters.get(0));
                break;
            case FixedString:
                column.precision = Integer.parseInt(column.parameters.get(0));
                break;
            default:
                break;
        }

        return column;
    }

    protected static int readColumn(String args, int startIndex, int len, String name, List<ClickHouseColumn> list) {
        ClickHouseColumn column = null;

        StringBuilder builder = new StringBuilder();

        int brackets = 0;
        boolean nullable = false;
        boolean lowCardinality = false;
        int i = startIndex;

        if (args.startsWith(KEYWORD_LOW_CARDINALITY, i)) {
            lowCardinality = true;
            int index = args.indexOf('(', i + KEYWORD_LOW_CARDINALITY.length());
            if (index < i) {
                throw new IllegalArgumentException(ERROR_MISSING_NESTED_TYPE);
            }
            i = index + 1;
            brackets++;
        }
        if (args.startsWith(KEYWORD_NULLABLE, i)) {
            nullable = true;
            int index = args.indexOf('(', i + KEYWORD_NULLABLE.length());
            if (index < i) {
                throw new IllegalArgumentException(ERROR_MISSING_NESTED_TYPE);
            }
            i = index + 1;
            brackets++;
        }

        String matchedKeyword;
        if (args.startsWith((matchedKeyword = KEYWORD_AGGREGATE_FUNCTION), i)
                || args.startsWith((matchedKeyword = KEYWORD_SIMPLE_AGGREGATE_FUNCTION), i)) {
            int index = args.indexOf('(', i + matchedKeyword.length());
            if (index < i) {
                throw new IllegalArgumentException("Missing function parameters");
            }
            List<String> params = new LinkedList<>();
            i = ClickHouseUtils.readParameters(args, index, len, params);

            ClickHouseAggregateFunction aggFunc = null;
            boolean isFirst = true;
            List<ClickHouseColumn> nestedColumns = new LinkedList<>();
            for (String p : params) {
                if (isFirst) {
                    int pIndex = p.indexOf('(');
                    aggFunc = ClickHouseAggregateFunction.of(pIndex > 0 ? p.substring(0, pIndex) : p);
                    isFirst = false;
                } else {
                    nestedColumns.add(ClickHouseColumn.of("", p));
                }
            }
            column = new ClickHouseColumn(ClickHouseDataType.valueOf(matchedKeyword), name,
                    args.substring(startIndex, i), nullable, lowCardinality, params, nestedColumns);
            column.aggFuncType = aggFunc;
        } else if (args.startsWith(KEYWORD_ARRAY, i)) {
            int index = args.indexOf('(', i + KEYWORD_ARRAY.length());
            if (index < i) {
                throw new IllegalArgumentException(ERROR_MISSING_NESTED_TYPE);
            }
            int endIndex = ClickHouseUtils.skipBrackets(args, index, len, '(');
            List<ClickHouseColumn> nestedColumns = new LinkedList<>();
            readColumn(args, index + 1, endIndex - 1, "", nestedColumns);
            if (nestedColumns.size() != 1) {
                throw new IllegalArgumentException(
                        "Array can have one and only one nested column, but we got: " + nestedColumns.size());
            }
            column = new ClickHouseColumn(ClickHouseDataType.Array, name, args.substring(startIndex, endIndex),
                    nullable, lowCardinality, null, nestedColumns);
            i = endIndex;
        } else if (args.startsWith(KEYWORD_MAP, i)) {
            int index = args.indexOf('(', i + KEYWORD_MAP.length());
            if (index < i) {
                throw new IllegalArgumentException(ERROR_MISSING_NESTED_TYPE);
            }
            int endIndex = ClickHouseUtils.skipBrackets(args, index, len, '(');
            List<ClickHouseColumn> nestedColumns = new LinkedList<>();
            for (i = index + 1; i < endIndex; i++) {
                char c = args.charAt(i);
                if (c == ')') {
                    break;
                } else if (c != ',' && !Character.isWhitespace(c)) {
                    i = readColumn(args, i, endIndex, "", nestedColumns) - 1;
                }
            }
            if (nestedColumns.size() != 2) {
                throw new IllegalArgumentException(
                        "Map should have two nested columns(key and value), but we got: " + nestedColumns.size());
            }
            column = new ClickHouseColumn(ClickHouseDataType.Map, name, args.substring(startIndex, endIndex), nullable,
                    lowCardinality, null, nestedColumns);
            i = endIndex;
        } else if (args.startsWith(KEYWORD_NESTED, i)) {
            int index = args.indexOf('(', i + KEYWORD_NESTED.length());
            if (index < i) {
                throw new IllegalArgumentException(ERROR_MISSING_NESTED_TYPE);
            }
            i = ClickHouseUtils.skipBrackets(args, index, len, '(');
            String originalTypeName = args.substring(startIndex, i);
            List<ClickHouseColumn> nestedColumns = parse(args.substring(index + 1, i - 1));
            if (nestedColumns.isEmpty()) {
                throw new IllegalArgumentException("Nested should have at least one nested column");
            }
            column = new ClickHouseColumn(ClickHouseDataType.Nested, name, originalTypeName, nullable, lowCardinality,
                    null, nestedColumns);
        } else if (args.startsWith(KEYWORD_TUPLE, i)) {
            int index = args.indexOf('(', i + KEYWORD_TUPLE.length());
            if (index < i) {
                throw new IllegalArgumentException(ERROR_MISSING_NESTED_TYPE);
            }
            int endIndex = ClickHouseUtils.skipBrackets(args, index, len, '(');
            List<ClickHouseColumn> nestedColumns = new LinkedList<>();
            for (i = index + 1; i < endIndex; i++) {
                char c = args.charAt(i);
                if (c == ')') {
                    break;
                } else if (c != ',' && !Character.isWhitespace(c)) {
                    i = readColumn(args, i, endIndex, "", nestedColumns) - 1;
                }
            }
            if (nestedColumns.isEmpty()) {
                throw new IllegalArgumentException("Tuple should have at least one nested column");
            }
            column = new ClickHouseColumn(ClickHouseDataType.Tuple, name, args.substring(startIndex, endIndex),
                    nullable, lowCardinality, null, nestedColumns);
        }

        if (column == null) {
            i = ClickHouseUtils.readNameOrQuotedString(args, i, len, builder);
            List<String> params = new LinkedList<>();
            for (; i < len; i++) {
                char ch = args.charAt(i);
                if (ch == '(') {
                    i = ClickHouseUtils.readParameters(args, i, len, params) - 1;
                } else if (ch == ')') {
                    brackets--;
                    if (brackets <= 0) {
                        i++;
                        break;
                    }
                } else if (ch == ',') {
                    break;
                } else if (!Character.isWhitespace(ch)) {
                    StringBuilder sb = new StringBuilder();
                    i = ClickHouseUtils.readNameOrQuotedString(args, i, len, sb);
                    String modifier = sb.toString();
                    sb.setLength(0);
                    boolean startsWithNot = false;
                    if ("not".equalsIgnoreCase(modifier)) {
                        startsWithNot = true;
                        i = ClickHouseUtils.readNameOrQuotedString(args, i, len, sb);
                        modifier = sb.toString();
                        sb.setLength(0);
                    }

                    if ("null".equalsIgnoreCase(modifier)) {
                        if (nullable) {
                            throw new IllegalArgumentException("Nullable and NULL cannot be used together");
                        }
                        nullable = !startsWithNot;
                        i = ClickHouseUtils.skipContentsUntil(args, i, len, ',') - 1;
                        break;
                    } else if (startsWithNot) {
                        throw new IllegalArgumentException("Expect keyword NULL after NOT");
                    } else if ("alias".equalsIgnoreCase(modifier) || "codec".equalsIgnoreCase(modifier)
                            || "default".equalsIgnoreCase(modifier) || "materialized".equalsIgnoreCase(modifier)
                            || "ttl".equalsIgnoreCase(modifier)) { // stop words
                        i = ClickHouseUtils.skipContentsUntil(args, i, len, ',') - 1;
                        break;
                    } else {
                        builder.append(' ').append(modifier);
                        i--;
                    }
                }
            }
            column = new ClickHouseColumn(ClickHouseDataType.of(builder.toString()), name,
                    args.substring(startIndex, i), nullable, lowCardinality, params, null);
            builder.setLength(0);
        }

        list.add(update(column));

        return i;
    }

    public static ClickHouseColumn of(String columnName, ClickHouseDataType dataType, boolean nullable, int precision,
            int scale) {
        ClickHouseColumn column = new ClickHouseColumn(dataType, columnName, null, nullable, false, null, null);
        column.precision = precision;
        column.scale = scale;
        return column;
    }

    public static ClickHouseColumn of(String columnName, ClickHouseDataType dataType, boolean nullable,
            boolean lowCardinality, String... parameters) {
        return new ClickHouseColumn(dataType, columnName, null, nullable, lowCardinality, Arrays.asList(parameters),
                null);
    }

    public static ClickHouseColumn of(String columnName, ClickHouseDataType dataType, boolean nullable,
            ClickHouseColumn... nestedColumns) {
        return new ClickHouseColumn(dataType, columnName, null, nullable, false, null, Arrays.asList(nestedColumns));
    }

    public static ClickHouseColumn of(String columnName, String columnType) {
        if (columnName == null || columnType == null) {
            throw new IllegalArgumentException("Non-null columnName and columnType are required");
        }

        List<ClickHouseColumn> list = new ArrayList<>(1);
        readColumn(columnType, 0, columnType.length(), columnName, list);
        if (list.size() != 1) { // should not happen
            throw new IllegalArgumentException("Failed to parse given column");
        }
        return list.get(0);
    }

    public static List<ClickHouseColumn> parse(String args) {
        if (args == null || args.isEmpty()) {
            return Collections.emptyList();
        }

        String name = null;
        ClickHouseColumn column = null;
        List<ClickHouseColumn> list = new LinkedList<>();
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = args.length(); i < len; i++) {
            char ch = args.charAt(i);
            if (Character.isWhitespace(ch)) {
                continue;
            }

            if (name == null) { // column name
                i = ClickHouseUtils.readNameOrQuotedString(args, i, len, builder) - 1;
                name = builder.toString();
                builder.setLength(0);
            } else if (column == null) { // now type
                LinkedList<ClickHouseColumn> colList = new LinkedList<>();
                i = readColumn(args, i, len, name, colList) - 1;
                list.add(column = colList.getFirst());
            } else { // prepare for next column
                i = ClickHouseUtils.skipContentsUntil(args, i, len, ',') - 1;
                name = null;
                column = null;
            }
        }

        List<ClickHouseColumn> c = new ArrayList<>(list.size());
        for (ClickHouseColumn cc : list) {
            c.add(cc);
        }
        return Collections.unmodifiableList(c);
    }

    private ClickHouseColumn(String originalTypeName, String columnName) {
        this.originalTypeName = originalTypeName;
        this.columnName = columnName;
    }

    private ClickHouseColumn(ClickHouseDataType dataType, String columnName, String originalTypeName, boolean nullable,
            boolean lowCardinality, List<String> parameters, List<ClickHouseColumn> nestedColumns) {
        this.aggFuncType = null;
        this.dataType = ClickHouseChecker.nonNull(dataType, "dataType");

        this.columnName = columnName == null ? "" : columnName;
        this.originalTypeName = originalTypeName == null ? dataType.name() : originalTypeName;
        this.nullable = nullable;
        this.lowCardinality = lowCardinality;

        if (parameters == null || parameters.isEmpty()) {
            this.parameters = Collections.emptyList();
        } else {
            List<String> list = new ArrayList<>(parameters.size());
            list.addAll(parameters);
            this.parameters = Collections.unmodifiableList(list);
        }

        if (nestedColumns == null || nestedColumns.isEmpty()) {
            this.nested = Collections.emptyList();
        } else {
            List<ClickHouseColumn> list = new ArrayList<>(nestedColumns.size());
            list.addAll(nestedColumns);
            this.nested = Collections.unmodifiableList(list);
        }
    }

    public boolean isAggregateFunction() {
        // || dataType == ClickHouseDataType.SimpleAggregateFunction;
        return dataType == ClickHouseDataType.AggregateFunction;

    }

    public boolean isArray() {
        return dataType == ClickHouseDataType.Array;
    }

    public boolean isEnum() {
        return dataType == ClickHouseDataType.Enum || dataType == ClickHouseDataType.Enum8
                || dataType == ClickHouseDataType.Enum16;
    }

    public boolean isMap() {
        return dataType == ClickHouseDataType.Map;
    }

    public boolean isNested() {
        return dataType == ClickHouseDataType.Nested;
    }

    public boolean isTuple() {
        return dataType == ClickHouseDataType.Tuple;
    }

    public int getArrayNestedLevel() {
        return arrayLevel;
    }

    public ClickHouseColumn getArrayBaseColumn() {
        return arrayBaseColumn;
    }

    public ClickHouseDataType getDataType() {
        return dataType;
    }

    public ClickHouseEnum getEnumConstants() {
        return enumConstants;
    }

    public String getOriginalTypeName() {
        return originalTypeName;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isNullable() {
        return nullable;
    }

    boolean isLowCardinality() {
        return lowCardinality;
    }

    public TimeZone getTimeZone() {
        return timeZone; // null means server timezone
    }

    public TimeZone getTimeZoneOrDefault(TimeZone defaultTz) {
        return timeZone != null ? timeZone : defaultTz;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public boolean hasNestedColumn() {
        return !nested.isEmpty();
    }

    public List<ClickHouseColumn> getNestedColumns() {
        return nested;
    }

    public List<String> getParameters() {
        return parameters;
    }

    public ClickHouseColumn getKeyInfo() {
        return dataType == ClickHouseDataType.Map && nested.size() == 2 ? nested.get(0) : null;
    }

    public ClickHouseColumn getValueInfo() {
        return dataType == ClickHouseDataType.Map && nested.size() == 2 ? nested.get(1) : null;
    }

    /**
     * Gets function when column type is
     * {@link ClickHouseDataType#AggregateFunction}. So it will return
     * {@code quantiles(0.5, 0.9)} when the column type is
     * {@code AggregateFunction(quantiles(0.5, 0.9), UInt64)}.
     *
     * @return function, null when column type is not AggregateFunction
     */
    public String getFunction() {
        return dataType == ClickHouseDataType.AggregateFunction ? parameters.get(0) : null;
    }

    /**
     * Gets aggregate function when column type is
     * {@link ClickHouseDataType#AggregateFunction}. So it will return
     * {@link ClickHouseAggregateFunction#quantile} when the column type is
     * {@code AggregateFunction(quantiles(0.5, 0.9), UInt64)}.
     *
     * @return function name, null when column type is not AggregateFunction
     */
    public ClickHouseAggregateFunction getAggregateFunction() {
        return aggFuncType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((arrayBaseColumn == null) ? 0 : arrayBaseColumn.hashCode());
        result = prime * result + ((aggFuncType == null) ? 0 : aggFuncType.hashCode());
        result = prime * result + arrayLevel;
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
        result = prime * result + (lowCardinality ? 1231 : 1237);
        result = prime * result + ((nested == null) ? 0 : nested.hashCode());
        result = prime * result + (nullable ? 1231 : 1237);
        result = prime * result + ((originalTypeName == null) ? 0 : originalTypeName.hashCode());
        result = prime * result + ((parameters == null) ? 0 : parameters.hashCode());
        result = prime * result + precision;
        result = prime * result + scale;
        result = prime * result + ((timeZone == null) ? 0 : timeZone.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseColumn other = (ClickHouseColumn) obj;
        return Objects.equals(arrayBaseColumn, other.arrayBaseColumn) && aggFuncType == other.aggFuncType
                && arrayLevel == other.arrayLevel && Objects.equals(columnName, other.columnName)
                && dataType == other.dataType && lowCardinality == other.lowCardinality
                && Objects.equals(nested, other.nested) && nullable == other.nullable
                && Objects.equals(originalTypeName, other.originalTypeName)
                && Objects.equals(parameters, other.parameters) && precision == other.precision && scale == other.scale
                && Objects.equals(timeZone, other.timeZone);
    }

    @Override
    public String toString() {
        return new StringBuilder().append(columnName == null || columnName.isEmpty() ? "column" : columnName)
                .append(' ').append(originalTypeName).toString();
    }
}
