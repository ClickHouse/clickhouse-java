package com.clickhouse.data;

import com.clickhouse.data.value.ClickHouseArrayValue;
import com.clickhouse.data.value.ClickHouseBigDecimalValue;
import com.clickhouse.data.value.ClickHouseBigIntegerValue;
import com.clickhouse.data.value.ClickHouseBitmapValue;
import com.clickhouse.data.value.ClickHouseBoolValue;
import com.clickhouse.data.value.ClickHouseByteValue;
import com.clickhouse.data.value.ClickHouseDateTimeValue;
import com.clickhouse.data.value.ClickHouseDateValue;
import com.clickhouse.data.value.ClickHouseDoubleValue;
import com.clickhouse.data.value.ClickHouseEmptyValue;
import com.clickhouse.data.value.ClickHouseEnumValue;
import com.clickhouse.data.value.ClickHouseFloatValue;
import com.clickhouse.data.value.ClickHouseGeoMultiPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoPointValue;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoRingValue;
import com.clickhouse.data.value.ClickHouseIntegerValue;
import com.clickhouse.data.value.ClickHouseIpv4Value;
import com.clickhouse.data.value.ClickHouseIpv6Value;
import com.clickhouse.data.value.ClickHouseLongValue;
import com.clickhouse.data.value.ClickHouseMapValue;
import com.clickhouse.data.value.ClickHouseNestedValue;
import com.clickhouse.data.value.ClickHouseOffsetDateTimeValue;
import com.clickhouse.data.value.ClickHouseShortValue;
import com.clickhouse.data.value.ClickHouseStringValue;
import com.clickhouse.data.value.ClickHouseTupleValue;
import com.clickhouse.data.value.ClickHouseUuidValue;
import com.clickhouse.data.value.array.ClickHouseBoolArrayValue;
import com.clickhouse.data.value.array.ClickHouseByteArrayValue;
import com.clickhouse.data.value.array.ClickHouseDoubleArrayValue;
import com.clickhouse.data.value.array.ClickHouseFloatArrayValue;
import com.clickhouse.data.value.array.ClickHouseIntArrayValue;
import com.clickhouse.data.value.array.ClickHouseLongArrayValue;
import com.clickhouse.data.value.array.ClickHouseShortArrayValue;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

/**
 * This class represents a column defined in database.
 */
public final class ClickHouseColumn implements Serializable {
    public static final String TYPE_NAME = "Column";
    public static final ClickHouseColumn[] EMPTY_ARRAY = new ClickHouseColumn[0];

    private static final long serialVersionUID = 8228660689532259640L;

    private static final String ERROR_MISSING_NESTED_TYPE = "Missing nested data type";
    private static final String KEYWORD_NULLABLE = "Nullable";
    private static final String KEYWORD_LOW_CARDINALITY = "LowCardinality";
    private static final String KEYWORD_AGGREGATE_FUNCTION = ClickHouseDataType.AggregateFunction.name();
    private static final String KEYWORD_SIMPLE_AGGREGATE_FUNCTION = ClickHouseDataType.SimpleAggregateFunction.name();
    private static final String KEYWORD_ARRAY = ClickHouseDataType.Array.name();
    private static final String KEYWORD_TUPLE = ClickHouseDataType.Tuple.name();
    private static final String KEYWORD_OBJECT = ClickHouseDataType.Object.name();
    private static final String KEYWORD_MAP = ClickHouseDataType.Map.name();
    private static final String KEYWORD_NESTED = ClickHouseDataType.Nested.name();
    private static final String KEYWORD_VARIANT = ClickHouseDataType.Variant.name();

    private int columnCount;
    private int columnIndex;
    private String columnName;
    private String originalTypeName;

    private ClickHouseAggregateFunction aggFuncType;
    private ClickHouseDataType dataType;
    private boolean nullable;
    private boolean hasDefault;
    private boolean lowCardinality;
    private boolean lowCardinalityDisabled;
    private TimeZone timeZone;
    private int precision;
    private int scale;
    private List<ClickHouseColumn> nested;
    private List<String> parameters;
    private ClickHouseEnum enumConstants;

    private int arrayLevel;
    private ClickHouseColumn arrayBaseColumn;

    private boolean fixedByteLength;
    private int estimatedByteLength;

    private ClickHouseValue template;

    private Map<Class<?>, Integer> classToVariantOrdNumMap;

    private Map<Class<?>, Integer> arrayToVariantOrdNumMap;

    private Map<Class<?>, Integer> mapKeyToVariantOrdNumMap;
    private Map<Class<?>, Integer> mapValueToVariantOrdNumMap;


    private static ClickHouseColumn update(ClickHouseColumn column) {
        column.enumConstants = ClickHouseEnum.EMPTY;
        int size = column.parameters.size();
        column.precision = column.dataType.getMaxPrecision();
        switch (column.dataType) {
            case Array:
                if (!column.nested.isEmpty()) {
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
                }
                break;
            case Bool:
                column.template = ClickHouseBoolValue.ofNull();
                break;
            case Enum:
            case Enum8:
            case Enum16:
                column.template = ClickHouseEnumValue
                        .ofNull(column.enumConstants = new ClickHouseEnum(column.parameters));
                break;
            case Int8:
                column.template = ClickHouseByteValue.ofNull();
                break;
            case Int16:
                column.template = ClickHouseShortValue.ofNull();
                break;
            case Int32:
                column.template = ClickHouseIntegerValue.ofNull();
                break;
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case IntervalMicrosecond:
            case IntervalMillisecond:
            case IntervalNanosecond:
            case Int64:
                column.template = ClickHouseLongValue.ofNull(false);
                break;
            case UInt64:
                column.template = ClickHouseLongValue.ofUnsignedNull();
                break;
            case Int128:
            case UInt128:
            case Int256:
            case UInt256:
                column.template = ClickHouseBigIntegerValue.ofNull();
                break;
            case Float32:
                column.template = ClickHouseFloatValue.ofNull();
                break;
            case Float64:
                column.template = ClickHouseDoubleValue.ofNull();
                break;
            case Date:
            case Date32:
                column.template = ClickHouseDateValue.ofNull();
                break;
            case DateTime:
                if (size >= 2) { // same as DateTime64
                    if (!column.nullable) {
                        column.estimatedByteLength += ClickHouseDataType.DateTime64.getByteLength();
                    }
                    column.template = ClickHouseOffsetDateTimeValue.ofNull(
                            column.scale = Integer.parseInt(column.parameters.get(0)),
                            column.timeZone = TimeZone.getTimeZone(column.parameters.get(1).replace("'", "")));
                } else if (size == 1) { // same as DateTime32
                    if (!column.nullable) {
                        column.estimatedByteLength += ClickHouseDataType.DateTime32.getByteLength();
                    }
                    column.template = ClickHouseOffsetDateTimeValue.ofNull(
                            column.scale,
                            // unfortunately this will fall back to GMT if the time zone cannot be resolved
                            column.timeZone = TimeZone.getTimeZone(column.parameters.get(0).replace("'", "")));
                }
                break;
            case DateTime32:
                if (size > 0) {
                    column.template = ClickHouseOffsetDateTimeValue.ofNull(
                            column.scale,
                            // unfortunately this will fall back to GMT if the time zone cannot be resolved
                            column.timeZone = TimeZone.getTimeZone(column.parameters.get(0).replace("'", "")));
                }
                break;
            case DateTime64:
                if (size > 0) {
                    column.scale = Integer.parseInt(column.parameters.get(0));
                }
                if (size > 1) {
                    column.template = ClickHouseOffsetDateTimeValue.ofNull(
                            column.scale,
                            column.timeZone = TimeZone.getTimeZone(column.parameters.get(1).replace("'", "")));
                }
                break;
            case Decimal:
                if (size >= 2) {
                    column.precision = Integer.parseInt(column.parameters.get(0));
                    column.scale = Integer.parseInt(column.parameters.get(1));

                    if (!column.nullable) {
                        if (column.precision > ClickHouseDataType.Decimal128.getMaxScale()) {
                            column.estimatedByteLength += ClickHouseDataType.Decimal256.getByteLength();
                        } else if (column.precision > ClickHouseDataType.Decimal64.getMaxScale()) {
                            column.estimatedByteLength += ClickHouseDataType.Decimal128.getByteLength();
                        } else if (column.precision > ClickHouseDataType.Decimal32.getMaxScale()) {
                            column.estimatedByteLength += ClickHouseDataType.Decimal64.getByteLength();
                        } else {
                            column.estimatedByteLength += ClickHouseDataType.Decimal32.getByteLength();
                        }
                    }
                }
                column.template = ClickHouseBigDecimalValue.ofNull();
                break;
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
                if (size > 0) {
                    column.scale = Integer.parseInt(column.parameters.get(0));
                }
                column.template = ClickHouseBigDecimalValue.ofNull();
                break;
            case IPv4:
                column.template = ClickHouseIpv4Value.ofNull();
                break;
            case IPv6:
                column.template = ClickHouseIpv6Value.ofNull();
                break;
            case FixedString:
                if (size > 0) {
                    column.precision = Integer.parseInt(column.parameters.get(0));
                    if (!column.nullable) {
                        column.estimatedByteLength += column.precision;
                    }
                }
                column.template = ClickHouseStringValue.ofNull();
                break;
            case Object:
            case JSON:
            case String:
                column.fixedByteLength = false;
                if (!column.nullable) {
                    column.estimatedByteLength += 1;
                }
                column.template = ClickHouseStringValue.ofNull();
                break;
            case UUID:
                column.template = ClickHouseUuidValue.ofNull();
                break;
            case Point:
                column.template = ClickHouseGeoPointValue.ofOrigin();
                break;
            case Ring:
                column.template = ClickHouseGeoRingValue.ofEmpty();
                break;
            case Polygon:
                column.template = ClickHouseGeoPolygonValue.ofEmpty();
                break;
            case MultiPolygon:
                column.template = ClickHouseGeoMultiPolygonValue.ofEmpty();
                break;
            case Nested:
                column.template = ClickHouseNestedValue.ofEmpty(column.nested);
                break;
            case Tuple:
                column.template = ClickHouseTupleValue.of();
                break;
            case Nothing:
                column.template = ClickHouseEmptyValue.INSTANCE;
                break;
            case Variant:
                column.template = ClickHouseTupleValue.of();
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

        boolean fixedLength = true;
        int estimatedLength = 0;

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
                    if (matchedKeyword == KEYWORD_AGGREGATE_FUNCTION) { // NOSONAR
                        int pIndex = p.indexOf('(');
                        aggFunc = ClickHouseAggregateFunction.of(pIndex > 0 ? p.substring(0, pIndex) : p);
                    }
                    isFirst = false;
                } else {
                    nestedColumns.add(ClickHouseColumn.of("", p));
                }
            }
            column = new ClickHouseColumn(ClickHouseDataType.valueOf(matchedKeyword), name,
                    args.substring(startIndex, i), nullable, lowCardinality, params, nestedColumns);
            column.aggFuncType = aggFunc;
            fixedLength = false;
            estimatedLength++;
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
            fixedLength = false;
            estimatedLength++;
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
            fixedLength = false;
            estimatedLength++;
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
            fixedLength = false;
            estimatedLength++;
        } else if (args.startsWith(matchedKeyword = KEYWORD_TUPLE, i)
                || args.startsWith(matchedKeyword = KEYWORD_OBJECT, i)
                || args.startsWith(matchedKeyword = KEYWORD_VARIANT, i)) {
            int index = args.indexOf('(', i + matchedKeyword.length());
            if (index < i) {
                throw new IllegalArgumentException(ERROR_MISSING_NESTED_TYPE);
            }
            int endIndex = ClickHouseUtils.skipBrackets(args, index, len, '(') - 1;
            List<ClickHouseColumn> nestedColumns = new LinkedList<>();
            for (i = index + 1; i <= endIndex; i++) {
                char c = args.charAt(i);
                if (c == ')') {
                    break;
                } else if (c != ',' && !Character.isWhitespace(c)) {
                    String columnName = "";
                    i = readColumn(args, i, endIndex, "", nestedColumns);
                }
            }
            if (nestedColumns.isEmpty()) {
                throw new IllegalArgumentException("Tuple should have at least one nested column");
            }

            List<ClickHouseDataType> variantDataTypes = new ArrayList<>();
            if (matchedKeyword.equals(KEYWORD_VARIANT)) {
                nestedColumns.sort(Comparator.comparing(o -> o.getDataType().name()));
                nestedColumns.forEach(c -> {
                    c.columnName = "v." + c.getDataType().name();
                    variantDataTypes.add(c.dataType);
                });
            }
            column = new ClickHouseColumn(ClickHouseDataType.valueOf(matchedKeyword), name,
                    args.substring(startIndex, endIndex + 1), nullable, lowCardinality, null, nestedColumns);
            for (ClickHouseColumn n : nestedColumns) {
                estimatedLength += n.estimatedByteLength;
                if (!n.fixedByteLength) {
                    fixedLength = false;
                }
            }
            column.classToVariantOrdNumMap = ClickHouseDataType.buildVariantMapping(variantDataTypes);

            for (int ordNum = 0; ordNum < nestedColumns.size(); ordNum++) {
                ClickHouseColumn nestedColumn = nestedColumns.get(ordNum);
                if (nestedColumn.getDataType() == ClickHouseDataType.Array) {
                    Set<Class<?>> classSet = ClickHouseDataType.DATA_TYPE_TO_CLASS.get(nestedColumn.arrayBaseColumn.dataType);
                    if (classSet != null) {
                        if (column.arrayToVariantOrdNumMap == null) {
                            column.arrayToVariantOrdNumMap = new HashMap<>();
                        }
                        for (Class<?> c : classSet) {
                            column.arrayToVariantOrdNumMap.put(c, ordNum);
                        }
                    }
                } else if (nestedColumn.getDataType() == ClickHouseDataType.Map) {
                    Set<Class<?>> keyClassSet = ClickHouseDataType.DATA_TYPE_TO_CLASS.get(nestedColumn.getKeyInfo().getDataType());
                    Set<Class<?>> valueClassSet = ClickHouseDataType.DATA_TYPE_TO_CLASS.get(nestedColumn.getValueInfo().getDataType());
                    if (keyClassSet != null && valueClassSet != null) {
                        if (column.mapKeyToVariantOrdNumMap == null) {
                            column.mapKeyToVariantOrdNumMap = new HashMap<>();
                        }
                        if (column.mapValueToVariantOrdNumMap == null) {
                            column.mapValueToVariantOrdNumMap = new HashMap<>();
                        }
                        for (Class<?> c : keyClassSet) {
                            column.mapKeyToVariantOrdNumMap.put(c, ordNum);
                        }
                        for (Class<?> c : valueClassSet) {
                            column.mapValueToVariantOrdNumMap.put(c, ordNum);
                        }
                    }
                }
            }
        }

        if (column == null) {
            i = ClickHouseUtils.readNameOrQuotedString(args, i, len, builder);
            List<String> params = new LinkedList<>();
            for (; i < len; i++) {
                char ch = args.charAt(i);
                if (ch == '(') {
                    i = ClickHouseUtils.readParameters(args, i, len, params) - 1;
                } else if (ch == ')') {
                    if (brackets <= 0) {
                        break;
                    } else {
                        if (--brackets <= 0) {
                            i++;
                            break;
                        }
                    }
                } else if (ch == ',') {
                    break;
                } else if (!Character.isWhitespace(ch)) {
                    StringBuilder sb = new StringBuilder();
                    i = ClickHouseUtils.readNameOrQuotedString(args, i, len, sb);
                    String modifier = sb.toString();
                    String normalizedModifier = modifier.toUpperCase();
                    sb.setLength(0);
                    boolean startsWithNot = false;
                    if ("NOT".equals(normalizedModifier)) {
                        startsWithNot = true;
                        i = ClickHouseUtils.readNameOrQuotedString(args, i, len, sb);
                        modifier = sb.toString();
                        normalizedModifier = modifier.toUpperCase();
                        sb.setLength(0);
                    }

                    if ("NULL".equals(normalizedModifier)) {
                        if (nullable) {
                            throw new IllegalArgumentException("Nullable and NULL cannot be used together");
                        }
                        nullable = !startsWithNot;
                        i = ClickHouseUtils.skipContentsUntil(args, i, len, ',') - 1;
                        break;
                    } else if (startsWithNot) {
                        throw new IllegalArgumentException("Expect keyword NULL after NOT");
                    } else if ("ALIAS".equals(normalizedModifier) || "CODEC".equals(normalizedModifier)
                            || "DEFAULT".equals(normalizedModifier) || "MATERIALIZED".equals(normalizedModifier)
                            || "TTL".equals(normalizedModifier)) { // stop words
                        i = ClickHouseUtils.skipContentsUntil(args, i, len, ',') - 1;
                        break;
                    } else {
                        if ((name == null || name.isEmpty())
                                && !ClickHouseDataType.mayStartWith(builder.toString(), normalizedModifier)) {
                            return readColumn(args, i - modifier.length(), len, builder.toString(), list);
                        } else {
                            builder.append(' ');
                        }
                        builder.append(modifier);
                        i--;
                    }
                }
            }
            column = new ClickHouseColumn(ClickHouseDataType.of(builder.toString()), name,
                    args.substring(startIndex, i), nullable, lowCardinality, params, null);
            builder.setLength(0);
        }

        if (nullable) {
            fixedLength = false;
            estimatedLength++;
        } else if (column.dataType == ClickHouseDataType.FixedString) {
            fixedLength = true;
            estimatedLength = column.precision;
        } else if (column.dataType.getByteLength() == 0) {
            fixedLength = false;
        } else {
            estimatedLength += column.dataType.getByteLength();
        }
        column.fixedByteLength = fixedLength;
        column.estimatedByteLength = estimatedLength;
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
        return update(new ClickHouseColumn(dataType, columnName, null, nullable, lowCardinality,
                Arrays.asList(parameters), null));
    }

    public static ClickHouseColumn of(String columnName, ClickHouseDataType dataType, boolean nullable,
            ClickHouseColumn... nestedColumns) {
        return update(
                new ClickHouseColumn(dataType, columnName, null, nullable, false, null, Arrays.asList(nestedColumns)));
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

    public ClickHouseColumn(ClickHouseDataType dataType, String columnName, String originalTypeName, boolean nullable,
            boolean lowCardinality, List<String> parameters, List<ClickHouseColumn> nestedColumns) {
        this(dataType, columnName, originalTypeName, nullable, lowCardinality, parameters, nestedColumns, ClickHouseEnum.EMPTY);
    }

    public ClickHouseColumn(ClickHouseDataType dataType, String columnName, String originalTypeName, boolean nullable,
            boolean lowCardinality, List<String> parameters, List<ClickHouseColumn> nestedColumns, ClickHouseEnum enumConstants) {
        this.aggFuncType = null;
        this.dataType = ClickHouseChecker.nonNull(dataType, "dataType");

        this.columnCount = 1;
        this.columnIndex = 0;
        this.columnName = columnName == null ? "" : columnName;
        this.originalTypeName = originalTypeName == null ? dataType.name() : originalTypeName;
        this.nullable = nullable;
        this.lowCardinality = lowCardinality;
        this.hasDefault = false;

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

        this.fixedByteLength = false;
        this.estimatedByteLength = 0;
        this.enumConstants = enumConstants;
    }

    /**
     * Sets zero-based column index and column count.
     * 
     * @param index zero-based column index, negative number is treated as zero
     * @param count column count, should be always greater than one
     */
    protected void setColumnIndex(int index, int count) {
        this.columnCount = count < 2 ? 1 : count;
        this.columnIndex = index < 1 ? 0 : (index < count ? index : count - 1);
    }

    public boolean isAggregateFunction() {
        return dataType == ClickHouseDataType.AggregateFunction;

    }

    public int getVariantOrdNum(Object value) {
        if (value != null && value.getClass().isArray()) {
            // TODO: add cache by value class
            Class<?> c = value.getClass();
            while (c.isArray()) {
                c = c.getComponentType();
            }
            return arrayToVariantOrdNumMap.getOrDefault(c, -1);
        } else if (value != null && value instanceof List<?>) {
            // TODO: add cache by instance of the list
            Object tmpV = ((List) value).get(0);
            Class<?> valueClass = tmpV.getClass();
            while (tmpV instanceof List<?>) {
                tmpV = ((List) tmpV).get(0);
                valueClass = tmpV.getClass();
            }
            return arrayToVariantOrdNumMap.getOrDefault(valueClass, -1);
        } else if (value != null && value instanceof Map<?,?>) {
            // TODO: add cache by instance of map
            Map<?, ?> map = (Map<?, ?>) value;
            if (!map.isEmpty()) {
                for (Map.Entry<?, ?> e : map.entrySet()) {
                    if (e.getValue() != null) {
                        int keyOrdNum = mapKeyToVariantOrdNumMap.getOrDefault(e.getKey().getClass(), -1);
                        int valueOrdNum = mapValueToVariantOrdNumMap.getOrDefault(e.getValue().getClass(), -1);

                        if (keyOrdNum == valueOrdNum) {
                            return valueOrdNum; // exact match
                        } else if (keyOrdNum != -1 && valueOrdNum != -1) {
                            if (ClickHouseDataType.DATA_TYPE_TO_CLASS.get(nested.get(keyOrdNum).getValueInfo().getDataType()).contains(e.getValue().getClass())){
                                return keyOrdNum; // can write to map found by key class because values are compatible
                            } else {
                                return valueOrdNum;
                            }
                        }

                        break;
                    }
                }
            }
            return -1;
        } else {
            return classToVariantOrdNumMap.getOrDefault(value.getClass(), -1);
        }
    }

    public boolean isArray() {
        return dataType == ClickHouseDataType.Array;
    }

    public boolean isEnum() {
        return dataType == ClickHouseDataType.Enum8 || dataType == ClickHouseDataType.Enum16 || dataType == ClickHouseDataType.Enum;
    }

    public boolean isFixedLength() {
        return fixedByteLength;
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

    public boolean isNestedType() {
        return dataType.isNested();
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

    public Class<?> getObjectClass(ClickHouseDataConfig config) {
        if (timeZone != null) {
            return OffsetDateTime.class;
        }

        return config != null && config.isWidenUnsignedTypes() ? dataType.getWiderObjectClass()
                : dataType.getObjectClass();
    }

    public Class<?> getObjectClassForArray(ClickHouseDataConfig config) {
        Class<?> javaClass = config.isUseObjectsInArray() || isNullable() ? getObjectClass(config)
                : getPrimitiveClass(config);
        if (config.isUseBinaryString() && javaClass == String.class) {
            javaClass = Object.class; // more specific to use byte[][]?
        }
        return javaClass;
    }

    public Class<?> getPrimitiveClass(ClickHouseDataConfig config) {
        if (timeZone != null) {
            return OffsetDateTime.class;
        }

        return config != null && config.isWidenUnsignedTypes() ? dataType.getWiderPrimitiveClass()
                : dataType.getPrimitiveClass();
    }

    public ClickHouseEnum getEnumConstants() {
        return enumConstants;
    }

    public int getEstimatedLength() {
        return estimatedByteLength;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getOriginalTypeName() {
        return originalTypeName;
    }

    public boolean isFirstColumn() {
        return columnCount == 0;
    }

    public boolean isLastColumn() {
        return columnCount - columnIndex == 1;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean hasDefault() {
        return hasDefault;
    }

    public void setHasDefault(boolean hasDefault) {
        this.hasDefault = hasDefault;
    }

    public boolean isLowCardinality() {
        return !lowCardinalityDisabled && lowCardinality;
    }

    public boolean isLowCardinalityDisabled() {
        return lowCardinalityDisabled;
    }

    public void disableLowCardinality() {
        lowCardinalityDisabled = true;
    }

    public boolean hasTimeZone() {
        return timeZone != null;
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

    public ClickHouseArraySequence newArrayValue(ClickHouseDataConfig config) {
        int level = arrayLevel;
        ClickHouseArraySequence value;
        if (level < 1 || arrayBaseColumn == null || arrayBaseColumn.nullable || config.isUseObjectsInArray()) {
            value = arrayBaseColumn == null ? ClickHouseArrayValue.ofEmpty()
                    : ClickHouseArrayValue.ofEmpty(arrayBaseColumn.getObjectClass(config));
        } else if (level > 255) {
            throw new IllegalArgumentException(
                    "Nested level of array should be less than or equal to 255 but we got: " + level);
        } else if (level > 1) {
            value = ClickHouseArrayValue
                    .of((Object[]) Array.newInstance(arrayBaseColumn.getPrimitiveClass(config), new int[level]));
        } else { // primitive arrays
            switch (arrayBaseColumn.dataType) {
                case Bool:
                    value = ClickHouseBoolArrayValue.ofEmpty();
                    break;
                case Int8:
                    value = ClickHouseByteArrayValue.ofEmpty();
                    break;
                case UInt8:
                    value = config.isWidenUnsignedTypes() ? ClickHouseShortArrayValue.ofEmpty()
                            : ClickHouseByteArrayValue.ofUnsignedEmpty();
                    break;
                case Int16:
                    value = ClickHouseShortArrayValue.ofEmpty();
                    break;
                case UInt16:
                    value = config.isWidenUnsignedTypes() ? ClickHouseIntArrayValue.ofEmpty()
                            : ClickHouseShortArrayValue.ofUnsignedEmpty();
                    break;
                case Int32:
                    value = ClickHouseIntArrayValue.ofEmpty();
                    break;
                case UInt32:
                    value = config.isWidenUnsignedTypes() ? ClickHouseLongArrayValue.ofEmpty()
                            : ClickHouseIntArrayValue.ofUnsignedEmpty();
                    break;
                case Int64:
                    value = ClickHouseLongArrayValue.ofEmpty();
                    break;
                case UInt64:
                    value = ClickHouseLongArrayValue.ofUnsignedEmpty();
                    break;
                case Float32:
                    value = ClickHouseFloatArrayValue.ofEmpty();
                    break;
                case Float64:
                    value = ClickHouseDoubleArrayValue.ofEmpty();
                    break;
                default:
                    value = ClickHouseArrayValue.ofEmpty(arrayBaseColumn.getObjectClass(config));
                    break;
            }
        }
        return value;
    }

    public ClickHouseValue newValue(ClickHouseDataConfig config) {
        if (template != null) {
            return template.copy();
        }

        ClickHouseValue value = null;
        switch (dataType) {
            case UInt8:
                value = config.isWidenUnsignedTypes() ? ClickHouseShortValue.ofNull()
                        : ClickHouseByteValue.ofUnsignedNull();
                break;
            case UInt16:
                value = config.isWidenUnsignedTypes() ? ClickHouseIntegerValue.ofNull()
                        : ClickHouseShortValue.ofUnsignedNull();
                break;
            case UInt32:
                value = config.isWidenUnsignedTypes() ? ClickHouseLongValue.ofNull()
                        : ClickHouseIntegerValue.ofUnsignedNull();
                break;
            case DateTime:
            case DateTime32:
            case DateTime64:
                value = ClickHouseDateTimeValue.ofNull(getScale(), config.getUseTimeZone());
                break;
            case SimpleAggregateFunction:
                value = nested.get(0).newValue(config);
                break;
            case AggregateFunction:
                value = ClickHouseEmptyValue.INSTANCE;
                switch (aggFuncType) {
                    case any:
                        value = nested.get(0).newValue(config);
                        break;
                    case groupBitmap:
                        value = ClickHouseBitmapValue.ofEmpty(nested.get(0).getDataType());
                        break;
                    default:
                        break;
                }
                break;
            case Array:
                value = newArrayValue(config);
                break;
            case Map:
                value = ClickHouseMapValue.ofEmpty(getKeyInfo().getObjectClass(config), // NOSONAR
                        getValueInfo().getObjectClass(config)); // NOSONAR
                break;
            default:
                break;
        }

        if (value == null) {
            throw new IllegalArgumentException("Unsupported data type: " + dataType.name());
        }

        return value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((arrayBaseColumn == null) ? 0 : arrayBaseColumn.hashCode());
        result = prime * result + ((aggFuncType == null) ? 0 : aggFuncType.hashCode());
        result = prime * result + arrayLevel;
        result = prime * result + columnCount;
        result = prime * result + columnIndex;
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        result = prime * result + ((originalTypeName == null) ? 0 : originalTypeName.hashCode());
        result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
        result = prime * result + (lowCardinality ? 1231 : 1237);
        result = prime * result + ((nested == null) ? 0 : nested.hashCode());
        result = prime * result + (nullable ? 1231 : 1237);
        result = prime * result + ((parameters == null) ? 0 : parameters.hashCode());
        result = prime * result + precision;
        result = prime * result + scale;
        result = prime * result + ((timeZone == null) ? 0 : timeZone.hashCode());
        result = prime * result + (fixedByteLength ? 1231 : 1237);
        result = prime * result + estimatedByteLength;
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
                && arrayLevel == other.arrayLevel && columnCount == other.columnCount
                && columnIndex == other.columnIndex && Objects.equals(columnName, other.columnName)
                && dataType == other.dataType && lowCardinality == other.lowCardinality
                && Objects.equals(nested, other.nested) && nullable == other.nullable
                && Objects.equals(originalTypeName, other.originalTypeName)
                && Objects.equals(parameters, other.parameters) && precision == other.precision && scale == other.scale
                && Objects.equals(timeZone, other.timeZone) && fixedByteLength == other.fixedByteLength
                && estimatedByteLength == other.estimatedByteLength;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (columnName == null || columnName.isEmpty()) {
            builder.append("column").append(columnIndex);
        } else {
            builder.append(columnName);
        }
        return builder.append(' ').append(originalTypeName).toString();
    }
}
