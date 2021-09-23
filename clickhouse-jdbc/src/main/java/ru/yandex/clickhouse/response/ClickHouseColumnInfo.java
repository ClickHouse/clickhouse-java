package ru.yandex.clickhouse.response;

import java.util.TimeZone;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

/**
 * This class represents a column defined in database.
 */
public final class ClickHouseColumnInfo {

    private static final String KEYWORD_NULLABLE = "Nullable";
    private static final String KEYWORD_LOW_CARDINALITY = "LowCardinality";
    private static final String KEYWORD_ARRAY = "Array";

    private ClickHouseDataType clickHouseDataType;
    private final String originalTypeName;
    private final String columnName;
    private boolean nullable;
    private boolean lowCardinality;
    private int arrayLevel;
    private ClickHouseDataType arrayBaseType;
    private TimeZone timeZone;
    private int precision;
    private int scale;
    private ClickHouseColumnInfo keyInfo;
    private ClickHouseColumnInfo valueInfo;
    private String functionName;

    @Deprecated
    public static ClickHouseColumnInfo parse(String typeInfo, String columnName) {
        return parse(typeInfo, columnName, null);
    }

    /**
     * Parse given type string.
     *
     * @param typeInfo type defined in database
     * @param columnName column name
     * @param serverTimeZone server time zone
     * @return parsed type
     */
    public static ClickHouseColumnInfo parse(String typeInfo, String columnName, TimeZone serverTimeZone) {
        ClickHouseColumnInfo column = new ClickHouseColumnInfo(typeInfo, columnName);
        int currIdx = 0;
        while (typeInfo.startsWith(KEYWORD_ARRAY, currIdx)) {
            column.arrayLevel++;
            column.clickHouseDataType = ClickHouseDataType.Array;
            currIdx += KEYWORD_ARRAY.length() + 1; // opening parenthesis
        }
        if (typeInfo.startsWith(KEYWORD_LOW_CARDINALITY, currIdx)) {
            column.lowCardinality = true;
            currIdx += KEYWORD_LOW_CARDINALITY.length() + 1;
        }
        if (typeInfo.startsWith(KEYWORD_NULLABLE, currIdx)) {
            column.nullable = true;
            currIdx += KEYWORD_NULLABLE.length() + 1;
        }
        int endIdx = typeInfo.indexOf("(", currIdx) < 0
            ? typeInfo.indexOf(")", currIdx)
            : typeInfo.indexOf("(", currIdx);
        if (endIdx < 0) {
            endIdx = typeInfo.length();
        }
        ClickHouseDataType dataType = ClickHouseDataType.fromTypeString(
            typeInfo.substring(currIdx, endIdx));
        if (column.arrayLevel > 0) {
            column.arrayBaseType = dataType;
        } else {
            column.clickHouseDataType = dataType;
        }
        column.precision = dataType.getDefaultPrecision();
        column.scale = dataType.getDefaultScale();
        column.timeZone = serverTimeZone;
        currIdx = endIdx;
        if (endIdx == typeInfo.length() || !typeInfo.startsWith("(", currIdx)) {
            return column;
        }

        switch (dataType) {
        case AggregateFunction :
            String[] argsAf = splitArgs(typeInfo, currIdx);
            column.functionName = argsAf[0];
            column.arrayBaseType = ClickHouseDataType.Unknown;
            if (argsAf.length == 2) {
                column.arrayBaseType = ClickHouseDataType.fromTypeString(argsAf[1]);
            }
            break;
        case DateTime :
            String[] argsDt = splitArgs(typeInfo, currIdx);
            if (argsDt.length == 2) { // same as DateTime64
                column.scale = Integer.parseInt(argsDt[0]);
                column.timeZone = TimeZone.getTimeZone(argsDt[1].replace("'", ""));
            } else if (argsDt.length == 1) { // same as DateTime32
                // unfortunately this will fall back to GMT if the time zone
                // cannot be resolved
                TimeZone tz = TimeZone.getTimeZone(argsDt[0].replace("'", ""));
                column.timeZone = tz;
            }
            break;
        case DateTime32:
            String[] argsD32 = splitArgs(typeInfo, currIdx);
            if (argsD32.length == 1) {
                // unfortunately this will fall back to GMT if the time zone
                // cannot be resolved
                TimeZone tz = TimeZone.getTimeZone(argsD32[0].replace("'", ""));
                column.timeZone = tz;
            }
            break;
        case DateTime64:
            String[] argsD64 = splitArgs(typeInfo, currIdx);
            if (argsD64.length == 2) {
                column.scale = Integer.parseInt(argsD64[0]);
                column.timeZone = TimeZone.getTimeZone(argsD64[1].replace("'", ""));
            }
            break;
        case Decimal :
            String[] argsDecimal = splitArgs(typeInfo, currIdx);
            if (argsDecimal.length == 2) {
                column.precision = Integer.parseInt(argsDecimal[0]);
                column.scale = Integer.parseInt(argsDecimal[1]);
            }
            break;
        case Decimal32 :
        case Decimal64 :
        case Decimal128 :
        case Decimal256 :
            String[] argsScale = splitArgs(typeInfo, currIdx);
            column.scale = Integer.parseInt(argsScale[0]);
            break;
        case FixedString :
            String[] argsPrecision = splitArgs(typeInfo, currIdx);
            column.precision = Integer.parseInt(argsPrecision[0]);
            break;
        case Map:
            String[] argsMap = splitArgs(typeInfo, currIdx);
            if (argsMap.length == 2) {
                column.keyInfo = ClickHouseColumnInfo.parse(argsMap[0], columnName + "Key", serverTimeZone);
                column.valueInfo = ClickHouseColumnInfo.parse(argsMap[1], columnName + "Value", serverTimeZone);
            }
            break;
        default:
            break;
        }

        return column;
    }

    private static String[] splitArgs(String args, int currIdx) {
        // There can be arguments containing a closing parentheses
        // e.g. Enum8(\'f(o)o\' = 42), but we currently do not try
        // to parse any of those
        return args
            .substring(
                args.indexOf("(", currIdx) + 1,
                args.lastIndexOf(")"))
            .split("\\s*,\\s*");
    }

    private ClickHouseColumnInfo(String originalTypeName, String columnName) {
        this.originalTypeName = originalTypeName;
        this.columnName = columnName;
    }

    public ClickHouseDataType getClickHouseDataType() {
        return clickHouseDataType;
    }

    public String getOriginalTypeName() {
        return originalTypeName;
    }

    /**
     * Get the type name returned from the database, without modifiers, i.e. Nullable or LowCardinality.
     *
     * @return the type name returned from the database
     */
    public String getCleanTypeName() {
        if (!nullable && !lowCardinality) {
            return originalTypeName;
        }
        StringBuilder sb = new StringBuilder();
        int idx = 0;
        int numParens = 0;
        if (lowCardinality) {
            int start = originalTypeName.indexOf(KEYWORD_LOW_CARDINALITY);
            sb.append(originalTypeName.substring(idx, start));
            numParens++;
            idx = start + KEYWORD_LOW_CARDINALITY.length() + 1;
        }
        if (nullable) {
            int start = originalTypeName.indexOf(KEYWORD_NULLABLE, idx);
            sb.append(originalTypeName.substring(idx, start));
            numParens++;
            idx = start + KEYWORD_NULLABLE.length() + 1;
        }
        sb.append(originalTypeName.substring(idx, originalTypeName.length() - numParens));
        return sb.toString();
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

    public int getArrayLevel() {
        return arrayLevel;
    }

    public boolean isArray() {
        return arrayLevel > 0;
    }

    public ClickHouseDataType getArrayBaseType() {
        return arrayBaseType;
    }

    public ClickHouseDataType getEffectiveClickHouseDataType() {
        return arrayLevel > 0 ? arrayBaseType : clickHouseDataType;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public ClickHouseColumnInfo getKeyInfo() {
        return this.keyInfo;
    }

    public ClickHouseColumnInfo getValueInfo() {
        return this.valueInfo;
    }

    public String getFunctionName() {
        return this.functionName;
    }
}
