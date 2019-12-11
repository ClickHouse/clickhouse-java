package ru.yandex.clickhouse.response;

import java.util.TimeZone;

import ru.yandex.clickhouse.domain.ClickHouseDataType;

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

    public static ClickHouseColumnInfo parse(String typeInfo, String columnName) {
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
        currIdx = endIdx;
        if (endIdx == typeInfo.length()
            || !typeInfo.startsWith("(", currIdx))
        {
            return column;
        }

        switch (dataType) {
            case DateTime :
                String[] argsTZ = splitArgs(typeInfo, currIdx);
                if (argsTZ.length == 1) {
                    // unfortunately this will fall back to GMT if the time zone
                    // cannot be resolved
                    TimeZone tz = TimeZone.getTimeZone(argsTZ[0].replace("'", ""));
                    column.timeZone = tz;
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
                String[] argsScale = splitArgs(typeInfo, currIdx);
                column.scale = Integer.parseInt(argsScale[0]);
                break;
            case FixedString :
                String[] argsPrecision = splitArgs(typeInfo, currIdx);
                column.precision = Integer.parseInt(argsPrecision[0]);
                break;
            default :
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
                args.indexOf(")", currIdx))
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
     * @return the type name returned from the database, without modifiers, i.e.
     *         Nullable or LowCardinality
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

    int getArrayLevel() {
        return arrayLevel;
    }

    public ClickHouseDataType getArrayBaseType() {
        return arrayBaseType;
    }

    TimeZone getTimeZone() {
        return timeZone;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

}
