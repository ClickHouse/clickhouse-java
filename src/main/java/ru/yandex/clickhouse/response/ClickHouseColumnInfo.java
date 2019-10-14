package ru.yandex.clickhouse.response;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

public final class ClickHouseColumnInfo {

    private static final String KEYWORD_NULLABLE = "Nullable";
    private static final String KEYWORD_LOW_CARDINALITY = "LowCardinality";
    private static final String KEYWORD_ARRAY = "Array";
    private static final String KEYWORD_TUPLE = "Tuple";

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
    private List<ClickHouseColumnInfo> tupleElementTypes;

    public static ClickHouseColumnInfo parse(String typeInfo, String columnName) {
        return parse(typeInfo, columnName, new int[]{0});
    }

    public static ClickHouseColumnInfo parse(String typeInfo, String columnName, int[] currIdx) {
        ClickHouseColumnInfo column = new ClickHouseColumnInfo(typeInfo, columnName);
        if (typeInfo.startsWith(KEYWORD_TUPLE, currIdx[0])) {
            column.clickHouseDataType = ClickHouseDataType.Tuple;
            column.tupleElementTypes = new ArrayList<ClickHouseColumnInfo>();
            currIdx[0] += KEYWORD_TUPLE.length() + 1; // 1 for '('
            while(true) {
                ClickHouseColumnInfo chci = parse(typeInfo, null, currIdx);
                column.tupleElementTypes.add(chci);
                if (typeInfo.charAt(currIdx[0]) == ')') {
                    currIdx[0] += 1; // 1 for ')'
                    return column;
                } else {
                    currIdx[0] += 2; //2 for ", " separator
                }
            }
        }
        while (typeInfo.startsWith(KEYWORD_ARRAY, currIdx[0])) {
            column.arrayLevel++;
            column.clickHouseDataType = ClickHouseDataType.Array;
            currIdx[0] += KEYWORD_ARRAY.length() + 1; // opening parenthesis
        }
        if (typeInfo.startsWith(KEYWORD_LOW_CARDINALITY, currIdx[0])) {
            column.lowCardinality = true;
            currIdx[0] += KEYWORD_LOW_CARDINALITY.length() + 1;
        }
        if (typeInfo.startsWith(KEYWORD_NULLABLE, currIdx[0])) {
            column.nullable = true;
            currIdx[0] += KEYWORD_NULLABLE.length() + 1;
        }
        int endIdx = findFirstChar(typeInfo, currIdx[0], '(', ')', ',');
        if (endIdx < 0) {
            endIdx = typeInfo.length();
        }
        ClickHouseDataType dataType = ClickHouseDataType.fromTypeString(
            typeInfo.substring(currIdx[0], endIdx));
        if (column.arrayLevel > 0) {
            column.arrayBaseType = dataType;
        } else {
            column.clickHouseDataType = dataType;
        }
        column.precision = dataType.getDefaultPrecision();
        column.scale = dataType.getDefaultScale();
        currIdx[0] = endIdx;
        if (endIdx == typeInfo.length()
            || !typeInfo.startsWith("(", currIdx[0]))
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

    private static int findFirstChar(String str, int startIdx, char... ch) {
        for (int i = startIdx; i<str.length(); i++) {
            char strCh = str.charAt(i);
            for (int j = 0; j<ch.length; j++) {
                if (strCh == ch[j]) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static String[] splitArgs(String args, int[] currIdx) {
        // There can be arguments containing a closing parentheses
        // e.g. Enum8(\'f(o)o\' = 42), but we currently do not try
        // to parse any of those
        int endIdx = args.indexOf(")", currIdx[0]);
        String[] result = args
            .substring(
                args.indexOf("(", currIdx[0]) + 1,
                endIdx)
            .split("\\s*,\\s*");
        currIdx[0] = endIdx + 1;
        return result;
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
