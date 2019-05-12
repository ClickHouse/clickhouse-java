package ru.yandex.clickhouse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.yandex.clickhouse.util.apache.StringUtils;

/**
 * Parser for JDBC SQL Strings
 * <p>
 * Tries to extract query parameters in a way that is usable for (batched)
 * prepared statements.
 */
final class PreparedStatementParser  {

    private static final Pattern VALUES = Pattern.compile(
        "(?i)INSERT\\s+INTO\\s+.+VALUES\\s*\\(",
        Pattern.MULTILINE | Pattern.DOTALL);

    private List<List<String>> parameters;
    private List<String> parts;
    private boolean valuesMode;

    private PreparedStatementParser() {
        parameters = new ArrayList<List<String>>();
        parts = new ArrayList<String>();
        valuesMode = false;
    }

    static PreparedStatementParser parse(String sql) {
        if (StringUtils.isBlank(sql)) {
            throw new IllegalArgumentException("SQL may not be blank");
        }
        PreparedStatementParser parser = new PreparedStatementParser();
        parser.parseSQL(sql);
        return parser;
    }

    List<List<String>> getParameters() {
        return Collections.unmodifiableList(parameters);
    }

    List<String> getParts() {
        return Collections.unmodifiableList(parts);
    }

    boolean isValuesMode() {
        return valuesMode;
    }

    private void reset() {
        parameters.clear();
        parts.clear();
        valuesMode = false;
    }

    private void parseSQL(String sql) {
        reset();
        List<String> currentParamList = new ArrayList<String>();
        boolean afterBackSlash = false;
        boolean inQuotes = false;
        boolean inBackQuotes = false;
        boolean inSingleLineComment = false;
        boolean inMultiLineComment = false;
        boolean whiteSpace = false;
        Matcher matcher = VALUES.matcher(sql);
        if (matcher.find()) {
            valuesMode = true;
        }
        int currentParensLevel = 0;
        int quotedStart = 0;
        int partStart = 0;
        for (int i = valuesMode ? matcher.end() - 1 : 0, idxStart = i, idxEnd = i ; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inSingleLineComment) {
                if (c == '\n') {
                    inSingleLineComment = false;
                }
            } else if (inMultiLineComment) {
                if (c == '*' && sql.length() > i + 1 && sql.charAt(i + 1) == '/') {
                    inMultiLineComment = false;
                    i++;
                }
            } else if (afterBackSlash) {
                afterBackSlash = false;
            } else if (c == '\\') {
                afterBackSlash = true;
            } else if (c == '\'') {
                inQuotes = !inQuotes;
                if (inQuotes) {
                    quotedStart = i;
                } else if (!afterBackSlash) {
                    idxStart = quotedStart;
                    idxEnd = i + 1;
                }
            } else if (c == '`') {
                inBackQuotes = !inBackQuotes;
            } else if (!inQuotes && !inBackQuotes) {
                if (c == '?') {
                    if (currentParensLevel > 0) {
                        idxStart = i;
                        idxEnd = i + 1;
                    }
                    if (!valuesMode) {
                        parts.add(sql.substring(partStart, i));
                        partStart = i + 1;
                        currentParamList.add(ClickHousePreparedStatementImpl.PARAM_MARKER);
                    }
                } else if (c == '-' && sql.length() > i + 1 && sql.charAt(i + 1) == '-') {
                    inSingleLineComment = true;
                    i++;
                } else if (c == '/' && sql.length() > i + 1 && sql.charAt(i + 1) == '*') {
                    inMultiLineComment = true;
                    i++;
                } else if (c == ',') {
                    if (valuesMode && idxEnd > idxStart) {
                        currentParamList.add(typeTransformParameterValue(sql.substring(idxStart, idxEnd)));
                        parts.add(sql.substring(partStart, idxStart));
                        partStart = idxEnd;
                        idxStart = idxEnd = i;
                    }
                    idxStart++;
                    idxEnd++;
                } else if (c == '(') {
                    currentParensLevel++;
                    idxStart++;
                    idxEnd++;
                } else if (c == ')') {
                   currentParensLevel--;
                   if (valuesMode && currentParensLevel == 0) {
                       if (idxEnd > idxStart) {
                           currentParamList.add(typeTransformParameterValue(sql.substring(idxStart, idxEnd)));
                           parts.add(sql.substring(partStart, idxStart));
                           partStart = idxEnd;
                           idxStart = idxEnd = i;
                       }
                       if (!currentParamList.isEmpty()) {
                           parameters.add(currentParamList);
                           currentParamList = new ArrayList<String>(currentParamList.size());
                       }
                   }
                } else if (Character.isWhitespace(c)) {
                    whiteSpace = true;
                } else if (currentParensLevel > 0) {
                    if (whiteSpace) {
                        idxStart = i;
                        idxEnd = i + 1;
                    } else {
                        idxEnd++;
                    }
                    whiteSpace = false;
                }
            }
        }
        if (!valuesMode && !currentParamList.isEmpty()) {
            parameters.add(currentParamList);
        }
        String lastPart = sql.substring(partStart, sql.length());
        parts.add(lastPart);
    }

    private static String typeTransformParameterValue(String paramValue) {
        if (paramValue == null) {
            return null;
        }
        if (Boolean.TRUE.toString().equalsIgnoreCase(paramValue)) {
            return "1";
        }
        if (Boolean.FALSE.toString().equalsIgnoreCase(paramValue)) {
            return "0";
        }
        if ("NULL".equalsIgnoreCase(paramValue)) {
            return "\\N";
        }
        return paramValue;
    }

}
