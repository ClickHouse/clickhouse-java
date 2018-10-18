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
        "(?i)INSERT\\s+INTO\\s+[^\'\"]+VALUES\\s*\\(");

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
        String currentParamToken = null;
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
        for (int i = valuesMode ? matcher.end() - 1 : 0 ; i < sql.length(); i++) {
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
                    currentParamToken = sql.substring(quotedStart, i + 1);
                }
            } else if (c == '`') {
                inBackQuotes = !inBackQuotes;
            } else if (!inQuotes && !inBackQuotes) {
                if (c == '?') {
                    if (currentParensLevel > 0) {
                        currentParamToken = ClickHousePreparedStatementImpl.PARAM_MARKER;
                    } else if (!valuesMode) {
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
                    if (valuesMode && currentParamToken != null) {
                        currentParamList.add(currentParamToken);
                        parts.add(sql.substring(partStart, sql.indexOf(currentParamToken, partStart)));
                        partStart = i ;
                        currentParamToken = null;
                    }
                } else if (c == '(') {
                    currentParensLevel++;
                } else if (c == ')') {
                   currentParensLevel--;
                   if (valuesMode && currentParensLevel == 0) {
                       if (currentParamToken != null) {
                           currentParamList.add(currentParamToken);
                           parts.add(sql.substring(partStart, sql.indexOf(currentParamToken, partStart)));
                           partStart = i;
                           currentParamToken = null;
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
                       currentParamToken = String.valueOf(c);
                    } else {
                        currentParamToken = currentParamToken != null
                            ? currentParamToken + c
                            : String.valueOf(c);
                    }
                    whiteSpace = false;
                }
            }
        }
        if (!valuesMode && !currentParamList.isEmpty()) {
            parameters.add(currentParamList);
        }
        String lastPart = sql.substring(partStart, sql.length());
        if (!StringUtils.isBlank(lastPart)) {
            parts.add(lastPart);
        }
    }



}
