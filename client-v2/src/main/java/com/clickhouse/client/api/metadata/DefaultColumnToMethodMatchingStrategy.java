package com.clickhouse.client.api.metadata;


import java.util.regex.Pattern;

/**
 * Default implementation of {@link ColumnToMethodMatchingStrategy} takes the following rules:
 * <ul>
 *     <li>Method name is normalized by removing prefixes like "get", "set", "is", "has".</li>
 *     <li>Column name is normalized by removing special characters like "-", "_", ".".</li>
 *     <li>Normalized method name and column name are compared case-insensitively.</li>
 * </ul>
 *
 *
 */
public class DefaultColumnToMethodMatchingStrategy implements ColumnToMethodMatchingStrategy {

    public static final DefaultColumnToMethodMatchingStrategy INSTANCE = new DefaultColumnToMethodMatchingStrategy();

    private final Pattern getterPattern;
    private final Pattern setterPattern;

    private final Pattern methodReplacePattern;

    private final Pattern columnReplacePattern;


    public DefaultColumnToMethodMatchingStrategy() {
        this("^(get|is|has).+", "^(set).+", "^(get|set|is|has)|_", "[-_.]");
    }

    public DefaultColumnToMethodMatchingStrategy(String getterPatternRegEx, String setterPatternRegEx, String methodReplacePatternRegEx, String columnReplacePatternRegEx) {
        this.getterPattern = Pattern.compile(getterPatternRegEx);
        this.setterPattern = Pattern.compile(setterPatternRegEx);
        this.methodReplacePattern = Pattern.compile(methodReplacePatternRegEx);
        this.columnReplacePattern = Pattern.compile(columnReplacePatternRegEx);
    }

    @Override
    public String normalizeMethodName(String methodName) {
        return methodReplacePattern.matcher(methodName).replaceAll("").toLowerCase();
    }

    @Override
    public boolean isSetter(String methodName) {
        return setterPattern.matcher(methodName).matches();
    }

    @Override
    public boolean isGetter(String methodName) {
        return getterPattern.matcher(methodName).matches();
    }

    @Override
    public String normalizeColumnName(String columnName) {
        return columnReplacePattern.matcher(columnName).replaceAll("").toLowerCase();
    }
}
