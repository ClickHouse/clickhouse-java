package com.clickhouse.client.api.metadata;


/**
 * Strategy to match column names to method names.
 */
public interface ColumnToMethodMatchingStrategy {

    /**
     * Normalizes method name to match column name.
     * @param methodName original method name
     * @return normalized method name
     */
    String normalizeMethodName(String methodName);

    /**
     * Checks if the method is a setter.
     * @param methodName original (not normalized) method name
     * @return true if the method is a setter
     */
    boolean isSetter(String methodName);

    /**
     * Checks if the method is a getter.
     * @param methodName original (not normalized) method name
     * @return true if the method is a getter
     */
    boolean isGetter(String methodName);

    /**
     * Normalizes column name to match method name.
     * @param columnName original column name
     * @return normalized column name
     */
    String normalizeColumnName(String columnName);
}
