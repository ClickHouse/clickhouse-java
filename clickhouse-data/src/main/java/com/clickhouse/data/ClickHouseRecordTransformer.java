package com.clickhouse.data;

@FunctionalInterface
@Deprecated
public interface ClickHouseRecordTransformer {
    /**
     * Updates values in the given record.
     *
     * @param rowIndex zero-based index of row
     * @param r        record to update
     */
    void update(int rowIndex, ClickHouseRecord r);
}
