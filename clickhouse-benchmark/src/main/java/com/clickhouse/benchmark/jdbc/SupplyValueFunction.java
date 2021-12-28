package com.clickhouse.benchmark.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface SupplyValueFunction {
    void set(PreparedStatement ps, Object value, int rowIndex, int columnIndex) throws SQLException;
}
