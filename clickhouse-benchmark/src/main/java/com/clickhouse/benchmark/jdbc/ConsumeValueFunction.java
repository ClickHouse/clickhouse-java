package com.clickhouse.benchmark.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.openjdk.jmh.infra.Blackhole;

@FunctionalInterface
public interface ConsumeValueFunction {
    void consume(Blackhole blackhole, ResultSet rs, int rowIndex, int columnIndex) throws SQLException;
}
