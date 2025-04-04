package com.clickhouse.benchmark.clients;

import com.clickhouse.data.ClickHouseColumn;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCQuery extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCQuery.class);

    void selectData(Connection connection, DataState dataState, Blackhole blackhole) throws SQLException {
        String sql = getSelectQuery(dataState.tableNameFilled);
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int nCol = dataState.dataSet.getSchema().getColumns().size();
            while (rs.next()) {
                for (int i = 1; i <= nCol; i++) {
                    blackhole.consume(rs.getObject(i));
                }
            }
        }
    }

    @Benchmark
    public void selectJDBCV1(DataState dataState, Blackhole blackhole) throws SQLException {
        selectData(jdbcV1, dataState, blackhole);
    }

    @Benchmark
    public void selectJDBCV2(DataState dataState, Blackhole blackhole) throws SQLException {
        selectData(jdbcV2, dataState, blackhole);
    }

    void selectDataUseNames(Connection connection, DataState dataState, Blackhole blackhole) throws SQLException {
        String sql = getSelectQuery(dataState.tableNameFilled);
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                for (ClickHouseColumn col : dataState.dataSet.getSchema().getColumns()) {
                    blackhole.consume(rs.getObject(col.getColumnName()));
                }
            }
        }
    }

    @Benchmark
    public void selectJDBCV1UseNames(DataState dataState, Blackhole blackhole) throws SQLException {
        selectDataUseNames(jdbcV1, dataState, blackhole);
    }

    @Benchmark
    public void selectJDBCV2UseName(DataState dataState, Blackhole blackhole) throws SQLException {
        selectDataUseNames(jdbcV2, dataState, blackhole);
    }
}