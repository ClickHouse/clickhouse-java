package com.clickhouse.benchmark.clients;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.clickhouse.benchmark.BenchmarkRunner.getSelectQuery;

public class JDBCQuery extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCQuery.class);
    void selectData(Connection connection, DataState dataState, Blackhole blackhole) throws SQLException {
        String sql = getSelectQuery(dataState.tableNameFilled);
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    for (int i = 1; i <= dataState.dataSet.getSchema().getColumns().size(); i++) {
                        blackhole.consume(rs.getObject(i));
                    }
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

}
