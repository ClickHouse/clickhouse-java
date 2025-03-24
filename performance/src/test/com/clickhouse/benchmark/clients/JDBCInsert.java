package com.clickhouse.benchmark.clients;

import org.openjdk.jmh.annotations.Benchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static com.clickhouse.benchmark.TestEnvironment.DB_NAME;

public class JDBCInsert extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCInsert.class);
    void insetData(Connection connection, DataState dataState) throws SQLException {
        int size = dataState.dataSet.getSchema().getColumns().size();
        String names = dataState.dataSet.getSchema().getColumns().stream().map(column -> column.getColumnName()).collect(Collectors.joining(","));
        String values = dataState.dataSet.getSchema().getColumns().stream().map(column -> "?").collect(Collectors.joining(","));
        String sql = String.format("INSERT INTO `%s`.`%s` (%s) VALUES (%s)", DB_NAME ,dataState.tableNameEmpty, names, values);
        LOGGER.info("SQL: " + sql);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (List<Object> data : dataState.dataSet.getRowsOrdered()) {
            for (int j = 0; j < size; j++) {
                preparedStatement.setObject(j + 1, data.get(j));
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
    }

    @Benchmark
    public void insertJDBCV1(DataState dataState) throws SQLException {
        insetData(jdbcV1, dataState);
    }

    @Benchmark
    public void insertJDBCV2(DataState dataState) throws SQLException {
        insetData(jdbcV2, dataState);
    }

}
