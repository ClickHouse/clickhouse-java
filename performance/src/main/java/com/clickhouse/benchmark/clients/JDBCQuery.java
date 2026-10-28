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
        selectData(jdbcV1RowBinary, dataState, blackhole);
    }

    @Benchmark
    public void selectJDBCV2(DataState dataState, Blackhole blackhole) throws SQLException {
        selectData(jdbcV2RowBinary, dataState, blackhole);
    }

    @Benchmark
    public void selectJDBCV1Compressed(DataState dataState, Blackhole blackhole) throws SQLException {
        selectData(jdbcV1Compressed, dataState, blackhole);
    }

    @Benchmark
    public void selectJDBCV2Compressed(DataState dataState, Blackhole blackhole) throws SQLException {
        selectData(jdbcV2Compressed, dataState, blackhole);
    }

    void selectDataUseNames(Connection connection, DataState dataState, Blackhole blackhole) throws SQLException {
        String sql = getSelectQuery(dataState.tableNameFilled);
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                for (ClickHouseColumn col : dataState.dataSet.getSchema().getColumns()) {
                    switch (col.getDataType()) {
                        case Int8:
                        case UInt8:
                        case Int16:
                        case UInt16:
                        case Int32:
                        case UInt32:
                            blackhole.consume(rs.getLong(col.getColumnName()));
                            break;
                        case Date:
                        case Date32:
                            blackhole.consume(rs.getDate(col.getColumnName()));
                            break;
                        case DateTime:
                        case DateTime32:
                        case DateTime64:
                            // slower than just getObject
                            blackhole.consume(rs.getTimestamp(col.getColumnName()));
                            break;
                        case String:
                        case FixedString:
                            blackhole.consume(rs.getString(col.getColumnName()));
                            break;
                        default:
                            blackhole.consume(rs.getObject(col.getColumnName()));
                    }
                }
            }
        }
    }

    @Benchmark
    public void selectJDBCV1UseNames(DataState dataState, Blackhole blackhole) throws SQLException {
        selectDataUseNames(jdbcV1RowBinary, dataState, blackhole);
    }

    @Benchmark
    public void selectJDBCV2UseName(DataState dataState, Blackhole blackhole) throws SQLException {
        selectDataUseNames(jdbcV2RowBinary, dataState, blackhole);
    }

}