package com.clickhouse.jdbc.perf;

import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class JDBCSelectBenchmark extends JDBCBenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCSelectBenchmark.class);
    private static final String tableName = "select_benchmark";
    private static final int limit = 100;
    private static final int LARGE_SIZE = 100000;
    @Setup
    public void setup() throws SQLException {
        super.setup();
        jdbcV2.createStatement().execute(String.format("CREATE TABLE IF NOT EXISTS %s (off16 Int16, str String, p_int8 Int8, p_int16 Int16, p_int32 Int32, p_int64 Int64, p_float32 Float32, p_float64 Float64, p_bool UInt8) ENGINE = Memory", tableName));
        // insert data to db for testing
        String payload = RandomStringUtils.random(8048, true, true);
        PreparedStatement ps = jdbcV2.prepareStatement(String.format("INSERT INTO %s (off16, str, p_int8, p_int16, p_int32, p_int64, p_float32, p_float64, p_bool) VALUES (?,?,?,?,?,?,?,?,?)", tableName));
        for (int i = 0; i < LARGE_SIZE; i++) {
            ps.setShort(1, (short) i);
            ps.setString(2, payload);
            ps.setByte(3, (byte)i);
            ps.setShort(4, (short)i);
            ps.setInt(5, i);
            ps.setLong(6, (long)i);
            ps.setFloat(7, (float)(i*0.1));
            ps.setDouble(8, (double)(i*0.1));
            ps.setBoolean(9, true);
            ps.addBatch();
        }
        ps.executeBatch();

    }

    @TearDown
    public void tearDown() throws SQLException {
        super.tearDown();
    }

    @State(Scope.Thread)
    public static class SelectState {
        @Param({"off16", "str", "p_int8", "p_int16", "p_int32", "p_int64", "p_float32", "p_float64", "p_bool"})
        // For research purpose, we only select one column (running the benchmark with all columns will take too long)
        // @Param({"off16"})
        String columnName;
        @Param({"t", "f"})
        String onlyConnection;
        public boolean isOnlyConnection(String onlyConnection) {
            if (onlyConnection.equals("t")) {
                return true;
            }
            return false;
        }
        @Setup(Level.Trial)
        public void setup() throws SQLException {
//            LOGGER.info("Setting up select state");
        }

        @TearDown(Level.Trial)
        public void tearDown() {
//            LOGGER.info("Tearing down select state");
        }
    }
    void selectData(Connection conn, String table, String filed, int limit, boolean onlyConnection) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s LIMIT %d", table, limit));
        if (onlyConnection) {
            rs.next();
            rs.close();
            return;
        }
        while (rs.next()) {
            rs.getString(filed);
        }
    }
    @Benchmark
    @Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
    public void selectV1(SelectState state) throws SQLException {
        selectData(jdbcV1, tableName, state.columnName, limit, state.isOnlyConnection(state.onlyConnection));
    }
    @Benchmark
    @Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
    public void selectV2(SelectState state) throws SQLException {
        selectData(jdbcV2, tableName, state.columnName, limit, state.isOnlyConnection(state.onlyConnection));
    }
}
