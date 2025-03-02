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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
@State(Scope.Benchmark)
public class JDBCInsertBenchmark extends JDBCBenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCInsertBenchmark.class);

    @Setup
    public void setup() throws SQLException {
        super.setup();
    }

    @TearDown
    public void tearDown() throws SQLException {
       super.tearDown();
    }

    @State(Scope.Thread)
    public static class InsertState {
        String payload1024 = RandomStringUtils.random(1024, true, true);;
        String payload2048 = RandomStringUtils.random(1024, true, true);;
        String payload4096 = RandomStringUtils.random(4096, true, true);;
        String payload8192 = RandomStringUtils.random(8192, true, true);;

        Map payloads = new HashMap<String, String>() {{
            put("1024", payload1024);
            put("2048", payload2048);
            put("4096", payload4096);
            put("8192", payload8192);
        }};
//        @Param({"100", "1000", "10000", "100000"})
        @Param({"100000"})
        int batchSize;
//        @Param({"1024", "2048", "4096", "8192"})
        @Param({"8192"})
        String payloadSize;

        public String getPayload(String payloadSize) {
            return (String) payloads.get(payloadSize);
        }
        @Setup(Level.Invocation)
        public void setup() throws SQLException {
            LOGGER.info("Setting up insert state");
            jdbcV1.createStatement().execute("DROP TABLE IF EXISTS insert_test_jdbcV1");
            jdbcV2.createStatement().execute("DROP TABLE IF EXISTS insert_test_jdbcV2");
            jdbcV1.createStatement().execute("CREATE TABLE insert_test_jdbcV1 ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = Memory");
            jdbcV2.createStatement().execute("CREATE TABLE insert_test_jdbcV2 ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) Engine = Memory");
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            LOGGER.info("Tearing down insert state");
        }
    }

    void insertData(Connection conn, String table, int size, String payload) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(String.format("INSERT INTO %s (off16, str, p_int8, p_int16, p_int32, p_int64, p_float32, p_float64, p_bool) VALUES (?,?,?,?,?,?,?,?,?)", table));
        for (int i = 0; i < size; i++) {
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

    @Benchmark
    @Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
    public void insertDataV1(InsertState state) throws Exception {
        LOGGER.info("(V1) JDBC Insert data benchmark");
        insertData(jdbcV1, "insert_test_jdbcV1", state.batchSize, state.getPayload(state.payloadSize));
    }

    @Benchmark
    @Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
    public void insertDataV2(InsertState state) throws Exception {
        LOGGER.info("(V2) JDBC Insert data benchmark");
        insertData(jdbcV2, "insert_test_jdbcV2", state.batchSize, state.getPayload(state.payloadSize));
    }

}
