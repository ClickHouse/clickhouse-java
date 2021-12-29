package com.clickhouse.benchmark.jdbc;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import org.openjdk.jmh.annotations.Benchmark;

public class Basic extends DriverBenchmark {
    @Benchmark
    public int insertOneRandomNumber(DriverState state) throws Throwable {
        final int num = state.getRandomSample();

        return executeInsert(state, "insert into test_insert(i) values(?)", (p, v, l, i) -> p.setObject(i, v),
                Collections.enumeration(Collections.singletonList(new Object[] { num })));
    }

    @Benchmark
    public int selectOneRandomNumber(DriverState state) throws Throwable {
        final int num = state.getRandomSample();

        try (Statement stmt = executeQuery(state, "select ? as n", num); ResultSet rs = stmt.getResultSet();) {
            if (!rs.next() || num != rs.getInt(1)) {
                throw new IllegalStateException();
            }

            return num;
        }
    }
}
