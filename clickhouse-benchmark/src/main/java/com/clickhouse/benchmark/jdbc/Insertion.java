package com.clickhouse.benchmark.jdbc;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Enumeration;
import java.util.Locale;
import java.util.UUID;

import org.openjdk.jmh.annotations.Benchmark;

public class Insertion extends DriverBenchmark {
    private void checkResult(DriverState state, String batchId, int expectedRows, int actualResult) throws Exception {
        boolean isValid = actualResult == expectedRows;
        if (isValid) {
            try (Statement stmt = executeQuery(state,
                    "select toInt32(count(1)) from system.test_insert where b=?", batchId)) {
                ResultSet rs = stmt.getResultSet();
                isValid = rs.next() && (actualResult = rs.getInt(1)) == expectedRows;
            }
        }
        if (!isValid) {
            throw new IllegalStateException(String.format(Locale.ROOT,
                    "Expected %d rows being inserted but we got %d", expectedRows, actualResult));
        }
    }

    @Benchmark
    public void insertInt64(DriverState state) throws Throwable {
        final int range = state.getRandomNumber();
        final int rows = state.getSampleSize() + range;
        final String batchId = UUID.randomUUID().toString();
        SupplyValueFunction func = state.getSupplyFunction((p, v, l, i) -> p.setLong(i, (long) v));
        int result = executeInsert(state,
                "insert into system.test_insert(b,i) -- select b,v from input('b String, v Int64')\nvalues(?,?)", func,
                new Enumeration<Object[]>() {
                    int counter = 0;

                    @Override
                    public boolean hasMoreElements() {
                        return counter < rows;
                    }

                    @Override
                    public Object[] nextElement() {
                        return new Object[] { batchId, (long) (range + (counter++)) };
                    }
                });

        checkResult(state, batchId, rows, result);
    }

    @Benchmark
    public void insertString(DriverState state) throws Throwable {
        final int range = state.getRandomNumber();
        final int rows = state.getSampleSize() + range;
        final String batchId = UUID.randomUUID().toString();
        SupplyValueFunction func = state.getSupplyFunction((p, v, l, i) -> p.setString(i, (String) v));
        int result = executeInsert(state,
                "insert into system.test_insert(b, s) -- select b, v from input('b String, v String')\nvalues(?, ?)",
                func, new Enumeration<Object[]>() {
                    int counter = 0;

                    @Override
                    public boolean hasMoreElements() {
                        return counter < rows;
                    }

                    @Override
                    public Object[] nextElement() {
                        return new Object[] { batchId, String.valueOf(range + (counter++)) };
                    }
                });

        checkResult(state, batchId, rows, result);
    }

    @Benchmark
    public void insertTimestamp(DriverState state) throws Throwable {
        final int range = state.getRandomNumber();
        final int rows = state.getSampleSize() + range;
        final String batchId = UUID.randomUUID().toString();
        SupplyValueFunction func = state
                .getSupplyFunction((p, v, l, i) -> p.setTimestamp(i, Timestamp.valueOf((LocalDateTime) v)));
        int result = executeInsert(state,
                "insert into system.test_insert(b,t) -- select b,v from input('b String,v DateTime32')\nvalues(?,?)",
                func, new Enumeration<Object[]>() {
                    int counter = 0;

                    @Override
                    public boolean hasMoreElements() {
                        return counter < rows;
                    }

                    @Override
                    public Object[] nextElement() {
                        return new Object[] { batchId,
                                LocalDateTime.ofEpochSecond((long) range + (counter++), 0, ZoneOffset.UTC) };
                    }
                });

        checkResult(state, batchId, rows, result);
    }
}
