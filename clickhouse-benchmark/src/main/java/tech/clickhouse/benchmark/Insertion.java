package tech.clickhouse.benchmark;

import java.sql.Timestamp;
import java.util.Enumeration;
import org.openjdk.jmh.annotations.Benchmark;

public class Insertion extends JdbcBenchmark {
    @Benchmark
    public int insert10kUInt64Rows(ClientState state) throws Throwable {
        final int range = state.getRandomNumber();
        final int rows = state.getSampleSize() + range;

        return executeInsert(state, "insert into system.test_insert(i) values(?)", new Enumeration<Object[]>() {
            int counter = 0;

            @Override
            public boolean hasMoreElements() {
                return counter < rows;
            }

            @Override
            public Object[] nextElement() {
                return new Object[] { range + (counter++) };
            }
        });
    }

    @Benchmark
    public int insert10kStringRows(ClientState state) throws Throwable {
        final int range = state.getRandomNumber();
        final int rows = state.getSampleSize() + range;

        return executeInsert(state, "insert into system.test_insert(s) values(?)", new Enumeration<Object[]>() {
            int counter = 0;

            @Override
            public boolean hasMoreElements() {
                return counter < rows;
            }

            @Override
            public Object[] nextElement() {
                return new Object[] { String.valueOf(range + (counter++)) };
            }
        });
    }

    @Benchmark
    public int insert10kTimestampRows(ClientState state) throws Throwable {
        final int range = state.getRandomNumber();
        final int rows = state.getSampleSize() + range;

        return executeInsert(state, "insert into system.test_insert(t) values(?)", new Enumeration<Object[]>() {
            int counter = 0;

            @Override
            public boolean hasMoreElements() {
                return counter < rows;
            }

            @Override
            public Object[] nextElement() {
                return new Object[] { new Timestamp((long) range + (counter++)) };
            }
        });
    }
}
