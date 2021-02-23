package tech.clickhouse.benchmark;

import java.sql.Timestamp;
// import java.util.Collections;
import java.util.Enumeration;

import org.openjdk.jmh.annotations.Benchmark;

public class Insertion extends JdbcBenchmark {
    // @Benchmark
    // public int insertOneNumber(ClientState state) throws Throwable {
    // return executeInsert(state, "insert into test_insert(i) values(?)",
    // Collections.enumeration(Collections.singletonList(new Object[] { (int)
    // (Math.random() * 1000) })));
    // }

    @Benchmark
    public int insert10kUInt64Rows(ClientState state) throws Throwable {
        final int rows = 10000;
        final int num = (int) (Math.random() * rows);

        return executeInsert(state, "insert into test_insert(i) values(?)", new Enumeration<Object[]>() {
            int counter = 0;

            @Override
            public boolean hasMoreElements() {
                return counter < rows;
            }

            @Override
            public Object[] nextElement() {
                return new Object[] { num + (counter++) };
            }
        });
    }

    @Benchmark
    public int insert10kStringRows(ClientState state) throws Throwable {
        final int rows = 10000;
        final int num = (int) (Math.random() * rows);

        return executeInsert(state, "insert into test_insert(s) values(?)", new Enumeration<Object[]>() {
            int counter = 0;

            @Override
            public boolean hasMoreElements() {
                return counter < rows;
            }

            @Override
            public Object[] nextElement() {
                return new Object[] { String.valueOf(num + (counter++)) };
            }
        });
    }

    @Benchmark
    public int insert10kTimestampRows(ClientState state) throws Throwable {
        final int rows = 10000;
        final int num = (int) (Math.random() * rows);

        return executeInsert(state, "insert into test_insert(t) values(?)", new Enumeration<Object[]>() {
            int counter = 0;

            @Override
            public boolean hasMoreElements() {
                return counter < rows;
            }

            @Override
            public Object[] nextElement() {
                return new Object[] { new Timestamp(num + (counter++)) };
            }
        });
    }
}
