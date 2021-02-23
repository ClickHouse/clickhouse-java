package tech.clickhouse.benchmark;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;

import org.openjdk.jmh.annotations.Benchmark;

public class Basic extends JdbcBenchmark {
    @Benchmark
    public int selectOneRandomNumber(ClientState state) throws Throwable {
        final int num = (int) (Math.random() * 1000);

        try (Statement stmt = executeQuery(state, "select ? as n", num)) {
            ResultSet rs = stmt.getResultSet();

            rs.next();

            if (num != rs.getInt(1)) {
                throw new IllegalStateException();
            }

            return num;
        }
    }

    @Benchmark
    public int insertOneRandomNumber(ClientState state) throws Throwable {
        final int num = (int) (Math.random() * 1000);

        return executeInsert(state, "insert into test_insert(i) values(?)",
                Collections.enumeration(Collections.singletonList(new Object[] { num })));
    }
}
