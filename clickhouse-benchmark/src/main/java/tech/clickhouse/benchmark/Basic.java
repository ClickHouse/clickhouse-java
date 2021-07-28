package tech.clickhouse.benchmark;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import org.openjdk.jmh.annotations.Benchmark;

public class Basic extends JdbcBenchmark {
    @Benchmark
    public int insertOneRandomNumber(ClientState state) throws Throwable {
        final int num = state.getRandomSample();

        return executeInsert(state, "insert into test_insert(i) values(?)",
                Collections.enumeration(Collections.singletonList(new Object[] { num })));
    }

    @Benchmark
    public int selectOneRandomNumber(ClientState state) throws Throwable {
        final int num = state.getRandomSample();

        try (Statement stmt = executeQuery(state, "select ? as n", num); ResultSet rs = stmt.getResultSet();) {
            if (!rs.next() || num != rs.getInt(1)) {
                throw new IllegalStateException();
            }

            return num;
        }
    }
}
