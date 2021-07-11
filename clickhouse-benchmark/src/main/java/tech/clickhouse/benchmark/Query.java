package tech.clickhouse.benchmark;

import java.sql.ResultSet;
import java.sql.Statement;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class Query extends JdbcBenchmark {
    @Benchmark
    public int select10kUInt64Rows(Blackhole blackhole, ClientState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        try (Statement stmt = executeQuery(state, "select * from system.numbers limit ?", rows)) {
            ResultSet rs = stmt.getResultSet();

            int count = 0;
            while (rs.next()) {
                blackhole.consume(rs.getInt(1));
                count++;
            }

            if (count != rows) {
                throw new IllegalStateException();
            }

            return count;
        }
    }

    @Benchmark
    public int select10kStringRows(Blackhole blackhole, ClientState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        try (Statement stmt = executeQuery(state, "select toString(number) as s from system.numbers limit ?", rows)) {
            ResultSet rs = stmt.getResultSet();

            int count = 0;
            while (rs.next()) {
                blackhole.consume(rs.getString(1));
                count++;
            }

            if (count != rows) {
                throw new IllegalStateException();
            }

            return count;
        }
    }

    @Benchmark
    public int select10kTimestampRows(Blackhole blackhole, ClientState state) throws Throwable {
        int num = state.getRandomNumber();
        int rows = state.getSampleSize() + num;
        try (Statement stmt = executeQuery(state,
                "select toDateTime('2021-02-20 13:15:20') + number as d from system.numbers limit ?", rows)) {
            ResultSet rs = stmt.getResultSet();

            int count = 0;
            while (rs.next()) {
                blackhole.consume(rs.getTimestamp(1));
                count++;
            }

            if (count != rows) {
                throw new IllegalStateException();
            }

            return count;
        }
    }
}
