package tech.clickhouse.benchmark;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import org.openjdk.jmh.annotations.Benchmark;

public class Query extends JdbcBenchmark {
    @Benchmark
    public int select10kUInt64Rows(ClientState state) throws Throwable {
        int rows = 10000;
        int num = (int) (Math.random() * rows);
        try (Statement stmt = executeQuery(state, "select * from system.numbers where number > ? limit " + rows, num)) {
            ResultSet rs = stmt.getResultSet();

            float avg = 0.0F;
            int count = 0;
            while (rs.next()) {
                avg = (rs.getInt(1) + avg * count) / (++count);
            }

            if (count != rows) {
                throw new IllegalStateException();
            }

            return count;
        }
    }

    @Benchmark
    public int select10kStringRows(ClientState state) throws Throwable {
        int rows = 10000;
        int num = (int) (Math.random() * rows);
        try (Statement stmt = executeQuery(state,
                "select toString(number) as s from system.numbers where number > ? limit " + rows, num)) {
            ResultSet rs = stmt.getResultSet();

            int count = 0;
            String str = null;
            while (rs.next()) {
                str = rs.getString(1);
                count++;
            }

            if (count != rows) {
                throw new IllegalStateException();
            }

            return count;
        }
    }

    @Benchmark
    public int select10kTimestampRows(ClientState state) throws Throwable {
        int rows = 10000;
        int num = (int) (Math.random() * rows);
        try (Statement stmt = executeQuery(state,
                "select toDateTime('2021-02-20 13:15:20') + number as d from system.numbers where number > ? limit "
                        + rows,
                num)) {
            ResultSet rs = stmt.getResultSet();

            int count = 0;
            Timestamp ts = null;
            while (rs.next()) {
                ts = rs.getTimestamp(1);
                count++;
            }

            if (count != rows) {
                throw new IllegalStateException();
            }

            return count;
        }
    }
}
