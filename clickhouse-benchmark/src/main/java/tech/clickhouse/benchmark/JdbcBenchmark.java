package tech.clickhouse.benchmark;

import org.openjdk.jmh.annotations.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
@Threads(value = -1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public abstract class JdbcBenchmark {
    // batch size for mutation
    private final int batchSize = Integer.parseInt(System.getProperty("batchSize", "1000"));
    // fetch size for query
    private final int fetchSize = Integer.parseInt(System.getProperty("fetchSize", "1000"));

    protected PreparedStatement setParameters(PreparedStatement s, Object... values) throws SQLException {
        if (values != null && values.length > 0) {
            int index = 1;
            for (Object v : values) {
                s.setObject(index++, v);
            }
        }

        return s;
    }

    protected String replaceParameters(String sql, Object... values) {
        if (values != null && values.length > 0) {
            for (Object v : values) {
                int index = sql.indexOf('?');
                if (index == -1) {
                    break;
                }

                String expr = null;
                if (v instanceof Number) {
                    expr = String.valueOf(v);
                } else {
                    expr = "'" + v + "'"; // without escaping...
                }

                sql = sql.substring(0, index) + expr + sql.substring(index + 1);
            }
        }

        return sql;
    }

    private int processBatch(Statement s, String sql, Enumeration<Object[]> generator) throws SQLException {
        int rows = 0;
        int counter = 0;
        PreparedStatement ps = s instanceof PreparedStatement ? (PreparedStatement) s : null;
        while (generator.hasMoreElements()) {
            Object[] values = generator.nextElement();
            if (ps != null) {
                setParameters(ps, values).addBatch();
            } else {
                s.addBatch(replaceParameters(sql, values));
            }
            if (++counter % batchSize == 0) {
                rows += s.executeBatch().length;
            }
        }

        if (counter % batchSize != 0) {
            rows += s.executeBatch().length;
        }

        return rows;
    }

    protected int executeInsert(ClientState state, String sql, Enumeration<Object[]> generator) throws SQLException {
        Objects.requireNonNull(generator);

        final Connection conn = state.getConnection();
        int rows = 0;

        if (state.usePreparedStatement()) {
            try (PreparedStatement s = conn.prepareStatement(sql)) {
                rows = processBatch(s, sql, generator);
            }
        } else {
            try (Statement s = conn.createStatement()) {
                rows = processBatch(s, sql, generator);
            }
        }

        return rows;
    }

    protected Statement executeQuery(ClientState state, String sql, Object... values) throws SQLException {
        final Statement stmt;

        final Connection conn = state.getConnection();

        if (state.usePreparedStatement()) {
            PreparedStatement s = conn.prepareStatement(sql);
            s.setFetchSize(fetchSize);
            setParameters(s, values).executeQuery();
            stmt = s;
        } else {
            stmt = conn.createStatement();
            stmt.setFetchSize(fetchSize);
            stmt.executeQuery(replaceParameters(sql, values));
        }

        return stmt;
    }
}
