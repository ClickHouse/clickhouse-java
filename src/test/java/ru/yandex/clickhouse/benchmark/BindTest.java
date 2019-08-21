package ru.yandex.clickhouse.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Created by krash on 21.08.19.
 */
@Warmup(iterations = 3, time = 3)
@Fork(value = 3, jvmArgsAppend = "-XX:+UseG1GC")
@Measurement(time = 3, iterations = 5)
public class BindTest {

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(BindTest.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void benchBindIntArray(StatementState statementState) throws Exception {
        PreparedStatement stmt = statementState.statement;
        stmt.setArray(1, statementState.intArray);
    }

    @Benchmark
    public void benchBindLongArray(StatementState statementState) throws Exception {
        PreparedStatement stmt = statementState.statement;
        stmt.setArray(1, statementState.longArray);
    }

    @Benchmark
    public void benchBindShortArray(StatementState statementState) throws Exception {
        PreparedStatement stmt = statementState.statement;
        stmt.setArray(1, statementState.shortArray);
    }

    @Benchmark
    public void benchBindFloatArray(StatementState statementState) throws Exception {
        PreparedStatement stmt = statementState.statement;
        stmt.setArray(1, statementState.floatArray);
    }

    @Benchmark
    public void benchBindDoubleArray(StatementState statementState) throws Exception {
        PreparedStatement stmt = statementState.statement;
        stmt.setArray(1, statementState.doubleArray);
    }

    @State(Scope.Thread)
    public static class StatementState {
        public final ClickHouseArray intArray = new ClickHouseArray(Types.INTEGER, new int[]{10, 20, 30, 40});
        public final ClickHouseArray floatArray = new ClickHouseArray(Types.FLOAT, new float[]{10, 20, 30, 40});
        public final ClickHouseArray doubleArray = new ClickHouseArray(Types.DOUBLE, new double[]{10, 20, 30, 40});
        public final ClickHouseArray longArray = new ClickHouseArray(Types.INTEGER, new long[]{10, 20, 30, 40});
        public final ClickHouseArray shortArray = new ClickHouseArray(Types.INTEGER, new short[]{10, 20, 30, 40});
        public volatile PreparedStatement statement;

        public StatementState() {
            try {
                ClickHouseDataSource source = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123");
                statement = source.getConnection().prepareStatement("INSERT INTO default.smth (bla) VALUES (?)");
            } catch (Exception err) {
                throw new RuntimeException(err);
            }
        }
    }

}
