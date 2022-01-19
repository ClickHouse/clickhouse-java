package com.clickhouse.benchmark.misc;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.clickhouse.client.data.ClickHouseFloatValue;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
@Threads(value = -1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class FloatBenchmark {
    @State(Scope.Thread)
    public static class ValueState {
        private final Random random = new Random();

        public int samples;
        public float num;

        @Setup(Level.Trial)
        public void setupSamples() {
            samples = 10000000;
        }

        @Setup(Level.Iteration)
        public void setupValue() {
            num = random.nextFloat() * 10F;
        }
    }

    @Benchmark
    public long removeE(ValueState state, Blackhole consumer) {
        for (int i = 0; i < state.samples; i++) {
            float v = 10000000F + state.num;
            consumer.consume(ClickHouseFloatValue.of(v).toSqlExpression());
            // consumer.consume(v);
        }

        return 1L;
    }

    @Benchmark
    public long normal(ValueState state, Blackhole consumer) {
        for (int i = 0; i < state.samples; i++) {
            float v = state.num;
            consumer.consume(ClickHouseFloatValue.of(v).toSqlExpression());
            // consumer.consume(v);
        }

        return 1L;
    }
}
