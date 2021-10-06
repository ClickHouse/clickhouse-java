package com.clickhouse.benchmark.misc;

import java.util.concurrent.TimeUnit;
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
import com.clickhouse.benchmark.BaseState;

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
@Threads(value = -1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class CompareBenchmark {
    @State(Scope.Thread)
    public static class CompareState extends BaseState {
        public int samples;
        public Object num;

        @Setup(Level.Trial)
        public void setupSamples() {
            samples = 500000;
        }

        @Setup(Level.Iteration)
        public void initValueClass() {
            num = (byte) getRandomNumber(Byte.MAX_VALUE);
        }
    }

    @Benchmark
    public void compareClass(CompareState state, Blackhole consumer) {
        for (int i = 0; i < state.samples; i++) {
            byte b = -1;
            if (state.num.getClass() == Byte.class) {
                b = (byte) state.num;
            }
            consumer.consume(b);
        }
    }

    @Benchmark
    public void instanceOf(CompareState state, Blackhole consumer) {
        for (int i = 0; i < state.samples; i++) {
            byte b = -1;
            if (state.num instanceof Number) {
                b = ((Number) state.num).byteValue();
            }
            consumer.consume(b);
        }
    }
}
