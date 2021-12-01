package com.clickhouse.benchmark.misc;

import java.util.Random;
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

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
@Threads(value = -1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ValuesBenchmark {
    @State(Scope.Thread)
    public static class ValueState {
        private final Random random = new Random();

        public int samples;
        public int num;

        @Setup(Level.Trial)
        public void setupSamples() {
            samples = 100000;
        }

        @Setup(Level.Iteration)
        public void setupValue() {
            num = random.nextInt(samples);
        }
    }

    static class AutoBoxing {
        private final Number value;

        AutoBoxing(Number value) {
            this.value = value;
        }

        long getValue() {
            return this.value.longValue();
        }
    }

    static class Primitive {
        private final int value;

        Primitive(int value) {
            this.value = value;
        }

        long getValue() {
            return (long) this.value;
        }
    }

    @Benchmark
    public long autoBoxing(ValueState state, Blackhole consumer) {
        for (int i = 0; i < state.samples; i++) {
            AutoBoxing v = new AutoBoxing(state.num);
            consumer.consume(v.getValue());
        }

        return 1L;
    }

    @Benchmark
    public long primitive(ValueState state, Blackhole consumer) {
        for (int i = 0; i < state.samples; i++) {
            Primitive v = new Primitive(state.num);
            consumer.consume(v.getValue());
        }

        return 1L;
    }
}
