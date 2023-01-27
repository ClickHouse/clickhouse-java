package com.clickhouse.benchmark.misc;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.config.ClickHouseBufferingMode;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.format.BinaryStreamUtils;

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
@Threads(value = -1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class QueueBenchmark {
    @State(Scope.Thread)
    public static class CompareState extends BaseState {
        public long samples;

        @Setup(Level.Trial)
        public void setupSamples() {
            samples = 5000000L;
        }

        @Setup(Level.Iteration)
        public void initValueClass() {
            // ignore
        }
    }

    @Benchmark
    public void ideal(CompareState state, Blackhole consumer) {
        long range = state.samples;
        byte[] bytes = new byte[8];
        ClickHouseByteBuffer buffer = ClickHouseByteBuffer.of(bytes);
        for (long i = 0L; i < range; i++) {
            BinaryStreamUtils.setInt64(bytes, 0, i);
            consumer.consume(buffer.asLong());
            buffer.update(bytes); // reset read position
        }
    }

    @Benchmark
    public void blocking(CompareState state, Blackhole consumer) throws Exception {
        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        // options.put(ClickHouseClientOption.BUFFER_QUEUE_VARIATION, 0);
        // options.put(ClickHouseClientOption.MAX_QUEUED_BUFFERS, 0);
        // options.put(ClickHouseClientOption.SOCKET_TIMEOUT, 0);
        options.put(ClickHouseClientOption.USE_BLOCKING_QUEUE, false);
        final ClickHouseConfig config = new ClickHouseConfig(options);
        final ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                .createPipedOutputStream(config);
        CompletableFuture<Long> future = ClickHouseClient.submit(() -> {
            long range = state.samples;
            try (ClickHouseOutputStream out = stream) {
                for (long i = 0L; i < range; i++) {
                    BinaryStreamUtils.writeInt64(out, i);
                }
            }
            return range;
        });

        try (ClickHouseInputStream input = stream.getInputStream()) {
            consumer.consume(BinaryStreamUtils.readInt64(input));
        }

        consumer.consume(future.get());
    }

    @Benchmark
    public void nonBlocking(CompareState state, Blackhole consumer) throws Exception {
        final ClickHouseConfig config = new ClickHouseConfig(Collections
                .singletonMap(ClickHouseClientOption.RESPONSE_BUFFERING, ClickHouseBufferingMode.PERFORMANCE));
        final ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                .createPipedOutputStream(config);
        CompletableFuture<Long> future = ClickHouseClient.submit(() -> {
            long range = state.samples;
            try (ClickHouseOutputStream out = stream) {
                for (long i = 0L; i < range; i++) {
                    BinaryStreamUtils.writeInt64(out, i);
                }
            }
            return range;
        });

        try (ClickHouseInputStream input = stream.getInputStream()) {
            consumer.consume(BinaryStreamUtils.readInt64(input));
        }

        consumer.consume(future.get());
    }
}
