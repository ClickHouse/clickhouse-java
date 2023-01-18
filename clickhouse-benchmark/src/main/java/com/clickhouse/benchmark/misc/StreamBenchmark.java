package com.clickhouse.benchmark.misc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.clickhouse.benchmark.BaseState;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.config.ClickHouseBufferingMode;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;

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

/**
 * Blocking:
 * Benchmark Mode Cnt Score Error Units
 * StreamBenchmark.async thrpt 20 1.574 ? 0.039 ops/s
 * StreamBenchmark.jdk thrpt 20 4281.206 ? 91.983 ops/s
 * StreamBenchmark.piped thrpt 20 3913.994 ? 142.566 ops/s
 * StreamBenchmark.wrapped thrpt 20 3939.248 ? 54.868 ops/s
 * 
 * Non-blocking:
 * 
 */
@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
@Threads(value = -1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class StreamBenchmark {
    @State(Scope.Thread)
    public static class StreamState extends BaseState {
        public int bufferSize;
        public int samples;

        public byte[] bytes;
        public ClickHouseConfig config;

        @Setup(Level.Trial)
        public void setupSamples() {
            bufferSize = Integer.getInteger("buffer", (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue());
            samples = Integer.getInteger("samples", 500000);

            bytes = new byte[samples];

            Map<ClickHouseOption, Serializable> options = new HashMap<>();
            options.put(ClickHouseClientOption.ASYNC, Boolean.parseBoolean(System.getProperty("async", "true")));
            options.put(ClickHouseClientOption.REQUEST_BUFFERING, ClickHouseBufferingMode.valueOf(
                    System.getProperty("mode", ClickHouseClientOption.REQUEST_BUFFERING.getDefaultValue().toString())
                            .toUpperCase()));
            options.put(ClickHouseClientOption.BUFFER_SIZE, bufferSize);
            options.put(ClickHouseClientOption.MAX_QUEUED_BUFFERS,
                    Integer.getInteger("queue", (int) ClickHouseClientOption.MAX_QUEUED_BUFFERS.getDefaultValue()));
            options.put(ClickHouseClientOption.COMPRESS, Boolean.parseBoolean(System.getProperty("compress", "false")));
            options.put(ClickHouseClientOption.DECOMPRESS,
                    Boolean.parseBoolean(System.getProperty("compress", "false")));
            options.put(ClickHouseClientOption.USE_BLOCKING_QUEUE,
                    Boolean.parseBoolean(System.getProperty("blocking", "true")));
            config = new ClickHouseConfig(options, null, null, null);
        }

        @Setup(Level.Iteration)
        public void initStream() {
            new Random().nextBytes(bytes);
        }
    }

    @Benchmark
    public void classic(StreamState state, Blackhole consumer) throws IOException {
        int size = state.bufferSize;
        byte[] buffer = new byte[size];
        int count = 0;
        ByteArrayOutputStream out = new ByteArrayOutputStream(state.samples);
        try (InputStream in = new ByteArrayInputStream(state.bytes)) {
            int read = 0;
            while ((read = in.read(buffer, 0, size)) > 0) {
                out.write(buffer, 0, read);
                count += read;
            }
            if (count != state.samples) {
                throw new IllegalStateException(String.format("Expect %d bytes but got %d", size, count));
            }
            out.flush();
            out.close();
        }
        if (!Arrays.equals(state.bytes, out.toByteArray())) {
            throw new IllegalStateException("Incorrect result");
        }
    }

    @Benchmark
    public void piped(StreamState state, Blackhole consumer) throws IOException {
        int size = state.bufferSize;
        long count = 0;
        ByteArrayOutputStream out = new ByteArrayOutputStream(state.samples);
        try (InputStream in = new ByteArrayInputStream(state.bytes)) {
            if ((count = ClickHouseInputStream.pipe(in, out, size)) != state.samples) {
                throw new IllegalStateException(String.format("Expect %d bytes but got %d", size, count));
            }
            out.flush();
            out.close();
        }
        if (!Arrays.equals(state.bytes, out.toByteArray())) {
            throw new IllegalStateException("Incorrect result");
        }
    }

    @Benchmark
    public void wrapped(StreamState state, Blackhole consumer) throws IOException {
        int size = state.bufferSize;
        long count = 0;
        ByteArrayOutputStream bao = new ByteArrayOutputStream(state.samples);
        try (ClickHouseInputStream in = ClickHouseInputStream.of(new ByteArrayInputStream(state.bytes), size);
                ClickHouseOutputStream out = ClickHouseOutputStream.of(bao, size)) {
            if ((count = in.pipe(out)) != state.samples) {
                throw new IllegalStateException(String.format("Expect %d bytes but got %d", size, count));
            }
        }
        if (!Arrays.equals(state.bytes, bao.toByteArray())) {
            throw new IllegalStateException("Incorrect result");
        }
    }

    @Benchmark
    public void async(StreamState state, Blackhole consumer) throws IOException {
        int size = state.bufferSize;
        long count = 0;
        ByteArrayOutputStream bao = new ByteArrayOutputStream(state.samples);
        try (ClickHouseInputStream in = ClickHouseInputStream.of(new ByteArrayInputStream(state.bytes), size);
                ClickHouseOutputStream out = ClickHouseClient.getAsyncRequestOutputStream(state.config, bao, null)) {
            if ((count = in.pipe(out)) != state.samples) {
                throw new IllegalStateException(String.format("Expect %d bytes but got %d", size, count));
            }
        }
        if (!Arrays.equals(state.bytes, bao.toByteArray())) {
            throw new IllegalStateException("Incorrect result");
        }
    }
}
