package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.client.api.internal.ClickHouseLZ4OutputStream;
import net.jpountz.lz4.LZ4Factory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

public class CompressorCompare extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompressorCompare.class);
    private static LZ4Factory factoryNative = null;
    private static LZ4Factory factoryUnsafe = null;
    private static LZ4Factory factorySafe = null;
    @Setup
    public void setup() {
        try {
            factoryNative = LZ4Factory.nativeInstance();
            LOGGER.info("Compressor type {}", factoryNative.fastCompressor());
        } catch (Throwable e) {
            LOGGER.error("Error: ", e);
        }
        try {
            factoryUnsafe = LZ4Factory.unsafeInstance();
            LOGGER.info("Compressor type {}", factoryUnsafe.fastCompressor());
        } catch (Throwable e) {
            LOGGER.error("Error: ", e);
        }
        try {
            factorySafe = LZ4Factory.safeInstance();
            LOGGER.info("Compressor type {}", factorySafe.fastCompressor());
        } catch (Throwable e) {
            LOGGER.error("Error: ", e);
        }
    }
    @State(Scope.Benchmark)
    public static class CompressorState {
        @Param({"65536", "1048576", "2097152", "4194304"})
        public int bufferSize;
        public LZ4Factory getCompressor(String compressor) {
            switch (compressor) {
                case "native":
                    return factoryNative;
                case "unsafe":
                    return factoryUnsafe;
                case "safe":
                    return factorySafe;
                default:
                    throw new IllegalArgumentException("Unknown compressor: " + compressor);
            }
        }
    }

    @Benchmark
    public void CompressingOutputStreamV2Native(DataState dataState, CompressorState compressorState) {
        DataSet dataSet = dataState.dataSet;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(baos,
                     compressorState.getCompressor("native").fastCompressor(), compressorState.bufferSize)) {
            for (byte[] bytes : dataSet.getBytesList(dataSet.getFormat())) {
                out.write(bytes);
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void CompressingOutputStreamV2Unsafe(DataState dataState, CompressorState compressorState) {
        DataSet dataSet = dataState.dataSet;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(baos,
                     compressorState.getCompressor("unsafe").fastCompressor(), compressorState.bufferSize)) {
            for (byte[] bytes : dataSet.getBytesList(dataSet.getFormat())) {
                out.write(bytes);
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    @Benchmark
    public void CompressingOutputStreamV2Safe(DataState dataState, CompressorState compressorState) {
        DataSet dataSet = dataState.dataSet;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(baos,
                     compressorState.getCompressor("safe").fastCompressor(), compressorState.bufferSize)) {
            for (byte[] bytes : dataSet.getBytesList(dataSet.getFormat())) {
                out.write(bytes);
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

}
