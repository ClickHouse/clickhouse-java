package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.data.DataSet;
import com.clickhouse.client.api.internal.ClickHouseLZ4OutputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.stream.Lz4OutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

public class Components extends BenchmarkBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(Components.class);

    @Setup(Level.Trial)
    public void setup(DataState dataState) throws Exception {
        super.setup(dataState, false);
    }


    @TearDown(Level.Invocation)
    public void tearDownIteration(DataState dataState) {
        verifyRowsInsertedAndCleanup(dataState.dataSet, false);
    }


    @Benchmark
    public void CompressingOutputStreamV1(DataState dataState) {
        DataSet dataSet = dataState.dataSet;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ClickHouseOutputStream out =
                new Lz4OutputStream(baos, 4 * 8196, null)) {
            for (byte[] bytes : dataSet.getBytesList(dataSet.getFormat())) {
                out.write(bytes);
            }
            int size = baos.size();
            LOGGER.info("Compressed size: " + size);
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }

    private static final LZ4Factory factory = LZ4Factory.fastestInstance();

    @Benchmark
    public void CompressingOutputStreamV2(DataState dataState) {
        DataSet dataSet = dataState.dataSet;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(baos,
                factory.fastCompressor(), 4 * 8196)) {
            for (byte[] bytes : dataSet.getBytesList(dataSet.getFormat())) {
                out.write(bytes);
            }
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
    }
}
