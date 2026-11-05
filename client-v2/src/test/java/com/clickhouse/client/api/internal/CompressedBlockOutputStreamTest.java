package com.clickhouse.client.api.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.function.Function;

import net.jpountz.lz4.LZ4Factory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CompressedBlockOutputStreamTest {

    private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();

    private static final int BUFFER_SIZE = CompressedBlockOutputStream.UNCOMPRESSED_BUFF_SIZE;

    @DataProvider(name = "streamsAndPayloads")
    public static Object[][] streamsAndPayloads() {
        Function<OutputStream, CompressedBlockOutputStream> lz4 =
                out -> new CompressedBlockOutputStream.LZ4OutputStream(out, LZ4_FACTORY.fastCompressor(), BUFFER_SIZE);
        Function<OutputStream, CompressedBlockOutputStream> zstd =
                out -> new CompressedBlockOutputStream.ZSTDOutputStream(out, BUFFER_SIZE);

        return new Object[][] {
                { "LZ4", "incompressible", lz4, incompressibleData(BUFFER_SIZE) },
                { "ZSTD", "incompressible", zstd, incompressibleData(BUFFER_SIZE) },
                { "LZ4", "compressible", lz4, compressibleData(BUFFER_SIZE) },
                { "ZSTD", "compressible", zstd, compressibleData(BUFFER_SIZE) },
        };
    }

    @Test(dataProvider = "streamsAndPayloads")
    public void testFullBlockRoundTrip(String method, String dataKind,
                                       Function<OutputStream, CompressedBlockOutputStream> streamFactory,
                                       byte[] payload) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (CompressedBlockOutputStream out = streamFactory.apply(compressed)) {
            out.write(payload);
        }

        try (CompressedBlockInputStream in = new CompressedBlockInputStream(
                new ByteArrayInputStream(compressed.toByteArray()), LZ4_FACTORY.fastDecompressor(), BUFFER_SIZE)) {
            ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) {
                decompressed.write(buf, 0, n);
            }
            Assert.assertEquals(decompressed.toByteArray(), payload, method + " " + dataKind + " round trip");
        }
    }

    private static byte[] incompressibleData(int length) {
        byte[] data = new byte[length];
        new Random(42).nextBytes(data);
        return data;
    }

    private static byte[] compressibleData(int length) {
        byte[] pattern = "ClickHouse compressed block test line. ".getBytes(StandardCharsets.US_ASCII);
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = pattern[i % pattern.length];
        }
        return data;
    }
}
