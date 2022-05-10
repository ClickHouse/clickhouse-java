package com.clickhouse.client.stream;

import java.io.IOException;
import java.io.OutputStream;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.data.BinaryStreamUtils;
import com.clickhouse.client.data.ClickHouseCityHash;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

public class Lz4OutputStream extends AbstractByteArrayOutputStream {
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();

    private final OutputStream output;

    private final LZ4Compressor compressor;
    private final byte[] compressedBlock;

    @Override
    protected void flushBuffer() throws IOException {
        int compressed = compressor.compress(buffer, 0, position, compressedBlock, 25);
        int compressedSizeWithHeader = compressed + 9;
        BinaryStreamUtils.setInt32(compressedBlock, 17, compressedSizeWithHeader); // compressed size with header
        BinaryStreamUtils.setInt32(compressedBlock, 21, position); // uncompressed size
        long[] hash = ClickHouseCityHash.cityHash128(compressedBlock, 16, compressedSizeWithHeader);
        BinaryStreamUtils.setInt64(compressedBlock, 0, hash[0]);
        BinaryStreamUtils.setInt64(compressedBlock, 8, hash[1]);
        output.write(compressedBlock, 0, compressed + 25);
        position = 0;
    }

    @Override
    protected void flushBuffer(byte[] bytes, int offset, int length) throws IOException {
        int maxLen = compressor.maxCompressedLength(length) + 15;
        byte[] block = maxLen < compressedBlock.length ? compressedBlock : new byte[maxLen];
        block[16] = Lz4InputStream.MAGIC;

        int compressed = compressor.compress(bytes, offset, length, block, 25);
        int compressedSizeWithHeader = compressed + 9;
        BinaryStreamUtils.setInt32(block, 17, compressedSizeWithHeader);
        BinaryStreamUtils.setInt32(block, 21, length);
        long[] hash = ClickHouseCityHash.cityHash128(block, 16, compressedSizeWithHeader);
        BinaryStreamUtils.setInt64(block, 0, hash[0]);
        BinaryStreamUtils.setInt64(block, 8, hash[1]);
        output.write(block, 0, compressed + 25);
    }

    public Lz4OutputStream(OutputStream stream, int maxCompressBlockSize, Runnable postCloseAction) {
        super(maxCompressBlockSize, postCloseAction);

        output = ClickHouseChecker.nonNull(stream, "OutputStream");

        compressor = factory.fastCompressor();
        // reserve the first 9 bytes for calculating checksum
        compressedBlock = new byte[compressor.maxCompressedLength(maxCompressBlockSize) + 15];
        compressedBlock[16] = Lz4InputStream.MAGIC;
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();

        if (position > 0) {
            flushBuffer();
        }
        output.flush();
    }
}
