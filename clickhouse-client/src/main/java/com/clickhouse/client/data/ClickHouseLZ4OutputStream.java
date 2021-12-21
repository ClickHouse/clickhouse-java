package com.clickhouse.client.data;

import java.io.IOException;
import java.io.OutputStream;

import com.clickhouse.client.ClickHouseChecker;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

public class ClickHouseLZ4OutputStream extends OutputStream {
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();

    private final OutputStream output;

    private final LZ4Compressor compressor;
    private final byte[] currentBlock;
    private final byte[] compressedBlock;

    private int pointer;

    public ClickHouseLZ4OutputStream(OutputStream stream, int maxCompressBlockSize) {
        output = ClickHouseChecker.nonNull(stream, "output");

        compressor = factory.fastCompressor();
        currentBlock = new byte[maxCompressBlockSize];
        // reserve the first 9 bytes for calculating checksum
        compressedBlock = new byte[compressor.maxCompressedLength(maxCompressBlockSize) + 15];
        compressedBlock[16] = ClickHouseLZ4InputStream.MAGIC;

        pointer = 0;
    }

    @Override
    public void write(int b) throws IOException {
        currentBlock[pointer++] = (byte) b;

        if (pointer == currentBlock.length) {
            writeBlock();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        int blockSize = currentBlock.length;
        int rest = blockSize - pointer;
        while (len >= rest) {
            System.arraycopy(b, off, currentBlock, pointer, rest);
            pointer += rest;
            writeBlock();
            off += rest;
            len -= rest;
            rest = blockSize;
        }

        if (len > 0) {
            System.arraycopy(b, off, currentBlock, pointer, len);
            pointer += len;
        }
    }

    @Override
    public void flush() throws IOException {
        if (pointer != 0) {
            writeBlock();
        }
        output.flush();
    }

    private void writeBlock() throws IOException {
        int compressed = compressor.compress(currentBlock, 0, pointer, compressedBlock, 25);
        int compressedSizeWithHeader = compressed + 9;
        BinaryStreamUtils.setInt32(compressedBlock, 17, compressedSizeWithHeader); // compressed size with header
        BinaryStreamUtils.setInt32(compressedBlock, 21, pointer); // uncompressed size
        long[] hash = ClickHouseCityHash.cityHash128(compressedBlock, 16, compressedSizeWithHeader);
        BinaryStreamUtils.setInt64(compressedBlock, 0, hash[0]);
        BinaryStreamUtils.setInt64(compressedBlock, 8, hash[1]);
        output.write(compressedBlock, 0, compressed + 25);
        pointer = 0;
    }
}
