package com.clickhouse.client.data;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

public class ClickHouseLZ4OutputStream extends OutputStream {
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();
    private final DataOutputStream dataWrapper;

    private final LZ4Compressor compressor;
    private final byte[] currentBlock;
    private final byte[] compressedBlock;

    private int pointer;

    public ClickHouseLZ4OutputStream(OutputStream stream, int maxCompressBlockSize) {
        dataWrapper = new DataOutputStream(stream);
        compressor = factory.fastCompressor();
        currentBlock = new byte[maxCompressBlockSize];
        compressedBlock = new byte[compressor.maxCompressedLength(maxCompressBlockSize)];
    }

    /**
     * @return Location of pointer in the byte buffer (bytes not yet flushed)
     */
    public int position() {
        return pointer;
    }

    @Override
    public void write(int b) throws IOException {
        currentBlock[pointer] = (byte) b;
        pointer++;

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
        dataWrapper.flush();
    }

    private void writeInt(int value) throws IOException {
        dataWrapper.write(0xFF & value);
        dataWrapper.write(0xFF & (value >> 8));
        dataWrapper.write(0xFF & (value >> 16));
        dataWrapper.write(0xFF & (value >> 24));
    }

    private void writeBlock() throws IOException {
        int compressed = compressor.compress(currentBlock, 0, pointer, compressedBlock, 0);
        ClickHouseBlockChecksum checksum = ClickHouseBlockChecksum.calculateForBlock(
                (byte) ClickHouseLZ4InputStream.MAGIC, compressed + 9, pointer, compressedBlock, compressed);
        dataWrapper.write(checksum.asBytes());
        dataWrapper.writeByte(ClickHouseLZ4InputStream.MAGIC);
        writeInt(compressed + 9); // compressed size with header
        writeInt(pointer); // uncompressed size
        dataWrapper.write(compressedBlock, 0, compressed);
        pointer = 0;
    }
}
