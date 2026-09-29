package com.clickhouse.client.api.internal;

import com.clickhouse.data.ClickHouseCityHash;
import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4Compressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class CompressedBlockOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(CompressedBlockOutputStream.class);
    public static final int UNCOMPRESSED_BUFF_SIZE = 64 * 1024; // 64K is most optimal for LZ4 compression

    private final ByteBuffer inBuffer;

    private final OutputStream out;

    private final ByteBuffer compressedBuffer;

    private static final int HEADER_LEN = 15; // 9 bytes for header, 6 bytes for checksum


    public CompressedBlockOutputStream(OutputStream out, int bufferSize) {
        super();
        LOG.debug("Using compressor with buffer size {}", bufferSize);
        this.inBuffer = ByteBuffer.allocate(bufferSize);
        this.out = out;
        this.compressedBuffer = ByteBuffer.allocate(bufferSize + HEADER_LEN);
    }

    @Override
    public void write(int b) throws IOException {
        if (inBuffer.remaining() == 0) {
            flush();
        }
        inBuffer.put((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (b.length == 1) {
            write(b[0]);
        } else {
            write(b, 0, b.length);
        }
    }

    @Override
    public void write( byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException("b is null");
        } else if (off < 0) {
            throw new IndexOutOfBoundsException("off is negative");
        } else if (len < 0) {
            throw new IndexOutOfBoundsException("len is negative");
        } else if (off + len > b.length) {
            throw new IndexOutOfBoundsException("off + len is greater than b.length");
        } else if (len == 0) {
            return;
        }

        int writtenBytes = 0;
        do {
            if (inBuffer.remaining() == 0) {
                flush(); // flush will make inBuffer clear
            }
            int remaining = Math.min(len - writtenBytes, inBuffer.remaining());
            inBuffer.put(b, off + writtenBytes, remaining);
            writtenBytes += remaining;
        } while (writtenBytes < len);
    }

    @Override
    public void flush() throws IOException {
        if (inBuffer.position() > 0) {
            compressedBuffer.clear();
            compressedBuffer.put(16, getMagicNumber());
            int uncompressedLen = inBuffer.position();
            inBuffer.flip();
            int compressed = compressData(inBuffer, uncompressedLen, compressedBuffer, compressedBuffer);
            int compressedSizeWithHeader = compressed + 9;
            CompressedBlockInputStream.setInt32(compressedBuffer.array(), 17, compressedSizeWithHeader); // compressed size with header
            CompressedBlockInputStream.setInt32(compressedBuffer.array(), 21, uncompressedLen); // uncompressed size
            long[] hash = ClickHouseCityHash.cityHash128(compressedBuffer.array(), 16, compressedSizeWithHeader);
            setInt64(compressedBuffer.array(), 0, hash[0]);
            setInt64(compressedBuffer.array(), 8, hash[1]);
            compressedBuffer.flip();
            out.write(compressedBuffer.array(), 0, compressed + 25);
            inBuffer.clear();
        }
    }

    protected abstract byte getMagicNumber();

    protected abstract int compressData(ByteBuffer inBuffer, int uncompressedLen, ByteBuffer compressedBuffer, ByteBuffer buffer);

    static void setInt64(byte[] bytes, int offset, long value) {
        bytes[offset] = (byte) (0xFF & value);
        bytes[offset + 1] = (byte) (0xFF & (value >> 8));
        bytes[offset + 2] = (byte) (0xFF & (value >> 16));
        bytes[offset + 3] = (byte) (0xFF & (value >> 24));
        bytes[offset + 4] = (byte) (0xFF & (value >> 32));
        bytes[offset + 5] = (byte) (0xFF & (value >> 40));
        bytes[offset + 6] = (byte) (0xFF & (value >> 48));
        bytes[offset + 7] = (byte) (0xFF & (value >> 56));
    }
    @Override
    public void close() throws IOException {
        flush();
        out.close();
    }

    public static class LZ4OutputStream extends CompressedBlockOutputStream {

        private final LZ4Compressor compressor;

        public LZ4OutputStream(OutputStream out, LZ4Compressor compressor, int bufferSize) {
            super(out, bufferSize);
            this.compressor = compressor;
        }

        @Override
        protected int compressData(ByteBuffer inBuffer, int uncompressedLen, ByteBuffer compressedBuffer, ByteBuffer buffer) {
            return compressor.compress(inBuffer, 0, uncompressedLen, compressedBuffer, CompressedBlockInputStream.HEADER_LENGTH,
                    compressedBuffer.remaining() - CompressedBlockInputStream.HEADER_LENGTH);
        }

        @Override
        protected byte getMagicNumber() {
            return CompressedBlockInputStream.MAGIC_LZ4;
        }
    }

    public static class ZSTDOutputStream extends CompressedBlockOutputStream {

        private static final int COMPRESSION_LEVEL = 3;

        public ZSTDOutputStream(OutputStream out, int bufferSize) {
            super(out, bufferSize);
        }

        @Override
        protected int compressData(ByteBuffer inBuffer, int uncompressedLen, ByteBuffer compressedBuffer, ByteBuffer buffer) {
            return (int) Zstd.compressByteArray(compressedBuffer.array(), CompressedBlockInputStream.HEADER_LENGTH,
                    compressedBuffer.remaining() - CompressedBlockInputStream.HEADER_LENGTH,
                    inBuffer.array(), 0, uncompressedLen, COMPRESSION_LEVEL);
        }

        @Override
        protected byte getMagicNumber() {
            return CompressedBlockInputStream.MAGIC_ZSTD_3;
        }
    }
}
