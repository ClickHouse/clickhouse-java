package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.DataTransferException;
import com.clickhouse.data.ClickHouseCityHash;
import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4Compressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class CompressedBlockOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(CompressedBlockOutputStream.class);
    public static final int UNCOMPRESSED_BUFF_SIZE = 64 * 1024; // 64K is most optimal for LZ4 compression

    private final ByteBuffer inBuffer;

    private final OutputStream out;

    private final ByteBuffer compressedBuffer;

    private boolean closed;

    public CompressedBlockOutputStream(OutputStream out, int bufferSize, int maxCompressedBufferSize) {
        super();
        Objects.requireNonNull(out, "out is null");
        if (bufferSize <= 0 || bufferSize > CompressedBlockInputStream.MAX_UNCOMPRESSED_BLOCK_SIZE) {
            throw new IllegalArgumentException("Invalid bufferSize: " + bufferSize);
        }
        if (maxCompressedBufferSize < bufferSize
                || maxCompressedBufferSize > CompressedBlockInputStream.MAX_COMPRESSED_BLOCK_SIZE - 9) {
            throw new IllegalArgumentException("Invalid maxCompressedBufferSize: " + maxCompressedBufferSize);
        }
        LOG.debug("Using compressor with buffer size {}", bufferSize);
        this.inBuffer = ByteBuffer.allocate(bufferSize);
        this.out = out;
        this.compressedBuffer = ByteBuffer.allocate(maxCompressedBufferSize + CompressedBlockInputStream.HEADER_LENGTH);
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    @Override
    public void write(int b) throws IOException {
        ensureOpen();
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
        ensureOpen();
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
        ensureOpen();
        if (inBuffer.position() > 0) {
            compressedBuffer.clear();
            compressedBuffer.put(16, getMagicNumber());
            int uncompressedLen = inBuffer.position();
            inBuffer.flip();
            int maxCompressedLen = compressedBuffer.capacity() - CompressedBlockInputStream.HEADER_LENGTH;
            int compressed = compressData(inBuffer, uncompressedLen, compressedBuffer, compressedBuffer);
            if (compressed <= 0 || compressed > maxCompressedLen) {
                throw new DataTransferException("Invalid compressed block size: " + compressed);
            }
            int compressedSizeWithHeader = compressed + 9;
            CompressedBlockInputStream.setInt32(compressedBuffer.array(), 17, compressedSizeWithHeader); // compressed size with header
            CompressedBlockInputStream.setInt32(compressedBuffer.array(), 21, uncompressedLen); // uncompressed size
            long[] hash = ClickHouseCityHash.cityHash128(compressedBuffer.array(), 16, compressedSizeWithHeader);
            setInt64(compressedBuffer.array(), 0, hash[0]);
            setInt64(compressedBuffer.array(), 8, hash[1]);
            out.write(compressedBuffer.array(), 0, compressed + CompressedBlockInputStream.HEADER_LENGTH);
            inBuffer.clear();
        }
    }

    protected abstract byte getMagicNumber();

    protected abstract int compressData(ByteBuffer inBuffer, int uncompressedLen, ByteBuffer compressedBuffer,
                                        ByteBuffer buffer);

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
        if (!closed) {
            Throwable failure = null;
            try {
                flush();
            } catch (IOException | RuntimeException | Error ex) {
                failure = ex;
                throw ex;
            } finally {
                closed = true;
                if (failure == null) {
                    out.close();
                } else {
                    try {
                        out.close();
                    } catch (IOException | RuntimeException | Error closeException) {
                        failure.addSuppressed(closeException);
                    }
                }
            }
        }
    }

    public static class LZ4OutputStream extends CompressedBlockOutputStream {

        private final LZ4Compressor compressor;

        public LZ4OutputStream(OutputStream out, LZ4Compressor compressor, int bufferSize) {
            super(out, validateBufferSize(bufferSize),
                    Objects.requireNonNull(compressor, "compressor is null").maxCompressedLength(bufferSize));
            this.compressor = compressor;
        }

        private static int validateBufferSize(int bufferSize) {
            if (bufferSize <= 0 || bufferSize > CompressedBlockInputStream.MAX_UNCOMPRESSED_BLOCK_SIZE) {
                throw new IllegalArgumentException("Invalid bufferSize: " + bufferSize);
            }
            return bufferSize;
        }

        @Override
        protected int compressData(ByteBuffer inBuffer, int uncompressedLen, ByteBuffer compressedBuffer,
                                   ByteBuffer buffer) {
            return compressor.compress(inBuffer, 0, uncompressedLen, compressedBuffer, CompressedBlockInputStream.HEADER_LENGTH,
                    compressedBuffer.capacity() - CompressedBlockInputStream.HEADER_LENGTH);
        }

        @Override
        protected byte getMagicNumber() {
            return CompressedBlockInputStream.MAGIC_LZ4;
        }
    }

    public static class ZSTDOutputStream extends CompressedBlockOutputStream {

        private static final int COMPRESSION_LEVEL = 3;

        public ZSTDOutputStream(OutputStream out, int bufferSize) {
            super(out, bufferSize, computeMaxCompressedSize(bufferSize));
        }

        private static int computeMaxCompressedSize(int bufferSize) {
            if (bufferSize <= 0 || bufferSize > CompressedBlockInputStream.MAX_UNCOMPRESSED_BLOCK_SIZE) {
                throw new IllegalArgumentException("Invalid bufferSize: " + bufferSize);
            }
            long bound = Zstd.compressBound(bufferSize);
            if (Zstd.isError(bound) || bound <= 0 || bound > Integer.MAX_VALUE - CompressedBlockInputStream.HEADER_LENGTH) {
                throw new IllegalArgumentException("Invalid ZSTD compressBound for bufferSize: " + bufferSize);
            }
            return (int) bound;
        }

        @Override
        protected int compressData(ByteBuffer inBuffer, int uncompressedLen, ByteBuffer compressedBuffer,
                                   ByteBuffer buffer) {
            return (int) Zstd.compressByteArray(compressedBuffer.array(), CompressedBlockInputStream.HEADER_LENGTH,
                    compressedBuffer.capacity() - CompressedBlockInputStream.HEADER_LENGTH,
                    inBuffer.array(), 0, uncompressedLen, COMPRESSION_LEVEL);
        }

        @Override
        protected byte getMagicNumber() {
            return CompressedBlockInputStream.MAGIC_ZSTD_3;
        }
    }
}
