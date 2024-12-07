package com.clickhouse.client.api.internal;

import com.clickhouse.data.ClickHouseCityHash;
import net.jpountz.lz4.LZ4Compressor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ClickHouseLZ4OutputStream extends OutputStream {

    public static final int UNCOMPRESSED_BUFF_SIZE = 8192;

    private final ByteBuffer buffer;

    private final OutputStream out;

    private final LZ4Compressor compressor;

    private byte tmpBuffer[] = new byte[1];

    private final ByteBuffer compressedBuffer;

    private static int HEADER_LEN = 15; // 9 bytes for header, 6 bytes for checksum


    public ClickHouseLZ4OutputStream(OutputStream out, LZ4Compressor compressor, int bufferSize) {
        super();
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.out = out;
        this.compressor = compressor;
        this.compressedBuffer = ByteBuffer.allocate(compressor.maxCompressedLength(buffer.capacity()) + HEADER_LEN);
    }

    @Override
    public void write(int b) throws IOException {
        tmpBuffer[0] = (byte) b;
        write(tmpBuffer, 0, 1);
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
            int remaining = Math.min(len - writtenBytes, buffer.remaining());
            buffer.put(b, off + writtenBytes, remaining);
            writtenBytes += remaining;
            if (buffer.remaining() == 0) {
                flush();
            }
        } while (writtenBytes < len);
    }

    @Override
    public void flush() throws IOException {
        if (buffer.position() > 0) {
            compressedBuffer.clear();
            compressedBuffer.put(16, ClickHouseLZ4InputStream.MAGIC);
            int uncompressedLen = buffer.position();
            buffer.flip();
            int compressed = compressor.compress(buffer, 0, uncompressedLen, compressedBuffer, 25,
                    compressedBuffer.remaining() - 25);
            int compressedSizeWithHeader = compressed + 9;
            ClickHouseLZ4InputStream.setInt32(compressedBuffer.array(), 17, compressedSizeWithHeader); // compressed size with header
            ClickHouseLZ4InputStream.setInt32(compressedBuffer.array(), 21, uncompressedLen); // uncompressed size
            long[] hash = ClickHouseCityHash.cityHash128(compressedBuffer.array(), 16, compressedSizeWithHeader);
            setInt64(compressedBuffer.array(), 0, hash[0]);
            setInt64(compressedBuffer.array(), 8, hash[1]);
            compressedBuffer.flip();
            out.write(compressedBuffer.array(), 0, compressed + 25);
            buffer.clear();
        }
    }


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
}
