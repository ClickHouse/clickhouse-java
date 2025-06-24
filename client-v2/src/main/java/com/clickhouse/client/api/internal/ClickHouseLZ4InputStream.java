package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.data.ClickHouseByteUtils;
import com.clickhouse.data.ClickHouseCityHash;
import com.clickhouse.data.ClickHouseUtils;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ClickHouseLZ4InputStream extends InputStream {

    private static Logger LOG = LoggerFactory.getLogger(ClickHouseLZ4InputStream.class);
    private final LZ4FastDecompressor decompressor;

    private final InputStream in;

    private ByteBuffer buffer;

    private byte[] tmpBuffer = new byte[1];


    public ClickHouseLZ4InputStream(InputStream in, LZ4FastDecompressor decompressor, int bufferSize) {
        super();
        LOG.debug("Using decompressor {}", decompressor);
        this.decompressor = decompressor;
        this.in = in;
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.buffer.limit(0);
    }

    @Override
    public int read() throws IOException {
        int n = read(tmpBuffer, 0, 1);
        return n == -1 ? -1 : tmpBuffer[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException("b is null");
        } else if (off < 0) {
            throw new IndexOutOfBoundsException("off is negative");
        } else if (len < 0) {
            throw new IndexOutOfBoundsException("len is negative");
        } else if (off + len > b.length) {
            throw new IndexOutOfBoundsException("off + len is greater than b.length");
        } else if (len == 0) {
            return 0;
        }

        int readBytes = 0;
        do {
            int remaining = Math.min(len - readBytes, buffer.remaining());
            buffer.get(b, off + readBytes, remaining);
            readBytes += remaining;
        } while (readBytes < len && refill() != -1);

        return readBytes == 0 ? -1 : readBytes;
    }


    static final byte MAGIC = (byte) 0x82;
    static final int HEADER_LENGTH = 25;

    final byte[] headerBuff = new byte[HEADER_LENGTH];

    /**
     * Method ensures to read all bytes from the input stream.
     * In case of network connection it may be a case when not all bytes are read at once.
     * @throws IOException
     */
    private boolean readFully(byte[] b, int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            int count = in.read(b, off + n, len - n);
            if (count < 0) {
                if (n == 0) {
                    return false;
                }
                throw new IOException(ClickHouseUtils.format("Incomplete read: {0} of {1}", n, len));
            }
            n += count;
        }

        return true;
    }

    private int refill() throws IOException {

        // read header
        boolean readFully = readFully(headerBuff, 0, HEADER_LENGTH);
        if (!readFully) {
            return -1;
        }

        if (headerBuff[16] != MAGIC) {
            // 1 byte - 0x82 (shows this is LZ4)
            throw new ClientException("Invalid LZ4 magic byte: '" + headerBuff[16] + "'");
        }

        // 4 bytes - size of the compressed data including 9 bytes of the header
        int compressedSizeWithHeader = getInt32(headerBuff, 17);
        // 4 bytes - size of uncompressed data
        int uncompressedSize = getInt32(headerBuff, 21);

        int offset = 9;
        final byte[] block =  new byte[compressedSizeWithHeader];
        block[0] = MAGIC;
        setInt32(block, 1, compressedSizeWithHeader);
        setInt32(block, 5, uncompressedSize);
        // compressed data: compressed_size - 9 bytes
        int remaining = compressedSizeWithHeader - offset;

        readFully = readFully(block, offset, remaining);
        if (!readFully) {
            throw new EOFException("Unexpected end of stream");
        }

        long[] real = ClickHouseCityHash.cityHash128(block, 0, compressedSizeWithHeader);
        if (real[0] != getInt64(headerBuff, 0) || real[1] != ClickHouseByteUtils.getInt64(headerBuff, 8)) {
            throw new ClientException("Corrupted stream: checksum mismatch");
        }

        if (buffer.capacity() < uncompressedSize) {
            buffer = ByteBuffer.allocate(uncompressedSize);
            LOG.debug("Buffer size is too small, reallocate buffer with size: {}", uncompressedSize);
        }
        decompressor.decompress(ByteBuffer.wrap(block), offset,  buffer, 0, uncompressedSize);
        buffer.position(0);
        buffer.limit(uncompressedSize);
        return uncompressedSize;
    }

    /**
     * Read int32 Little Endian
     * @param bytes
     * @param offset
     * @return
     */
    static int getInt32(byte[] bytes, int offset) {
        return (0xFF & bytes[offset]) | ((0xFF & bytes[offset + 1]) << 8) | ((0xFF & bytes[offset + 2]) << 16)
                | ((0xFF & bytes[offset + 3]) << 24);
    }

    /**
     * Read int64 Little Endian
     * @param bytes
     * @param offset
     * @return
     */
    static long getInt64(byte[] bytes, int offset) {
        return (0xFFL & bytes[offset]) | ((0xFFL & bytes[offset + 1]) << 8) | ((0xFFL & bytes[offset + 2]) << 16)
                | ((0xFFL & bytes[offset + 3]) << 24) | ((0xFFL & bytes[offset + 4]) << 32)
                | ((0xFFL & bytes[offset + 5]) << 40) | ((0xFFL & bytes[offset + 6]) << 48)
                | ((0xFFL & bytes[offset + 7]) << 56);
    }

    static void setInt32(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (0xFF & value);
        bytes[offset + 1] = (byte) (0xFF & (value >> 8));
        bytes[offset + 2] = (byte) (0xFF & (value >> 16));
        bytes[offset + 3] = (byte) (0xFF & (value >> 24));
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
