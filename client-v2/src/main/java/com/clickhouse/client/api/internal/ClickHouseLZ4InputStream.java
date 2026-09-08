package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.data.ClickHouseByteUtils;
import com.clickhouse.data.ClickHouseCityHash;
import com.clickhouse.data.ClickHouseUtils;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Reads the framed output of the ClickHouse HTTP {@code compress=1} interface. Each block is
 * self-describing: its header carries the compression method the server used, so the codec is
 * detected per block instead of being assumed. LZ4, ZSTD and uncompressed blocks are supported.
 */
public class ClickHouseLZ4InputStream extends InputStream {

    private static Logger LOG = LoggerFactory.getLogger(ClickHouseLZ4InputStream.class);
    private final LZ4FastDecompressor decompressor;

    private final InputStream in;

    private ByteBuffer buffer;

    private byte[] tmpBuffer = new byte[1];


    public ClickHouseLZ4InputStream(InputStream in, LZ4FastDecompressor decompressor, int bufferSize) {
        super();
        LOG.debug("Reading compressed response with buffer size {}", bufferSize);
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
    static final byte MAGIC_ZSTD = (byte) 0x90;
    static final byte MAGIC_NONE = (byte) 0x02;
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

    public byte[] getHeaderBuffer() {
        return headerBuff;
    }

    public InputStream getInputStream() {
        return in;
    }

    private int refill() throws IOException {

        // read header
        boolean readFully = readFully(headerBuff, 0, HEADER_LENGTH);
        if (!readFully) {
            return -1;
        }

        // 1 byte - compression method (0x82 LZ4, 0x90 ZSTD, 0x02 uncompressed)
        final byte method = headerBuff[16];
        if (method != MAGIC && method != MAGIC_ZSTD && method != MAGIC_NONE) {
            throw new ClientException("Invalid compression method byte: '" + method + "'");
        }

        // 4 bytes - size of the compressed data including 9 bytes of the header
        int compressedSizeWithHeader = getInt32(headerBuff, 17);
        // 4 bytes - size of uncompressed data
        int uncompressedSize = getInt32(headerBuff, 21);

        int offset = 9;
        if (compressedSizeWithHeader < offset || uncompressedSize < 0) {
            throw new ClientException(ClickHouseUtils.format(
                    "Corrupted stream: block declares %s compressed and %s uncompressed bytes",
                    compressedSizeWithHeader, uncompressedSize));
        }

        final byte[] block =  new byte[compressedSizeWithHeader];
        block[0] = method;
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
        }
        decompress(method, block, offset, remaining, uncompressedSize);
        buffer.position(0);
        buffer.limit(uncompressedSize);
        return uncompressedSize;
    }

    /**
     * Decompresses a single block into {@link #buffer} with the codec the block header declares.
     *
     * @param method compression method byte of the block
     * @param block block, including its 9 bytes of header
     * @param offset offset of the compressed data in the block
     * @param compressedSize size of the compressed data
     * @param uncompressedSize size of the data after decompression
     */
    private void decompress(byte method, byte[] block, int offset, int compressedSize, int uncompressedSize) {
        switch (method) {
            case MAGIC:
                decompressor.decompress(ByteBuffer.wrap(block), offset, buffer, 0, uncompressedSize);
                break;
            case MAGIC_ZSTD:
                long decompressedSize;
                try {
                    decompressedSize = Zstd.decompressByteArray(buffer.array(), buffer.arrayOffset(),
                            uncompressedSize, block, offset, compressedSize);
                } catch (ZstdException e) {
                    throw new ClientException("Failed to decompress ZSTD block: " + e.getMessage(), e);
                } catch (LinkageError e) {
                    // the server picks the codec of the response, so ZSTD cannot be avoided by configuration
                    throw new ClientException("Server compressed the response with ZSTD but the native library of "
                            + "zstd-jni is not available on this platform", e);
                }
                if (decompressedSize != uncompressedSize) {
                    throw new ClientException(ClickHouseUtils.format(
                            "Corrupted stream: decompressed %s bytes while %s were expected",
                            decompressedSize, uncompressedSize));
                }
                break;
            default: // MAGIC_NONE
                if (compressedSize != uncompressedSize) {
                    throw new ClientException(ClickHouseUtils.format(
                            "Corrupted stream: uncompressed block holds %s bytes while %s were expected",
                            compressedSize, uncompressedSize));
                }
                System.arraycopy(block, offset, buffer.array(), buffer.arrayOffset(), uncompressedSize);
                break;
        }
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
        in.close();
    }
}
