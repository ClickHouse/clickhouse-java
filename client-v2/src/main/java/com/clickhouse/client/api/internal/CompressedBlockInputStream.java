package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.DataTransferException;
import com.clickhouse.data.ClickHouseByteUtils;
import com.clickhouse.data.ClickHouseCityHash;
import com.clickhouse.data.ClickHouseUtils;
import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CompressedBlockInputStream extends InputStream {

    private static final Logger LOG = LoggerFactory.getLogger(CompressedBlockInputStream.class);
    private final LZ4FastDecompressor decompressor;

    private final InputStream in;

    private ByteBuffer buffer;

    private final byte[] tmpBuffer = new byte[1];


    public CompressedBlockInputStream(InputStream in, LZ4FastDecompressor decompressor, int bufferSize) {
        super();
        Objects.requireNonNull(in, "in is null");
        Objects.requireNonNull(decompressor, "decompressor is null");
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be greater than 0");
        }
        LOG.debug("Using decompressor with buffer size {}", bufferSize);
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

    static final byte MAGIC_LZ4 = (byte) 0x82;
    static final byte MAGIC_ZSTD_3 = (byte) 0x90;
    static final byte MAGIC_NONE = (byte) 0x02;
    static final int HEADER_LENGTH = 25;
    static final int MAGIC_NUM_POS = 16;
    // ClickHouse DBMS_MAX_COMPRESSED_SIZE (CompressionInfo.h): exactly 1 GiB (2^30).
    // The server accepts this value and rejects one byte more, for both the compressed-size
    // and the decompressed-size fields. This is the protocol ceiling, not a normal block
    // (the server writes at most max_compress_block_size, 1 MiB by default).
    // Both fields are UInt32 and are read here as a signed int, so a value >= 2 GiB arrives
    // negative and fails the < 0 check. If the server raises DBMS_MAX_COMPRESSED_SIZE, raise
    // these caps as well; a valid larger block would otherwise be rejected as corrupted.
    // A limit >= 2 GiB also requires reading the fields as unsigned.
    static final int MAX_COMPRESSED_BLOCK_SIZE = 0x40000000;
    static final int MAX_UNCOMPRESSED_BLOCK_SIZE = 0x40000000;

    final byte[] headerBuff = new byte[HEADER_LENGTH];

    /**
     * Method ensures to read all bytes from the input stream.
     * In case of network connection it may be a case when not all bytes are read at once.
     * @throws IOException
     */
    private boolean readFully(byte[] b, int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            int count;
            try {
                count = in.read(b, off + n, len - n);
            } catch (EOFException ex) {
                // Same signal as read() == -1. A clean end (nothing read yet) must stay a
                // false return so refill can end the stream or report a truncated block.
                // Wrapping it here would hide that from the callers that branch on it.
                if (n == 0) {
                    return false;
                }
                throw new DataTransferException(ClickHouseUtils.format("Incomplete read: %s of %s", n, len), ex);
            } catch (IOException ex) {
                throw new DataTransferException(ClickHouseUtils.format("Incomplete read: %s of %s", n, len), ex);
            }
            if (count < 0) {
                if (n == 0) {
                    return false;
                }
                throw new DataTransferException(ClickHouseUtils.format("Incomplete read: %s of %s", n, len));
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

        byte magicNumber = headerBuff[MAGIC_NUM_POS];
        switch (magicNumber) {
            case MAGIC_LZ4:
            case MAGIC_ZSTD_3:
            case MAGIC_NONE:
                break;
            default:
                throw new ClientException("Invalid LZ4 magic byte: '" + magicNumber + "'");
        }

        // 4 bytes - size of the compressed data including 9 bytes of the header
        int compressedSizeWithHeader = getInt32(headerBuff, 17);
        // 4 bytes - size of uncompressed data
        int uncompressedSize = getInt32(headerBuff, 21);

        int offset = 9;
        if (compressedSizeWithHeader < offset || compressedSizeWithHeader > MAX_COMPRESSED_BLOCK_SIZE) {
            throw new DataTransferException("Corrupted stream: invalid compressed block size " + compressedSizeWithHeader);
        }
        if (uncompressedSize < 0 || uncompressedSize > MAX_UNCOMPRESSED_BLOCK_SIZE) {
            throw new DataTransferException("Corrupted stream: invalid uncompressed block size " + uncompressedSize);
        }

        final byte[] block =  new byte[compressedSizeWithHeader];
        block[0] = magicNumber;
        setInt32(block, 1, compressedSizeWithHeader);
        setInt32(block, 5, uncompressedSize);
        // compressed data: compressed_size - 9 bytes
        int remaining = compressedSizeWithHeader - offset;

        readFully = readFully(block, offset, remaining);
        if (!readFully) {
            throw new DataTransferException("Unexpected end of stream");
        }

        long[] real = ClickHouseCityHash.cityHash128(block, 0, compressedSizeWithHeader);
        if (real[0] != getInt64(headerBuff, 0) || real[1] != ClickHouseByteUtils.getInt64(headerBuff, 8)) {
            throw new ClientException("Corrupted stream: checksum mismatch");
        }

        if (buffer.capacity() < uncompressedSize) {
            buffer = ByteBuffer.allocate(uncompressedSize);
        }

        switch (magicNumber) {
            case MAGIC_LZ4: {
                int consumed = decompressor.decompress(block, offset, buffer.array(), 0, uncompressedSize);
                if (consumed != remaining) {
                    throw new DataTransferException("Corrupted stream: LZ4 compressed size mismatch (expected "
                            + remaining + ", consumed " + consumed + ")");
                }
                break;
            }
            case MAGIC_ZSTD_3: {
                long decompressed = Zstd.decompressByteArray(buffer.array(), 0, uncompressedSize, block, offset, remaining);
                if (decompressed != uncompressedSize) {
                    throw new DataTransferException("Corrupted stream: ZSTD decompressed size mismatch (expected "
                            + uncompressedSize + ", got " + decompressed + ")");
                }
                break;
            }
            case MAGIC_NONE:
                if (remaining != uncompressedSize) {
                    throw new DataTransferException("Corrupted stream: uncompressed block size mismatch (expected "
                            + uncompressedSize + ", got " + remaining + ")");
                }
                // block is not compressed - just put it.
                System.arraycopy(block, offset, buffer.array(), 0, uncompressedSize);
                break;
            default:
                throw new ClientException("bug: Should not reach here");
        }
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
        in.close();
    }
}
