package com.clickhouse.client.stream;

import java.io.IOException;
import java.io.InputStream;

import com.clickhouse.client.ClickHouseByteBuffer;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.data.BinaryStreamUtils;
import com.clickhouse.client.data.ClickHouseCityHash;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Reader from clickhouse in lz4.
 */
public class Lz4InputStream extends AbstractByteArrayInputStream {
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();

    static final byte MAGIC = (byte) 0x82;
    static final int HEADER_LENGTH = 25;

    private final LZ4FastDecompressor decompressor;
    private final InputStream stream;
    private final byte[] header;

    private boolean readFully(byte[] b, int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            int count = stream.read(b, off + n, len - n);
            if (count < 0) {
                if (n == 0) {
                    return false;
                }
                throw new IOException(ClickHouseUtils.format(ERROR_INCOMPLETE_READ, n, len));
            }
            n += count;
        }

        return true;
    }

    @Override
    protected int updateBuffer() throws IOException {
        position = 0;

        // checksum(16 bytes) + 1 magic byte + header(8 bytes)
        if (!readFully(header, 0, HEADER_LENGTH)) {
            buffer = ClickHouseByteBuffer.EMPTY_BYTES;
            return limit = 0;
        } else if (header[16] != MAGIC) {
            // 1 byte - 0x82 (shows this is LZ4)
            throw new IOException(
                    ClickHouseUtils.format("Magic is not correct - expect [%d] but got [%d]", MAGIC, header[16]));
        }

        // 4 bytes - size of the compressed data including 9 bytes of the header
        int compressedSizeWithHeader = BinaryStreamUtils.toInt32(header, 17);
        // 4 bytes - size of uncompressed data
        int uncompressedSize = BinaryStreamUtils.toInt32(header, 21);
        int offset = 9;
        byte[] block = new byte[compressedSizeWithHeader];
        block[0] = header[16];
        BinaryStreamUtils.setInt32(block, 1, compressedSizeWithHeader);
        BinaryStreamUtils.setInt32(block, 5, uncompressedSize);
        // compressed data: compressed_size - 9 bytes
        if (!readFully(block, offset, compressedSizeWithHeader - offset)) {
            throw new IOException(ClickHouseUtils.format(ERROR_INCOMPLETE_READ, 0, compressedSizeWithHeader - offset));
        }

        long[] real = ClickHouseCityHash.cityHash128(block, 0, block.length);
        if (real[0] != BinaryStreamUtils.toInt64(header, 0) || real[1] != BinaryStreamUtils.toInt64(header, 8)) {
            throw new IOException("Checksum doesn't match: corrupted data.");
        }

        buffer = new byte[uncompressedSize];
        decompressor.decompress(block, offset, buffer, 0, uncompressedSize);
        if (copyTo != null) {
            copyTo.write(buffer);
        }
        return limit = buffer.length;
    }

    public Lz4InputStream(InputStream stream) {
        this(stream, null);
    }

    public Lz4InputStream(InputStream stream, Runnable postCloseAction) {
        super(null, postCloseAction);

        this.decompressor = factory.fastDecompressor();
        this.stream = ClickHouseChecker.nonNull(stream, "InputStream");
        this.header = new byte[HEADER_LENGTH];
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                stream.close();
            } finally {
                super.close();
            }
        }
    }
}
