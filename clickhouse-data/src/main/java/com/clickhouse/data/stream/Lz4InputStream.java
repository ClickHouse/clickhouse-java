package com.clickhouse.data.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.StreamCorruptedException;
import java.util.Arrays;
import java.util.LinkedList;

import com.clickhouse.data.ClickHouseByteUtils;
import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseCityHash;
import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHouseUtils;

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
    private final InputStream input;
    private final byte[] header;

    private byte[] compressedBlock;

    private boolean readFully(byte[] b, int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            int count = input.read(b, off + n, len - n);
            if (count < 0) {
                if (n == 0) {
                    return false;
                }
                throw new StreamCorruptedException(ClickHouseUtils.format(ERROR_INCOMPLETE_READ, n, len));
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
        int compressedSizeWithHeader = ClickHouseByteUtils.getInt32LE(header, 17);
        // 4 bytes - size of uncompressed data
        int uncompressedSize = ClickHouseByteUtils.getInt32LE(header, 21);
        int offset = 9;
        final byte[] block = compressedBlock.length >= compressedSizeWithHeader ? compressedBlock
                : (compressedBlock = new byte[compressedSizeWithHeader]);
        block[0] = header[16];
        ClickHouseByteUtils.setInt32LE(block, 1, compressedSizeWithHeader);
        ClickHouseByteUtils.setInt32LE(block, 5, uncompressedSize);
        // compressed data: compressed_size - 9 bytes
        if (!readFully(block, offset, compressedSizeWithHeader - offset)) {
            throw new StreamCorruptedException(
                    ClickHouseUtils.format(ERROR_INCOMPLETE_READ, 0, compressedSizeWithHeader - offset));
        }

        long[] real = ClickHouseCityHash.cityHash128(block, 0, compressedSizeWithHeader);
        if (real[0] != ClickHouseByteUtils.getInt64LE(header, 0)
                || real[1] != ClickHouseByteUtils.getInt64LE(header, 8)) {
            throw new InvalidObjectException("Checksum doesn't match: corrupted data.");
        }

        final byte[] buf = buffer.length >= uncompressedSize ? buffer : (buffer = new byte[uncompressedSize]);
        decompressor.decompress(block, offset, buf, 0, uncompressedSize);
        if (copyTo != null) {
            copyTo.write(buf, 0, uncompressedSize);
        }
        return limit = uncompressedSize;
    }

    public Lz4InputStream(InputStream stream) {
        this(null, stream, null);
    }

    public Lz4InputStream(ClickHousePassThruStream stream, InputStream input, Runnable postCloseAction) {
        super(stream, null, postCloseAction);

        this.decompressor = factory.fastDecompressor();
        this.input = ClickHouseChecker.nonNull(input, ClickHouseInputStream.TYPE_NAME);
        this.header = new byte[HEADER_LENGTH];

        this.compressedBlock = ClickHouseByteBuffer.EMPTY_BYTES;
    }

    @Override
    public ClickHouseByteBuffer readCustom(ClickHouseDataUpdater reader) throws IOException {
        if (reader == null) {
            return byteBuffer.reset();
        }
        ensureOpen();

        LinkedList<byte[]> list = new LinkedList<>();
        int length = 0;
        boolean more = true;
        while (more) {
            int remain = limit - position;
            if (remain < 1) {
                closeQuietly();
                more = false;
            } else {
                int read = reader.update(buffer, position, limit);
                if (read == -1) {
                    list.add(Arrays.copyOfRange(buffer, position, limit));
                    length += remain;
                    position = limit;
                    if (updateBuffer() < 1) {
                        closeQuietly();
                        more = false;
                    }
                } else {
                    if (read > 0) {
                        byte[] bytes = new byte[read];
                        System.arraycopy(buffer, position, bytes, 0, read);
                        list.add(bytes);
                        length += read;
                        position += read;
                    }
                    more = false;
                }
            }
        }
        return byteBuffer.update(list, 0, length);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                input.close();
            } finally {
                super.close();
            }
        }
    }
}
