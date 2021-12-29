package com.clickhouse.client.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseUtils;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Reader from clickhouse in lz4.
 */
public class ClickHouseLZ4InputStream extends ClickHouseInputStream {
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();

    static final byte MAGIC = (byte) 0x82;
    static final int HEADER_LENGTH = 25;

    private final LZ4FastDecompressor decompressor;
    private final InputStream stream;
    private final byte[] header;

    private byte[] currentBlock;
    private int position;
    private boolean closed;

    private boolean checkNext() throws IOException {
        if (!closed && position >= currentBlock.length) {
            currentBlock = readNextBlock();
        }
        return currentBlock.length > 0;
    }

    // every block is:
    private byte[] readNextBlock() throws IOException {
        position = 0;

        // checksum(16 bytes) + 1 magic byte + header(8 bytes)
        if (!readFully(header, 0, HEADER_LENGTH)) {
            return EMPTY_BYTES;
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
            throw new EOFException();
        }

        long[] real = ClickHouseCityHash.cityHash128(block, 0, block.length);
        if (real[0] != BinaryStreamUtils.toInt64(header, 0) || real[1] != BinaryStreamUtils.toInt64(header, 8)) {
            throw new IllegalArgumentException("Checksum doesn't match: corrupted data.");
        }

        byte[] decompressed = new byte[uncompressedSize];
        decompressor.decompress(block, offset, decompressed, 0, uncompressedSize);
        return decompressed;
    }

    private boolean readFully(byte[] b, int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            int count = stream.read(b, off + n, len - n);
            if (count < 0) {
                if (n == 0) {
                    return false;
                }
                throw new EOFException();
            }
            n += count;
        }

        return true;
    }

    public ClickHouseLZ4InputStream(InputStream stream) {
        this.decompressor = factory.fastDecompressor();
        this.stream = ClickHouseChecker.nonNull(stream, "InputStream");
        this.header = new byte[HEADER_LENGTH];

        this.currentBlock = EMPTY_BYTES;
        this.position = 0;
        this.closed = false;
    }

    @Override
    public byte readByte() throws IOException {
        if (!checkNext()) {
            try {
                close();
            } catch (IOException e) {
                // ignore
            }
            throw new EOFException();
        }

        return currentBlock[position++];
    }

    @Override
    public int available() throws IOException {
        if (closed) {
            return 0;
        }

        int estimated = currentBlock.length - position;
        if (estimated == 0 && checkNext()) {
            estimated = currentBlock.length - position;
        }
        return estimated;
    }

    @Override
    public int read() throws IOException {
        return checkNext() ? 0xFF & currentBlock[position++] : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        if (!checkNext()) {
            return -1;
        }

        int copied = 0;
        while (copied != len) {
            int toCopy = Math.min(currentBlock.length - position, len - copied);
            System.arraycopy(currentBlock, position, b, off, toCopy);
            position += toCopy;
            off += toCopy;
            copied += toCopy;

            if (!checkNext()) {
                break;
            }
        }

        return copied;
    }

    @Override
    public void close() throws IOException {
        try {
            stream.close();
        } finally {
            closed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public String readString(int byteLength, Charset charset) throws IOException {
        if (byteLength < 1) {
            return "";
        } else if (!checkNext()) {
            try {
                close();
            } catch (IOException e) {
                // ignore
            }
            throw new EOFException();
        }

        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }

        if (currentBlock.length - position > byteLength) {
            int offset = position;
            position += byteLength;
            return new String(currentBlock, offset, byteLength, charset);
        }

        return new String(readBytes(byteLength), charset);
    }
}
