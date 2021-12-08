package com.clickhouse.client.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseInputStream;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Reader from clickhouse in lz4.
 */
public class ClickHouseLZ4InputStream extends ClickHouseInputStream {
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();

    static final int MAGIC = 0x82;

    private final InputStream stream;

    private ByteBuffer currentBlock;
    private boolean closed;

    private boolean checkNext() throws IOException {
        if (currentBlock == null) {
            currentBlock = readNextBlock();
        }
        return currentBlock != null && currentBlock.hasRemaining();
    }

    // every block is:
    private ByteBuffer readNextBlock() throws IOException {
        int read = stream.read();
        if (read < 0) {
            return null;
        }

        byte[] bytes = new byte[16];
        bytes[0] = (byte) read;
        // checksum - 16 bytes.
        readFully(bytes, 1, 15);
        ClickHouseBlockChecksum expected = ClickHouseBlockChecksum.fromBytes(bytes);
        // header:
        // 1 byte - 0x82 (shows this is LZ4)
        int magic = readUnsignedByteFromInput();
        if (magic != MAGIC) {
            throw new IOException("Magic is not correct: " + magic);
        }

        readFully(bytes, 0, 8);
        // 4 bytes - size of the compressed data including 9 bytes of the header
        int compressedSizeWithHeader = BinaryStreamUtils.toInt32(bytes, 0);
        // 4 bytes - size of uncompressed data
        int uncompressedSize = BinaryStreamUtils.toInt32(bytes, 4);
        int compressedSize = compressedSizeWithHeader - 9; // header
        byte[] block = new byte[compressedSize];
        // compressed data: compressed_size - 9 байт.
        readFully(block, 0, block.length);

        ClickHouseBlockChecksum real = ClickHouseBlockChecksum.calculateForBlock((byte) magic, compressedSizeWithHeader,
                uncompressedSize, block, compressedSize);
        if (!real.equals(expected)) {
            throw new IllegalArgumentException("Checksum doesn't match: corrupted data.");
        }

        byte[] decompressed = new byte[uncompressedSize];
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        decompressor.decompress(block, 0, decompressed, 0, uncompressedSize);
        return ByteBuffer.wrap(decompressed);
    }

    private void readFully(byte b[], int off, int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }
        int n = 0;
        while (n < len) {
            int count = stream.read(b, off + n, len - n);
            if (count < 0) {
                try {
                    close();
                } catch (IOException e) {
                    // ignore
                }
                throw new EOFException();
            }
            n += count;
        }
    }

    private int readUnsignedByteFromInput() throws IOException {
        int ch = stream.read();
        if (ch < 0) {
            try {
                close();
            } catch (IOException e) {
                // ignore
            }
            throw new EOFException();
        }
        return ch;
    }

    public ClickHouseLZ4InputStream(InputStream stream) {
        this.stream = ClickHouseChecker.nonNull(stream, "InputStream");
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

        return currentBlock.get();
    }

    @Override
    public int available() throws IOException {
        if (closed) {
            return 0;
        }

        int estimated = stream.available();
        if (estimated == 0 && checkNext()) {
            estimated = currentBlock.remaining();
        }
        return estimated;
    }

    @Override
    public int read() throws IOException {
        return checkNext() ? 0xFF & currentBlock.get() : -1;
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
            int toCopy = Math.min(currentBlock.remaining(), len - copied);
            currentBlock.get(b, off, toCopy);
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

        if (byteLength > 8 && currentBlock.remaining() > byteLength) {
            int pos = currentBlock.position();
            ((Buffer) currentBlock).position(pos + byteLength);
            return charset.decode(ByteBuffer.wrap(currentBlock.array(), pos, byteLength)).toString();
        }

        return new String(readBytes(byteLength), charset);
    }
}
