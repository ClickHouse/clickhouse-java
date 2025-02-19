package com.clickhouse.data.stream;

import java.io.IOException;
import java.io.OutputStream;

import com.clickhouse.data.ClickHouseByteUtils;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseCityHash;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;

import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

@Deprecated
public class Lz4OutputStream extends AbstractByteArrayOutputStream {
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();
    private static final Logger log = LoggerFactory.getLogger(Lz4OutputStream.class);
    private final OutputStream output;

    private final LZ4Compressor compressor;
    private final byte[] compressedBlock;

    @Override
    protected void flushBuffer() throws IOException {
        log.debug("flushBuffer [{}:{}]", 0, position);
        if (position == 0) {
            log.debug("flushBuffer: nothing to flush");
            return;
        }
        byte[] block = compressedBlock;
        block[16] = Lz4InputStream.MAGIC;
        int compressed = compressor.compress(buffer, 0, position, block, 25);
        int compressedSizeWithHeader = compressed + 9;
        ClickHouseByteUtils.setInt32(block, 17, compressedSizeWithHeader); // compressed size with header
        ClickHouseByteUtils.setInt32(block, 21, position); // uncompressed size
        long[] hash = ClickHouseCityHash.cityHash128(block, 16, compressedSizeWithHeader);
        ClickHouseByteUtils.setInt64(block, 0, hash[0]);
        ClickHouseByteUtils.setInt64(block, 8, hash[1]);
        output.write(block, 0, compressed + 25);
        position = 0;
    }

    @Override
    protected void flushBuffer(byte[] bytes, int offset, int length) throws IOException {
        log.debug("flushBuffer [{}:{}]", offset, length);
        int maxLen = compressor.maxCompressedLength(length) + 15;
        byte[] block = maxLen <= compressedBlock.length ? compressedBlock : new byte[maxLen];
        block[16] = Lz4InputStream.MAGIC;

        int compressed = compressor.compress(bytes, offset, length, block, 25);
        int compressedSizeWithHeader = compressed + 9;
        ClickHouseByteUtils.setInt32(block, 17, compressedSizeWithHeader);
        ClickHouseByteUtils.setInt32(block, 21, length);
        long[] hash = ClickHouseCityHash.cityHash128(block, 16, compressedSizeWithHeader);
        ClickHouseByteUtils.setInt64(block, 0, hash[0]);
        ClickHouseByteUtils.setInt64(block, 8, hash[1]);
        output.write(block, 0, compressed + 25);
    }

    public Lz4OutputStream(OutputStream out, int maxCompressBlockSize, Runnable postCloseAction) {
        this(null, out, -1, maxCompressBlockSize, postCloseAction);
    }

    public Lz4OutputStream(OutputStream out, int compressLevel, int maxCompressBlockSize, Runnable postCloseAction) {
        this(null, out, compressLevel, maxCompressBlockSize, postCloseAction);
    }

    public Lz4OutputStream(ClickHousePassThruStream stream, OutputStream out, int compressLevel,
            int maxCompressBlockSize, Runnable postCloseAction) {
        super(stream, maxCompressBlockSize, postCloseAction);

        output = ClickHouseChecker.nonNull(out, ClickHouseOutputStream.TYPE_NAME);

        if (compressLevel < 0) {
            compressor = factory.fastCompressor();
        } else {
            compressor = factory.highCompressor(compressLevel);
        }
        // reserve the first 9 bytes for calculating checksum
        compressedBlock = new byte[compressor.maxCompressedLength(maxCompressBlockSize) + 15];
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();

        if (position > 0) {
            flushBuffer();
        }
        output.flush();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                // flush before closing the inner output stream
                super.close();
            } finally {
                output.close();
            }
        }
    }
}
