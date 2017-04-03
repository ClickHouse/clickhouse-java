package ru.yandex.clickhouse.util;

import com.google.common.io.LittleEndianDataOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import ru.yandex.clickhouse.response.ClickHouseLZ4Stream;

import java.io.IOException;
import java.io.OutputStream;

public class ClickHouseLZ4OutputStream extends OutputStream {
    private static final LZ4Factory factory = LZ4Factory.safeInstance();
    private final LittleEndianDataOutputStream dataWrapper;

    private byte[] currentBlock;
    private int pointer;
    private byte[] compressedBlock;
    private final LZ4Compressor compressor;

    public ClickHouseLZ4OutputStream(OutputStream stream, int maxCompressBlockSize) {
        dataWrapper = new LittleEndianDataOutputStream(stream);
        compressor = factory.fastCompressor();
        currentBlock = new byte[maxCompressBlockSize];
        compressedBlock = new byte[compressor.maxCompressedLength(maxCompressBlockSize)];
    }

    @Override
    public void write(int b) throws IOException {
        currentBlock[pointer] = (byte) b;
        pointer++;

        if (pointer == currentBlock.length) {
            writeBlock();
        }
    }

    @Override
    public void flush() throws IOException {
        if (pointer != 0){
            writeBlock();
        }
        dataWrapper.flush();
    }

    private void writeBlock() throws IOException {
        int compressed = compressor.compress(currentBlock, 0, pointer, compressedBlock, 0);
        ClickHouseBlockChecksum checksum = ClickHouseBlockChecksum.calculateForBlock((byte)ClickHouseLZ4Stream.MAGIC,
                compressed + 9, pointer, compressedBlock, compressed);
        dataWrapper.write(checksum.asBytes());
        dataWrapper.writeByte(ClickHouseLZ4Stream.MAGIC);
        dataWrapper.writeInt(compressed + 9); // compressed size with header
        dataWrapper.writeInt(pointer); // uncompressed size
        dataWrapper.write(compressedBlock, 0, compressed);
        pointer = 0;
    }
}
