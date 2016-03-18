package ru.yandex.metrika.clickhouse.copypaste;

import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.IOException;
import java.io.InputStream;

/**
 * CONV-8365
 * Читалка из кликхауса в lz4
 * @author jkee
 */

public class ClickhouseLZ4Stream extends InputStream {

    private static final LZ4Factory factory = LZ4Factory.safeInstance();

    private static final int MAGIC = 0x82;

    private final InputStream stream;
    private final LittleEndianDataInputStream dataWrapper;

    private byte[] currentBlock;
    private int pointer;

    public ClickhouseLZ4Stream(InputStream stream) {
        this.stream = stream;
        dataWrapper = new LittleEndianDataInputStream(stream);
    }

    @Override
    public int read() throws IOException {
        if (!checkNext()) return -1;
        byte b = currentBlock[pointer];
        pointer += 1;
        return b & 0xFF;
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

        if (!checkNext()) return -1;

        int copied = 0;
        int targetPointer = off;
        while(copied != len) {
            int toCopy = Math.min(currentBlock.length - pointer, len - copied);
            System.arraycopy(currentBlock, pointer, b, targetPointer, toCopy);
            targetPointer += toCopy;
            pointer += toCopy;
            copied += toCopy;
            if (!checkNext()) { //закончили
                return copied;
            }
        }
        return copied;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    private boolean checkNext() throws IOException {
        if (currentBlock == null || pointer == currentBlock.length) {
            currentBlock = readNextBlock();
            pointer = 0;
        }
        return currentBlock != null;
    }

    //Каждый блок, это:
    private byte[] readNextBlock() throws IOException {
        int read = stream.read();
        if (read < 0) return null;

        byte[] checksum = new byte[16];
        checksum[0] = (byte)read;
        // Чексумма - 16 байт.
        dataWrapper.readFully(checksum, 1, 15);
        // Заголовок:
        // 1 байт - 0x82 (обозначает, что это LZ4)
        int magic = dataWrapper.readUnsignedByte();
        if (magic != MAGIC) throw new IOException("Magic is not correct: " + magic);
        // 4 байта - размер сжатых данных с учётом дополнительных 9 байт заголовка (compressed_size)
        int compressedSizeWithHeader = dataWrapper.readInt();
        // 4 байта - размер несжатых данных
        int uncompressedSize = dataWrapper.readInt();
        int compressedSize = compressedSizeWithHeader - 9; //header
        byte[] block = new byte[compressedSize];
        // Сжатые данные: compressed_size - 9 байт.
        dataWrapper.readFully(block);

        byte[] decompressed = new byte[uncompressedSize];
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        decompressor.decompress(block, 0, decompressed, 0, uncompressedSize);

        return decompressed;
    }


}
