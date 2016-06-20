package ru.yandex.clickhouse.copypaste;


import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Несинхронизированная быстрая версия {@link java.io.ByteArrayOutputStream}
 * @author Artur
 * @version $Id: FastByteArrayOutputStream.java 5083 2009-11-11 12:46:49Z dedmajor $
 * @since 07.05.2008
 */
public final class FastByteArrayOutputStream extends OutputStream {

    /**
     * The buffer where data is stored.
     */
    private byte[] buf;

    /**
     * The number of valid bytes in the buffer.
     */
    private int count;

    /**
     * Creates a new byte array output stream. The buffer capacity is
     * initially 32 bytes, though its size increases if necessary.
     */
    public FastByteArrayOutputStream() {
	this(1024);
    }

    /**
     * Creates a new byte array output stream, with a buffer capacity of
     * the specified size, in bytes.
     *
     * @param   size   the initial size.
     * @exception  IllegalArgumentException if size is negative.
     */
    public FastByteArrayOutputStream(int size) {
        super();
        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: "
                    + size);
        }
        buf = new byte[size];
    }

    private int ensureCapacity(int datalen) {
        int newcount = count + datalen;
        if (newcount > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        return newcount;
    }


    /**
     * Writes the specified byte to this byte array output stream.
     *
     * @param   b   the byte to be written.
     */
    @Override
    public void write(int b) {
	int newcount = ensureCapacity(1);
	buf[count] = (byte)b;
	count = newcount;
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this byte array output stream.
     *
     * @param   b     the data.
     * @param   off   the start offset in the data.
     * @param   len   the number of bytes to write.
     */
    @Override
    public void write(byte[] b, int off, int len) {
        if (off < 0 || off > b.length || len < 0 || off + len > b.length || off + len < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        int newcount = ensureCapacity(len);
        System.arraycopy(b, off, buf, count, len);
        count = newcount;
    }



    /**
     * Возвращает напрямую внутренний массив
     *
     * @return  the current contents of this output stream, as a byte array.
     */
    public byte[] toByteArray() {
        byte[] result = new byte[count];
        System.arraycopy(buf, 0, result, 0, count);
        return result;
    }

    public void writeTo(OutputStream output) throws IOException {
        output.write(buf, 0, count);
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return  the value of the <code>count</code> field, which is the number
     *          of valid bytes in this output stream.
     */
    public int size() {
	return count;
    }


    /**
     * Closing a <tt>ByteArrayOutputStream</tt> has no effect. The methods in
     * this class can be called after the stream has been closed without
     * generating an <tt>IOException</tt>.
     * <p>
     *
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * Копирует данные из Input потока
     * @param source Поток-источник данных
     * @param offset Смещение от начала данных в источнике
     * @param count Кол-во байт к копированию
     */
    public void copyFrom(FastByteArrayInputStream source, int offset, int count) {
        if (offset + count > source.getCount()) {
            throw new IndexOutOfBoundsException(
                    "Trying to copy data past the end of source"
                    + ", source.size=" + source.getCount()
                    + ", offset=" + offset + ", count=" + count
            );
        }
        byte[] srcBuf = source.getBuf();
        write(srcBuf, offset, count);
    }

    public void copyTo(OutputStream dest) throws IOException {
        dest.write(buf, 0, count);
    }

    public void copyTo(DataOutput dest) throws IOException {
        dest.write(buf, 0, count);
    }

    /**
     * Создает InputStream на основе тех же данных, которые уже записаны в этот поток,
     * без копирования данных в памяти.
     */
    public FastByteArrayInputStream convertToInputStream() {
        return new FastByteArrayInputStream(buf, count); 
    }

    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(buf, 0, count);
    }

    public byte[] getBuffer() {
        return buf;
    }


    public void reset() {
        count = 0;
    }
}

