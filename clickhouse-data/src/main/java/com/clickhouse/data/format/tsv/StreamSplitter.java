package com.clickhouse.data.format.tsv;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * We have a stream of bytes and a separator as an input. We split the stream by
 * the separator and pass the byte arrays to output.
 */
@Deprecated
public class StreamSplitter {
    private static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;
    private static final int buflen = 65536;

    // initial parameters
    private final InputStream delegate;
    private final byte sep;

    private byte[] buf;
    // position until which the buf is filled with values read from delegate
    private int posRead;
    // position until which the values from buf already passed out through next()
    private int posNext;

    private int markedRead;
    private int markedNext;

    private boolean readOnce;

    private boolean closed;

    public StreamSplitter(ByteFragment bf, byte sep) {
        this.delegate = bf.asStream();
        this.sep = sep;
        buf = new byte[bf.getLen()];
        readOnce = true;
    }

    public StreamSplitter(InputStream delegate, byte sep, int buflen) {
        this.delegate = delegate;
        this.sep = sep;
        buf = new byte[buflen];
    }

    public StreamSplitter(InputStream delegate, byte sep) {
        this(delegate, sep, buflen);
    }

    public ByteFragment next() throws IOException {
        // if sent out all that have read
        if (posNext >= posRead) {
            // need to read more from the stream
            int readBytes = readFromStream();
            if (readBytes <= 0) {
                // if everything was sent out and there is nothing left in the stream
                return null;
            }
        }
        // looking for the separator
        int positionSep;
        while ((positionSep = indexOf(buf, sep, posNext, posRead)) < posNext) {
            // read from stream till we find the separator
            int readBytes = readFromStream();
            if (readBytes <= 0) {
                // if there is nothing to read, return everything left as a result
                positionSep = posRead;
                break;
            }
        }
        // if the separator is found, return the fragment
        int fragmentStart = posNext;
        posNext = positionSep + 1;
        return new ByteFragment(buf, fragmentStart, positionSep - fragmentStart);
    }

    // if there is no separator in read but not sent fragment - read more data
    protected int readFromStream() throws IOException {
        if (readOnce) {
            if (posRead >= buf.length) {
                return -1;
            } else {
                int read = delegate.read(buf, posRead, buf.length - posRead);
                if (read > 0) {
                    posRead += read;
                }
                return read;
            }
        } else {
            if (posRead >= buf.length) { // buffer is filled
                shiftOrResize();
            }
            int read = delegate.read(buf, posRead, buf.length - posRead);
            if (read > 0) {
                posRead += read;
            }
            return read;
        }
    }

    // if we have read till the end of buffer, we have to create a new buffer
    // and move data by posNext (already send data position)
    // if there is no sent data and buffer is still full - expand the buffer
    private void shiftOrResize() {
        if (posNext > 0) {
            byte[] oldBuf = buf;
            buf = new byte[buf.length];
            System.arraycopy(oldBuf, posNext, buf, 0, oldBuf.length - posNext);
            posRead -= posNext;
            posNext = 0;
        } else {
            byte[] oldBuf = buf;
            int len = buf.length * 2;
            if (len > MAX_ARRAY_LENGTH) {
                len = MAX_ARRAY_LENGTH;
            }
            buf = new byte[len];
            System.arraycopy(oldBuf, 0, buf, 0, oldBuf.length);
        }
    }

    private static int indexOf(byte[] array, byte target, int start, int end) {
        for (int i = start; i < end; i++) {
            if (array[i] == target) {
                return i;
            }
        }
        return -1;
    }

    public void close() throws IOException {
        closed = true;
        delegate.close();
    }

    public boolean isClosed() throws IOException {
        return closed;
    }

    @Override
    public String toString() {
        String bufStr = new String(buf, StandardCharsets.UTF_8).trim();

        return "StreamSplitter{" + "delegate=" + delegate + ", sep=" + sep + ", buf=" + bufStr + ", posRead=" + posRead
                + ", posNext=" + posNext + ", readOnce=" + readOnce + '}';
    }

    public void mark() {
        markedRead = posRead;
        markedNext = posNext;
    }

    public void reset() {
        posRead = markedRead;
        posNext = markedNext;
    }
}
