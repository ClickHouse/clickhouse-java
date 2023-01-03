package com.clickhouse.client.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Wraps some byte arrays into an input stream.
 * It behaves in a zero copy way.
 */
public class ByteArraysInputStream extends InputStream {

    /**
     * An array of bytes that was provided by the creator of the stream.
     * Elements buf[0] through buf[count-1] are the only bytes that can ever be
     * read from the stream;
     */
    protected final List<byte[]> bufs;

    /**
     * Current buf index
     */
    protected int current;
    /**
     * position of current buf
     */
    protected int currentPos;

    /**
     * Position of bufs
     */
    protected int pos;

    /**
     * The currently marked position in the stream.
     * The current buffer position is set to this point by the
     * <code>reset()</code> method.
     */
    protected int mark = 0;

    /**
     * Total byte size in bufs.
     */
    protected int count;

    public ByteArraysInputStream(List<byte[]> bufs) {
        Objects.requireNonNull(bufs, "bufs");
        this.bufs = bufs;
        this.pos = 0;
        this.current = 0;
        this.currentPos = 0;
        int totalCount = 0;
        for (byte[] buf : bufs) {
            Objects.requireNonNull(buf, "element of bufs");
            totalCount += buf.length;
        }
        this.count = totalCount;
    }

    public static ByteArraysInputStream of(List<byte[]> bufs) {
        return new ByteArraysInputStream(bufs);
    }

    public static ByteArraysInputStream of(byte[] ...bufs) {
        return new ByteArraysInputStream(Arrays.asList(bufs));
    }

    /**
     * Reads the next byte of data from this input stream.
     */
    public synchronized int read() {
        if (pos >= count)
            return -1;
        while (currentPos == bufs.get(current).length) {
            current++;
            currentPos = 0;
        }
        int ret = bufs.get(current)[currentPos] & 0xff;

        currentPos++;
        pos++;

        return ret;
    }

    /**
     * Reads up to <code>len</code> bytes of data into an array of bytes
     * from this input stream.
     *
     * @return the number bytes read or -1 if read nothing.
     */
    public synchronized int read(byte b[], int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }

        if (pos >= count) {
            return -1;
        }

        int avail = count - pos;
        if (len > avail) {
            len = avail;
        }
        if (len <= 0) { // TODO remove
            return 0;
        }
        int remaining = len;
        for (int i=current; i<bufs.size(); i++) {
            byte[] buf = bufs.get(current);
            if (buf.length - currentPos >= remaining) {
                System.arraycopy(buf, currentPos, b, off, remaining);
                currentPos += remaining;
                break;
            } else {
                System.arraycopy(buf, currentPos, b, off, buf.length - currentPos);
                remaining -= (buf.length - currentPos);
                off += (buf.length - currentPos);
                current++;
                currentPos = 0;
            }
        }
        pos += len;
        return len;
    }

    /**
     * Skips <code>n</code> bytes of input from this input stream. Fewer
     * bytes might be skipped if the end of the input stream is reached.
     * The actual number <code>k</code>
     * of bytes to be skipped is equal to the smaller
     * of <code>n</code> and  <code>count-pos</code>.
     * The value <code>k</code> is added into <code>pos</code>
     * and <code>k</code> is returned.
     *
     * @return the actual number of bytes skipped.
     */
    public synchronized long skip(long n) {
        long k = count - pos;
        if (n < k) {
            k = n < 0 ? 0 : n;
        }

        long remaining = k;
        for (int i=current; i<bufs.size(); i++) {
            byte[] buf = bufs.get(i);
            if (buf.length - currentPos >= remaining) {
                currentPos += remaining;
                break;
            } else {
                remaining -= (buf.length - currentPos);
                current++;
                currentPos = 0;
            }
        }
        pos += k;
        return k;
    }

    /**
     * Returns the number of remaining bytes that can be read (or skipped over)
     * from this input stream.
     * <p>
     * The value returned is <code>count&nbsp;- pos</code>,
     * which is the number of bytes remaining to be read from the input buffer.
     *
     * @return  the number of remaining bytes that can be read (or skipped
     *          over) from this input stream without blocking.
     */
    public synchronized int available() {
        return count - pos;
    }

    public boolean markSupported() {
        return true;
    }

    /**
     * Mark the pos and you can reset to it.
     */
    public synchronized void mark(int readAheadLimit) {
        mark = pos;
    }

    /**
     * Resets the buffer to the marked position. The marked position
     * is 0 unless another position was marked or an offset was specified
     * in the constructor.
     */
    public synchronized void reset() {
        int remaining = mark;
        for (int i=0; i<bufs.size(); i++) {
            byte[] buf = bufs.get(i);
            if (buf.length >= remaining) {
                currentPos = remaining;
                current = i;
                break;
            } else {
                remaining -= buf.length;
            }
        }
        pos = mark;
    }

    public void close() throws IOException {
        // do nothing.
    }
}
