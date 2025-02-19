package com.clickhouse.data.format.tsv;

import java.io.IOException;
import java.io.InputStream;

/**
 * Not synchronized quick version of {@link java.io.ByteArrayInputStream}
 */
@Deprecated
public final class FastByteArrayInputStream extends InputStream {
    private final byte[] buf;

    private int pos;

    private final int count;

    public FastByteArrayInputStream(byte[] buf) {
        this.buf = buf;
        pos = 0;
        count = buf.length;
    }

    /**
     * Special constructor fo creating InputStream over not fully filled array
     * 
     * @param buf   byte array
     * @param count number of filled elements
     */
    public FastByteArrayInputStream(byte[] buf, int count) {
        this.buf = buf;
        pos = 0;
        this.count = count;
    }

    @Override
    public int read() {
        return pos < count ? buf[pos++] & 0xff : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (pos >= count) {
            return -1;
        }
        if (pos + len > count) {
            // noinspection AssignmentToMethodParameter
            len = count - pos;
        }
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }

    @Override
    public long skip(long n) {
        if (pos + n > count) {
            // noinspection AssignmentToMethodParameter
            n = (long) count - pos;
        }
        if (n < 0L) {
            return 0L;
        }
        pos += (int) n;
        return n;
    }

    @Override
    public int available() {
        return count - pos;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void close() throws IOException {
    }

    public int getPos() {
        return pos;
    }

    public int getCount() {
        return count;
    }

    public byte[] getBuf() {
        return buf;
    }

    public byte[] getData() {
        if (buf.length > count) {
            byte[] data = new byte[count];
            System.arraycopy(buf, 0, data, 0, count);
            return data;
        } else {
            return buf;
        }
    }

    @Override
    public void reset() { // NOSONAR
        pos = 0;
    }
}
