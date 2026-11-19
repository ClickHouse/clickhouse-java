package com.clickhouse.client.cli;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** A growable byte buffer for the text that the shim writes to stdout. Text is written as UTF-8. */
final class TsvOutput {
    private byte[] buf = new byte[8192];
    private int len;

    int size() {
        return len;
    }

    void reset() {
        len = 0;
    }

    /** Removes the bytes after position size, for a value that failed to render. */
    void truncate(int size) {
        len = size;
    }

    TsvOutput write(int b) {
        ensure(1);
        buf[len++] = (byte) b;
        return this;
    }

    TsvOutput write(byte[] b) {
        ensure(b.length);
        System.arraycopy(b, 0, buf, len, b.length);
        len += b.length;
        return this;
    }

    TsvOutput write(String s) {
        boolean ascii = true;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) >= 0x80) {
                ascii = false;
                break;
            }
        }
        if (!ascii) {
            return write(s.getBytes(StandardCharsets.UTF_8));
        }
        ensure(s.length());
        for (int i = 0; i < s.length(); i++) {
            buf[len++] = (byte) s.charAt(i);
        }
        return this;
    }

    void writeTo(OutputStream out) throws IOException {
        out.write(buf, 0, len);
    }

    byte[] toByteArray() {
        return Arrays.copyOf(buf, len);
    }

    @Override
    public String toString() {
        return new String(buf, 0, len, StandardCharsets.UTF_8);
    }

    private void ensure(int n) {
        if (len + n > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length * 2, len + n));
        }
    }
}
