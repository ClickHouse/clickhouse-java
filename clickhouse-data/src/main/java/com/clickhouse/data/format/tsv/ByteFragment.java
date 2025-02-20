package com.clickhouse.data.format.tsv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

@Deprecated
public class ByteFragment {

    protected final byte[] buf;
    protected final int start;
    protected final int len;
    private static final ByteFragment EMPTY = new ByteFragment(new byte[0], 0, 0);

    public ByteFragment(byte[] buf, int start, int len) {
        this.buf = buf;
        this.start = start;
        this.len = len;
    }

    public static ByteFragment fromString(String str) {
        // https://bugs.openjdk.java.net/browse/JDK-6219899
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        return new ByteFragment(bytes, 0, bytes.length);
    }

    public String asString() {
        return new String(buf, start, len, StandardCharsets.UTF_8);
    }

    public String asString(boolean unescape) {
        if (unescape) {
            if (isNull()) {
                return null;
            }
            return new String(unescape(), StandardCharsets.UTF_8);
        } else {
            return asString();
        }
    }

    public boolean isNull() {
        // \N
        return len == 2 && buf[start] == '\\' && buf[start + 1] == 'N';
    }

    public boolean isEmpty() {
        return len == 0;
    }

    public boolean isNaN() {
        // nan
        return len == 3 && buf[start] == 'n' && buf[start + 1] == 'a' && buf[start + 2] == 'n';
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("ByteFragment{[");
        for (byte b1 : buf) {
            if (b1 == '\t') {
                b.append("<TAB>");
            } else {
                b.append((char) b1);
            }
        }
        b.append(']');
        b.append(", start=" + start + ", len=" + len + '}');
        return b.toString();
    }

    public ByteFragment[] split(byte sep) {
        StreamSplitter ss = new StreamSplitter(this, sep);
        int c = count(sep) + 1;
        ByteFragment[] res = new ByteFragment[c];
        try {
            int i = 0;
            ByteFragment next = null;
            while ((next = ss.next()) != null) {
                res[i++] = next;
            }
        } catch (IOException ignore) {
        }
        if (res[c - 1] == null) {
            res[c - 1] = ByteFragment.EMPTY;
        }
        return res;
    }

    // [45, 49, 57, 52, 49, 51, 56, 48, 57, 49, 52, 9, 9, 50, 48, 49, 50, 45, 48,
    // 55, 45, 49, 55, 32, 49, 51, 58, 49, 50, 58, 50, 49, 9, 49, 50, 49, 50, 55,
    // 53, 53, 9, 50, 57, 57, 57, 55, 55, 57, 57, 55, 56, 9, 48, 9, 52, 48, 57, 49,
    // 57, 55, 52, 49, 49, 51, 50, 56, 53, 53, 50, 54, 57, 51, 9, 51, 9, 54, 9, 50,
    // 48, 9, 48, 92, 48, 9, 104, 116, 116, 112, 58, 47, 47, 119, 119, 119, 46, 97,
    // 118, 105, 116, 111, 46, 114, 117, 47, 99, 97, 116, 97, 108, 111, 103, 47,
    // 103, 97, 114, 97, 122, 104, 105, 95, 105, 95, 109, 97, 115, 104, 105, 110,
    // 111, 109, 101, 115, 116, 97, 45, 56, 53, 47, 116, 97, 116, 97, 114, 115, 116,
    // 97, 110, 45, 54, 53, 48, 49, 51, 48, 47, 112, 97, 103, 101, 56, 9, 104, 116,
    // 116, 112, 58, 47, 47, 119, 119, 119, 46, 97, 118, 105, 116, 111, 46, 114,
    // 117, 47, 99, 97, 116, 97, 108, 111, 103, 47, 103, 97, 114, 97, 122, 104, 105,
    // 95, 105, 95, 109, 97, 115, 104, 105, 110, 111, 109, 101, 115, 116, 97, 45,
    // 56, 53, 47, 116, 97, 116, 97, 114, 115, 116, 97, 110, 45, 54, 53, 48, 49, 51,
    // 48, 47, 112, 97, 103, 101, 55, 9, 48, 9, 48, 9, 50, 56, 53, 55, 48, 56, 48,
    // 9, 45, 49, 9, 48, 9, 9, 48, 9, 48, 9, 48, 9, 45, 49, 9, 48, 48, 48, 48, 45,
    // 48, 48, 45, 48, 48, 32, 48, 48, 58, 48, 48, 58, 48, 48, 9, 9, 48, 9, 48, 9,
    // 103, 9, 45, 49, 9, 45, 49, 9, 45, 49, 9]
    public ByteArrayInputStream asStream() {
        return new ByteArrayInputStream(buf, start, len);
    }

    private int count(byte sep) {
        int res = 0;
        for (int i = start; i < start + len; i++) {
            if (buf[i] == sep) {
                res++;
            }
        }
        return res;
    }

    public int getLen() {
        return len;
    }

    // "\0" => 0
    // "\r" => 13
    // "\n" => 10
    // "\\" => 92
    // "\'" => 39
    // "\b" => 8
    // "\f" => 12
    // "\t" => 9
    // null
    // "\N" => 0
    private static final byte[] convert = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 0.. 9
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 10..19
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 20..29
            -1, -1, -1, -1, -1, -1, -1, -1, -1, 39, // 30..39
            -1, -1, -1, -1, -1, -1, -1, -1, 0, -1, // 40..49
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 50..59
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 60..69
            -1, -1, -1, -1, -1, -1, -1, -1, 0, -1, // 70..79
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 80..89
            -1, -1, 92, -1, -1, -1, -1, -1, 8, -1, // 90..99
            -1, -1, 12, -1, -1, -1, -1, -1, -1, -1, // 100..109
            10, -1, -1, -1, 13, -1, 9, -1, -1, -1, // 110..119
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 120..129
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, };
    // [0xb6][0xfe][0x7][0x1][0xd8][0xd6][0x94][0x80][0x5]\0 html.
    // [-74,-2,7,1,-40,-42,-108,-128,5,0] real value
    // [-74,-2,7,1,-40,-42,-108,-128,5,92,48] parsed value

    public byte[] unescape() {
        int resLen = 0;
        {
            boolean prevSlash = false;
            for (int i = start; i < start + len; i++) {
                if (prevSlash) {
                    resLen++;
                    prevSlash = false;
                } else {
                    if (buf[i] == 92) { // slash character
                        prevSlash = true;
                    } else {
                        resLen++;
                    }
                }
            }
        }
        if (resLen == len) {
            return getBytesCopy();
        }
        byte[] res = new byte[resLen];
        int index = 0;
        {
            boolean prevSlash = false;
            for (int i = start; i < start + len; i++) {
                if (prevSlash) {
                    prevSlash = false;
                    res[index++] = convert[buf[i]];

                } else {
                    if (buf[i] == 92) { // slash character
                        prevSlash = true;
                    } else {
                        res[index++] = buf[i];
                    }
                }

            }
        }
        return res;
    }

    final static byte[] reverse;
    static {
        reverse = new byte[convert.length];
        for (int i = 0; i < convert.length; i++) {
            reverse[i] = -1;
            byte c = convert[i];
            if (c != -1) {
                reverse[c] = (byte) i;
            }
        }
    }

    public static void escape(byte[] bytes, OutputStream stream) throws IOException {
        for (byte b : bytes) {
            if (b < 0 || b >= reverse.length) {
                stream.write(b);
            } else {
                byte converted = reverse[b];
                if (converted != -1) {
                    stream.write(92);
                    stream.write(converted);
                } else {
                    stream.write(b);
                }
            }
        }
    }

    private byte[] getBytesCopy() {
        byte[] bytes = new byte[len];
        System.arraycopy(buf, start, bytes, 0, len);
        return bytes;
    }

    public int length() {
        return len;
    }

    public int charAt(int i) {
        return buf[start + i];
    }

    public ByteFragment subseq(int start, int len) {
        if (start < 0 || start + len > this.len) {
            throw new IllegalArgumentException(
                    "arg start,len=" + (start + "," + len) + " while this start,len=" + (this.start + "," + this.len));
        }
        return new ByteFragment(buf, this.start + start, len);
    }
}
