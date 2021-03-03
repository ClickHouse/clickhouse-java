package ru.yandex.clickhouse.util;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Utils {
    private static final int BUF_SIZE = 0x1000; // 4K
    private static final Map<Class<?>, Class<?>> classToPrimitive;
    
    static {
        Map<Class<?>, Class<?>> map = new HashMap<>();
        map.put(Boolean.class, boolean.class);
        map.put(Byte.class, byte.class);
        map.put(Character.class, char.class);
        map.put(Double.class, double.class);
        map.put(Float.class, float.class);
        map.put(Integer.class, int.class);
        map.put(Long.class, long.class);
        map.put(Short.class, short.class);
        map.put(Void.class, void.class);

        classToPrimitive = Collections.unmodifiableMap(map);
    }

    public static <T> Class<T> unwrap(Class<T> type) {
        @SuppressWarnings("unchecked")
        Class<T> unwrapped = (Class<T>) classToPrimitive.get(Objects.requireNonNull(type));
        return (unwrapped == null) ? type : unwrapped;
    }

    public static String toString(InputStream in) throws IOException {
        return new String(toByteArray(in), StandardCharsets.UTF_8);
    }

    public static byte[] toByteArray(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        copy(in, out);
        return out.toByteArray();
    }

    public static long copy(InputStream from, OutputStream to) throws IOException {
        byte[] buf = new byte[BUF_SIZE];
        long total = 0;
        while (true) {
            int r = from.read(buf);
            if (r == -1) {
                break;
            }
            to.write(buf, 0, r);
            total += r;
        }
        return total;
    }
    
    public static boolean isNullOrEmptyString(String str) {
        return str == null || str.isEmpty();
    }

    public static void checkArgument(byte[] value, int length) {
        if (value.length > length) {
            throw new IllegalArgumentException(
                    new StringBuilder().append("Given byte array should NOT greater than ").append(length).toString());
        }
    }

    public static void checkArgument(int value, int minValue) {
        if (value < minValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should NOT less than ").append(minValue).toString());
        }
    }

    public static void checkArgument(long value, long minValue) {
        if (value < minValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should NOT less than ").append(minValue).toString());
        }
    }

    public static void checkArgument(int value, int minValue, int maxValue) {
        if (value < minValue || value > maxValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should between ").append(minValue).append(" and ").append(maxValue).toString());
        }
    }

    public static void checkArgument(long value, long minValue, long maxValue) {
        if (value < minValue || value > maxValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should between ").append(minValue).append(" and ").append(maxValue).toString());
        }
    }

    public static void checkArgument(BigInteger value, BigInteger minValue) {
        if (value.compareTo(minValue) < 0) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should greater than ").append(minValue).toString());
        }
    }

    public static void checkArgument(BigInteger value, BigInteger minValue, BigInteger maxValue) {
        if (value.compareTo(minValue) < 0 || value.compareTo(maxValue) > 0) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should between ").append(minValue).append(" and ").append(maxValue).toString());
        }
    }

    public static void readFully(DataInputStream in, byte[] b) throws IOException {
        readFully(in, b, 0, b.length);
    }

    public static void readFully(DataInputStream in, byte[] b, int off, int len) throws IOException {
        Objects.requireNonNull(in);
        Objects.requireNonNull(b);
        if (len < 0 || off < 0) {
            throw new IndexOutOfBoundsException(String.format("length (%s) and offset (%s) cannot be negative", len, off));
        }
        int end = off + len;
        if (end < off || end > b.length) {
            throw new IndexOutOfBoundsException(String.format("offset (%s) should less than length (%s) and buffer length (%s)", off, len, b.length));
        }

        int total = 0;
        while (total < len) {
            int result = in.read(b, off + total, len - total);
            if (result == -1) {
                break;
            }
            total += result;
        }

        if (total != len) {
            throw new EOFException(
                    "reached end of stream after reading " + total + " bytes; " + len + " bytes expected");
        }
    }

    public static int readUnsignedShort(DataInputStream inputStream) throws IOException {
        byte b1 = (byte) inputStream.readUnsignedByte();
        byte b2 = (byte) inputStream.readUnsignedByte();

        return (b2 & 0xFF) << 8 | (b1 & 0xFF);
    }

    public static int readInt(DataInputStream inputStream) throws IOException {
        byte b1 = (byte) inputStream.readUnsignedByte();
        byte b2 = (byte) inputStream.readUnsignedByte();
        byte b3 = (byte) inputStream.readUnsignedByte();
        byte b4 = (byte) inputStream.readUnsignedByte();

        return b4 << 24 | (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);
    }

    public static long readLong(DataInputStream inputStream) throws IOException {
        byte b1 = (byte) inputStream.readUnsignedByte();
        byte b2 = (byte) inputStream.readUnsignedByte();
        byte b3 = (byte) inputStream.readUnsignedByte();
        byte b4 = (byte) inputStream.readUnsignedByte();
        byte b5 = (byte) inputStream.readUnsignedByte();
        byte b6 = (byte) inputStream.readUnsignedByte();
        byte b7 = (byte) inputStream.readUnsignedByte();
        byte b8 = (byte) inputStream.readUnsignedByte();

        return (b8 & 0xFFL) << 56
            | (b7 & 0xFFL) << 48
            | (b6 & 0xFFL) << 40
            | (b5 & 0xFFL) << 32
            | (b4 & 0xFFL) << 24
            | (b3 & 0xFFL) << 16
            | (b2 & 0xFFL) << 8
            | (b1 & 0xFFL);
    }

    public static BigInteger readLongAsBigInteger(DataInputStream inputStream) throws IOException {
        byte b1 = (byte) inputStream.readUnsignedByte();
        byte b2 = (byte) inputStream.readUnsignedByte();
        byte b3 = (byte) inputStream.readUnsignedByte();
        byte b4 = (byte) inputStream.readUnsignedByte();
        byte b5 = (byte) inputStream.readUnsignedByte();
        byte b6 = (byte) inputStream.readUnsignedByte();
        byte b7 = (byte) inputStream.readUnsignedByte();
        byte b8 = (byte) inputStream.readUnsignedByte();

        return new BigInteger(new byte[] { 0, b8, b7, b6, b5, b4, b3, b2, b1 });
    }

    public static int readUnsignedLeb128(DataInputStream inputStream) throws IOException {
		int value = 0;
		int read;
		int count = 0;
		do {
			read = inputStream.readUnsignedByte() & 0xff;
			value |= (read & 0x7f) << (count * 7);
			count++;
		} while (((read & 0x80) == 0x80) && count < 5);

		if ((read & 0x80) == 0x80) {
			throw new IOException("invalid LEB128 sequence");
		}
		return value;
	}

    public static void writeShort(DataOutputStream outputStream, int value) throws IOException {
        outputStream.write(0xFF & value);
        outputStream.write(0xFF & (value >> 8));
    }

    public static void writeInt(DataOutputStream outputStream, int value) throws IOException {
        outputStream.write(0xFF & value);
        outputStream.write(0xFF & (value >> 8));
        outputStream.write(0xFF & (value >> 16));
        outputStream.write(0xFF & (value >> 24));
    }

    public static void writeLong(DataOutputStream outputStream, long value) throws IOException {
        outputStream.write((int) (0xFF & value));
        outputStream.write((int) (0xFF & (value >> 8)));
        outputStream.write((int) (0xFF & (value >> 16)));
        outputStream.write((int) (0xFF & (value >> 24)));
        outputStream.write((int) (0xFF & (value >> 32)));
        outputStream.write((int) (0xFF & (value >> 40)));
        outputStream.write((int) (0xFF & (value >> 48)));
        outputStream.write((int) (0xFF & (value >> 56)));
    }

    public static BigInteger toBigInteger(BigDecimal num, int scale) {
        BigDecimal ten = BigDecimal.valueOf(10);
        BigDecimal s = ten.pow(scale);
        return num.multiply(s).toBigInteger();
    }

    public static boolean startsWithIgnoreCase(String haystack, String pattern) {
        return haystack.substring(0, pattern.length()).equalsIgnoreCase(pattern);
    }

    public static String retainUnquoted(String haystack, char quoteChar) {
        StringBuilder sb = new StringBuilder();
        String[] split = splitWithoutEscaped(haystack, quoteChar, true);
        for (int i = 0; i < split.length; i++) {
            String s = split[i];
            if ((i & 1) == 0) {
                sb.append(s);
            }
        }
        return sb.toString();
    }

    /**
     * Does not take into account escaped separators
     *
     * @param str  the String to parse, may be null
     * @param separatorChar  the character used as the delimiter
     * @param retainEmpty if it is true, result can contain empty strings
     * @return string array
     */
    private static String[] splitWithoutEscaped(String str, char separatorChar, boolean retainEmpty) {
        int len = str.length();
        if (len == 0) {
            return new String[0];
        }
        List<String> list = new ArrayList<String>();
        int i = 0;
        int start = 0;
        boolean match = false;
        while (i < len) {
            if (str.charAt(i) == '\\') {
                match = true;
                i += 2;
            } else if (str.charAt(i) == separatorChar) {
                if (retainEmpty || match) {
                    list.add(str.substring(start, i));
                    match = false;
                }
                start = ++i;
            } else {
                match = true;
                i++;
            }
        }
        if (retainEmpty || match) {
            list.add(str.substring(start, i));
        }
        return list.toArray(new String[0]);
    }
}
