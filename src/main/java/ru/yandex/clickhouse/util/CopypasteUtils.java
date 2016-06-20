package ru.yandex.clickhouse.util;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jkee on 16.03.15.
 */
public class CopypasteUtils {

    private static final Logger log = Logger.of(CopypasteUtils.class);
    public static final Charset UTF_8 = Charset.forName("UTF-8");

    /////// Metrika code //////

    public static boolean startsWithIgnoreCase(String haystack, String pattern) {
        return haystack.substring(0, pattern.length()).equalsIgnoreCase(pattern);
    }

    /**
     * Оставляет от haystack только все части не в кавычках
     */
    public static String retainUnquoted(String haystack, char quoteChar) {
        StringBuilder sb = new StringBuilder();
        String[] split = splitWithoutEscaped(haystack, quoteChar, true);
        // нечетные - наши пациенты
        for (int i = 0; i < split.length; i++) {
            String s = split[i];
            if ((i & 1) == 0) {
                sb.append(s);
            }
        }
        return sb.toString();
    }

    public static String[] splitWithoutEscaped(String str, char separatorChar) {
        return splitWithoutEscaped(str, separatorChar, false);
    }

    /**
     * Не учитывает эскейпленные сепараторы
     */
    public static String[] splitWithoutEscaped(String str, char separatorChar, boolean retainEmpty) {
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
        return list.toArray(new String[list.size()]);
    }

    /**
     * раскукоживатель
     */
    public static String unEscapeString(String string) {
        if (isBlank(string)) return string;

        char current = 0;
        int length = string.length();
        StringBuilder sb = new StringBuilder(length + 4);
        for (int i = 0; i < length; i += 1) {
            current = string.charAt(i);
            if (current == '\\') {
                if (i + 1 >= length) {
                    return sb.toString();
                }
                if (string.charAt(i + 1) == 'u') {
                    if (i + 5 >= length) {
                        return sb.toString();
                    }
                    sb.append((char) Integer.parseInt(string.substring(i + 2, i + 6), 16));
                    //noinspection AssignmentToForLoopParameter
                    i += 4;
                } else {
                    sb.append(string.charAt(i + 1));
                }
                //noinspection AssignmentToForLoopParameter
                i++;
            } else {
                sb.append(current);
            }
        }

        return sb.toString();
    }

    /////// Apache StringUtils ////////

    public static boolean isBlank(final CharSequence cs) {
        int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    public static String join(final Iterable<?> iterable, final char separator) {

        Iterator<?> iterator = iterable.iterator();

        // handle null, zero and one elements before building a buffer
        if (iterator == null) {
            return null;
        }
        if (!iterator.hasNext()) {
            return "";
        }
        final Object first = iterator.next();
        if (!iterator.hasNext()) {
            return first == null ? "" : first.toString();
        }

        // two or more elements
        final StringBuilder buf = new StringBuilder(256); // Java default is 16, probably too small
        if (first != null) {
            buf.append(first);
        }

        while (iterator.hasNext()) {
            buf.append(separator);
            final Object obj = iterator.next();
            if (obj != null) {
                buf.append(obj);
            }
        }

        return buf.toString();
    }

    /////// Guava //////

    private static final int BUF_SIZE = 0x1000; // 4K

    public static String toString(InputStream in) throws IOException {
        return new String(toByteArray(in), UTF_8);
    }

    public static byte[] toByteArray(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        copy(in, out);
        return out.toByteArray();
    }

    public static long copy(InputStream from, OutputStream to)
            throws IOException {
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

    public static void close(Closeable closeable){
        if (closeable == null) return;
        try{
            closeable.close();
        } catch (IOException e){
            log.error("can not close stream: " + e.getMessage());
        }
    }

    public static void close(ResultSet rs){
        if (rs == null) return;
        try{
            rs.close();
        } catch (SQLException e){
            log.error("can not close resultset: " + e.getMessage());
        }
    }

}
