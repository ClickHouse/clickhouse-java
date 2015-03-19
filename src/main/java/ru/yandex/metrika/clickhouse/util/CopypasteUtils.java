package ru.yandex.metrika.clickhouse.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jkee on 16.03.15.
 */
public class CopypasteUtils {

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

}
