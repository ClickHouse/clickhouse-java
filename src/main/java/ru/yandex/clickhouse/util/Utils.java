package ru.yandex.clickhouse.util;

import ru.yandex.clickhouse.util.apache.StringUtils;

import java.util.ArrayList;
import java.util.List;


public class Utils {

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

    public static String[] splitWithoutEscaped(String str, char separatorChar) {
        return splitWithoutEscaped(str, separatorChar, false);
    }

    /**
     * does not take into account escaped separators
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

    public static String unEscapeString(String string) {
        if (StringUtils.isBlank(string)) return string;

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
}
