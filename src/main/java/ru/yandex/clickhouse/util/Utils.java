package ru.yandex.clickhouse.util;

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
        return list.toArray(new String[0]);
    }
}
