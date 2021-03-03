package ru.yandex.clickhouse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ClickHouseUtil {
    private static final Map<Character, String> escapeMapping;

    static {
        Map<Character, String> map = new HashMap<>();
        map.put('\\', "\\\\");
        map.put('\n', "\\n");
        map.put('\t', "\\t");
        map.put('\b', "\\b");
        map.put('\f', "\\f");
        map.put('\r', "\\r");
        map.put('\0', "\\0");
        map.put('\'', "\\'");
        map.put('`', "\\`");
        escapeMapping = Collections.unmodifiableMap(map);
    }

    public static String escape(String s) {
        if (s == null) {
            return "\\N";
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            
            String escaped = escapeMapping.get(ch);
            if (escaped != null) {
                sb.append(escaped);
            } else {
                sb.append(ch);
            }
        }

        return sb.toString();
    }

    public static String quoteIdentifier(String s) {
        if (s == null) {
            throw new IllegalArgumentException("Can't quote null as identifier");
        }
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('`');
        sb.append(escape(s));
        sb.append('`');
        return sb.toString();
    }
}
