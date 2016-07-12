package ru.yandex.clickhouse;


public class ClickHouseUtil {
    static void quoteInternals(String s, StringBuilder sb) {
        for(int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\0':
                    sb.append("\\0");
                    break;
                case '\'':
                    sb.append("\\'");
                    break;
                case '`':
                    sb.append("\\`");
                    break;
                default:
                    sb.append(c);
            }
        }
    }


    public static String quote(String s) {
        if (s == null) {
            return "NULL";
        }
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('\'');
        quoteInternals(s, sb);
        sb.append('\'');
        return sb.toString();
    }

    public static String quoteIdentifier(String s) {
        if (s == null) {
            throw new IllegalArgumentException("Can't quote null as identifier");
        }
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('`');
        quoteInternals(s, sb);
        sb.append('`');
        return sb.toString();
    }

}
