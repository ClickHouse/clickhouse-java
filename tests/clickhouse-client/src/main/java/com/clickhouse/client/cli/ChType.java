package com.clickhouse.client.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A parsed ClickHouse data type, for example {@code Array(Nullable(DateTime64(3, 'UTC')))}.
 *
 * <p>The shim parses the type name that the server sends itself, so that the renderer does not depend on the type
 * parser of the client. {@link #args} holds the parameters inside the parentheses. A parameter is either a nested
 * type ({@code type != null}) or a literal ({@code literal != null}), such as the {@code 3} and {@code 'UTC'} above
 * or the {@code 'a' = 1} of an {@code Enum8}. {@code name} is set for the named elements of a {@code Tuple} or
 * {@code Nested}, as in {@code Tuple(a Int32, b String)}.
 */
final class ChType {

    static final class Arg {
        final String name;
        final ChType type;
        final String literal;

        Arg(String name, ChType type, String literal) {
            this.name = name;
            this.type = type;
            this.literal = literal;
        }
    }

    final String base;
    final List<Arg> args;

    ChType(String base, List<Arg> args) {
        this.base = base;
        this.args = args;
    }

    static ChType of(String base) {
        return new ChType(base, Collections.emptyList());
    }

    /** Returns the nested type of argument i, or null. */
    ChType typeArg(int i) {
        return i < args.size() ? args.get(i).type : null;
    }

    /** Returns the literal of argument i without its single quotes, or null when argument i is absent. */
    String literalArg(int i) {
        if (i >= args.size() || args.get(i).literal == null) {
            return null;
        }
        String s = args.get(i).literal;
        if (s.length() >= 2 && s.startsWith("'") && s.endsWith("'")) {
            s = s.substring(1, s.length() - 1);
        }
        return s;
    }

    @Override
    public String toString() {
        if (args.isEmpty()) {
            return base;
        }
        StringBuilder sb = new StringBuilder(base).append('(');
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Arg a = args.get(i);
            if (a.type != null && a.name != null) {
                sb.append(a.name).append(' ').append(a.type);
            } else if (a.type != null) {
                sb.append(a.type);
            } else {
                sb.append(a.literal);
            }
        }
        return sb.append(')').toString();
    }

    /** Parses the type name that the server sends in the result header. */
    static ChType parse(String s) {
        String in = s.trim();
        String[] rest = new String[1];
        ChType t = parseAt(in, rest);
        if (!rest[0].trim().isEmpty()) {
            throw new IllegalArgumentException("parse type \"" + s + "\": unexpected trailing text \"" + rest[0] + "\"");
        }
        return t;
    }

    private static ChType parseAt(String s, String[] rest) {
        int i = 0;
        while (i < s.length() && (SqlText.isWordChar(s.charAt(i)) || s.charAt(i) == '.')) {
            i++;
        }
        if (i == 0) {
            throw new IllegalArgumentException("expected a type name at \"" + s + "\"");
        }
        String base = s.substring(0, i);
        String after = stripLeadingSpaces(s.substring(i));
        if (!after.startsWith("(")) {
            rest[0] = after;
            return of(base);
        }
        List<String> parts = new ArrayList<>();
        rest[0] = splitArgs(after, parts, s);
        List<Arg> args = new ArrayList<>(parts.size());
        for (String p : parts) {
            args.add(parseArg(base, p));
        }
        return new ChType(base, args);
    }

    private static String stripLeadingSpaces(String s) {
        int i = 0;
        while (i < s.length() && s.charAt(i) == ' ') {
            i++;
        }
        return s.substring(i);
    }

    /** Parses a whole string as a type, or returns null. */
    private static ChType tryParse(String s) {
        try {
            String[] rest = new String[1];
            ChType t = parseAt(s, rest);
            return rest[0].trim().isEmpty() ? t : null;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** Turns one raw parameter into a nested type, a named nested type, or a literal. */
    private static Arg parseArg(String base, String raw) {
        raw = raw.trim();
        if (raw.isEmpty() || raw.charAt(0) == '\'' || raw.charAt(0) == '-'
                || (raw.charAt(0) >= '0' && raw.charAt(0) <= '9')) {
            return new Arg(null, null, raw);
        }
        switch (base) {
            case "Enum8":
            case "Enum16":
            case "Enum":
                return new Arg(null, null, raw);
            case "AggregateFunction":
            case "SimpleAggregateFunction": {
                // The first argument is a function name, such as `sum` or `quantiles(0.5, 0.9)`. It is not a type.
                ChType t = tryParse(raw);
                if (t != null && KNOWN_TYPES.contains(t.base)) {
                    return new Arg(null, t, null);
                }
                return new Arg(null, null, raw);
            }
            case "Tuple":
            case "Nested":
            case "JSON":
            case "Object":
            case "Variant": {
                // Named element: `name Type` (the name can be quoted with backticks).
                String[] named = splitNamedElement(raw);
                if (named != null) {
                    ChType t = tryParse(named[1]);
                    if (t != null) {
                        return new Arg(named[0], t, null);
                    }
                }
                break;
            }
            default:
                break;
        }
        ChType t = tryParse(raw);
        if (t != null) {
            return new Arg(null, t, null);
        }
        return new Arg(null, null, raw);
    }

    /**
     * Splits {@code name Type} into its two parts. Returns null for an element that is only a type, such as
     * {@code Int32} or {@code Array(String)}.
     */
    private static String[] splitNamedElement(String raw) {
        if (raw.startsWith("`")) {
            StringBuilder name = new StringBuilder();
            int i = 1;
            while (i < raw.length()) {
                char c = raw.charAt(i);
                if (c == '\\' && i + 1 < raw.length()) {
                    name.append(raw.charAt(i + 1));
                    i += 2;
                } else if (c == '`' && i + 1 < raw.length() && raw.charAt(i + 1) == '`') {
                    name.append('`');
                    i += 2;
                } else if (c == '`') {
                    return new String[] {name.toString(), raw.substring(i + 1).trim()};
                } else {
                    name.append(c);
                    i++;
                }
            }
            return null;
        }
        int i = 0;
        while (i < raw.length() && (SqlText.isWordChar(raw.charAt(i)) || raw.charAt(i) == '.')) {
            i++;
        }
        if (i == 0 || i >= raw.length() || raw.charAt(i) != ' ') {
            return null;
        }
        return new String[] {raw.substring(0, i), raw.substring(i).trim()};
    }

    /**
     * Splits {@code (a, b(c, d), 'e,f')rest} into its top-level parameters and returns the text after the closing
     * parenthesis.
     */
    private static String splitArgs(String s, List<String> parts, String whole) {
        int depth = 0;
        int start = 1;
        char quote = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (quote != 0) {
                if (c == '\\') {
                    i++;
                } else if (c == quote) {
                    quote = 0;
                }
                continue;
            }
            switch (c) {
                case '\'':
                case '`':
                case '"':
                    quote = c;
                    break;
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    if (depth == 0) {
                        String p = s.substring(start, i).trim();
                        if (!p.isEmpty() || !parts.isEmpty()) {
                            parts.add(p);
                        }
                        return s.substring(i + 1);
                    }
                    break;
                case ',':
                    if (depth == 1) {
                        parts.add(s.substring(start, i).trim());
                        start = i + 1;
                    }
                    break;
                default:
                    break;
            }
        }
        throw new IllegalArgumentException("unbalanced parentheses in \"" + whole + "\"");
    }

    /**
     * Used only to tell {@code SimpleAggregateFunction(sum, UInt64)} (function name, then type) apart; a false
     * negative only makes the renderer report an unsupported type.
     */
    private static final Set<String> KNOWN_TYPES = new HashSet<>(Arrays.asList(
            "Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
            "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256",
            "Float32", "Float64", "BFloat16", "Bool", "String", "FixedString",
            "Date", "Date32", "DateTime", "DateTime64", "Time", "Time64", "UUID", "IPv4", "IPv6",
            "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256",
            "Enum8", "Enum16", "Nullable", "LowCardinality", "Array", "Tuple", "Map", "Nothing",
            "Point", "Ring", "LineString", "MultiLineString", "Polygon", "MultiPolygon", "MultiPoint", "Geometry",
            "JSON", "Object", "Dynamic", "Variant", "Nested", "QBit"));
}
