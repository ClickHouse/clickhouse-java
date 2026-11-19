package com.clickhouse.client.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The test hints of one statement, as {@code clickhouse-client} parses them ({@code src/Client/TestHint.cpp}):
 *
 * <pre>
 * SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
 * SELECT 1 / 0;      -- { serverError 153, 6 }
 * -- { echoOn }
 * </pre>
 *
 * An error hint counts only when it is not in a leading comment. An {@code echo} hint counts in any comment.
 */
final class TestHint {
    final List<Integer> serverErrors = new ArrayList<>();
    final List<Integer> clientErrors = new ArrayList<>();
    /** Set by {@code echo}, {@code echoOn} (true) or {@code echoOff} (false); null when there is no echo hint. */
    Boolean echo;
    /** Set when a hint is malformed or names an unknown error. */
    String parseError;

    boolean expectsError() {
        return !serverErrors.isEmpty() || !clientErrors.isEmpty();
    }

    /**
     * Tells if the error code satisfies the hint. Through the Java client the server finds every error, including
     * syntax errors that {@code clickhouse-client} finds locally, so a server error can satisfy a
     * {@code clientError} hint too.
     */
    boolean matches(int code) {
        return serverErrors.contains(code) || clientErrors.contains(code);
    }

    String label() {
        if (!serverErrors.isEmpty() && !clientErrors.isEmpty()) {
            return "error " + codes(serverErrors);
        }
        if (!serverErrors.isEmpty()) {
            return "serverError " + codes(serverErrors);
        }
        return "clientError " + codes(clientErrors);
    }

    private static String codes(List<Integer> list) {
        return list.stream().map(c -> {
            String name = ErrorCodes.name(c);
            return name != null ? name : String.valueOf(c);
        }).collect(Collectors.joining(", "));
    }

    static TestHint parse(String full) {
        TestHint h = new TestHint();
        boolean leading = true;
        int i = 0;
        while (i < full.length()) {
            char c = full.charAt(i);
            int end;
            if (SqlText.isSpace(c)) {
                i++;
                continue;
            } else if (SqlText.lineCommentAt(full, i)) {
                end = SqlText.skipLineComment(full, i);
            } else if (full.startsWith("/*", i)) {
                end = SqlText.skipBlockComment(full, i);
            } else if (c == '\'' || c == '"' || c == '`') {
                leading = false;
                i = SqlText.skipQuoted(full, i);
                continue;
            } else if (c == '$') {
                leading = false;
                int e = SqlText.heredocEnd(full, i);
                i = e > 0 ? e : i + 1;
                continue;
            } else {
                leading = false;
                i++;
                continue;
            }
            String comment = full.substring(i, end);
            int open = comment.indexOf('{');
            if (open >= 0) {
                int close = comment.indexOf('}', open);
                if (close >= 0) {
                    h.parseBody(comment.substring(open + 1, close), leading);
                }
            }
            i = end;
        }
        return h;
    }

    private void parseBody(String body, boolean leading) {
        List<String> tokens = tokenize(body);
        for (int i = 0; i < tokens.size(); i++) {
            String tok = tokens.get(i);
            switch (tok) {
                case "echo":
                case "echoOn":
                    echo = Boolean.TRUE;
                    break;
                case "echoOff":
                    echo = Boolean.FALSE;
                    break;
                case "retry":
                    // `retry N until|while serverError X` is used by a few tests that wait for a background
                    // process. It is not supported.
                    parseError = "the \"retry\" test hint is not supported";
                    return;
                case "serverError":
                case "clientError":
                case "error": {
                    if (leading) {
                        continue;
                    }
                    List<Integer> codes = new ArrayList<>();
                    for (String t : tokens.subList(i + 1, tokens.size())) {
                        if (t.equals(",")) {
                            continue;
                        }
                        Integer code = parseCode(t);
                        if (code == null) {
                            parseError = "unknown error name \"" + t + "\" in test hint";
                            return;
                        }
                        codes.add(code);
                    }
                    if (tok.equals("serverError")) {
                        serverErrors.addAll(codes);
                    } else if (tok.equals("clientError")) {
                        clientErrors.addAll(codes);
                    } else {
                        serverErrors.addAll(codes);
                        clientErrors.addAll(codes);
                    }
                    return;
                }
                default:
                    break;
            }
        }
    }

    private static Integer parseCode(String t) {
        try {
            return Integer.valueOf(t);
        } catch (NumberFormatException e) {
            return ErrorCodes.code(t);
        }
    }

    private static List<String> tokenize(String body) {
        List<String> out = new ArrayList<>();
        int i = 0;
        while (i < body.length()) {
            char c = body.charAt(i);
            if (SqlText.isSpace(c)) {
                i++;
            } else if (c == ',') {
                out.add(",");
                i++;
            } else {
                int j = i;
                while (j < body.length() && !SqlText.isSpace(body.charAt(j)) && body.charAt(j) != ',') {
                    j++;
                }
                out.add(body.substring(i, j));
                i = j;
            }
        }
        return out;
    }
}
