package com.clickhouse.client.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The error codes of ClickHouse's {@code src/Common/ErrorCodes.cpp}. Test hints can name an error instead of giving
 * its code. {@code scripts/gen-error-codes.sh} generates the table ({@code error-codes.tsv}) from the upstream
 * checkout, so it always matches the tests.
 */
final class ErrorCodes {

    private static final Map<String, Integer> CODES;
    private static final Map<Integer, String> NAMES;

    static {
        Map<String, Integer> codes = new HashMap<>();
        Map<Integer, String> names = new HashMap<>();
        try (InputStream in = ErrorCodes.class.getResourceAsStream("error-codes.tsv")) {
            if (in == null) {
                throw new IllegalStateException("error-codes.tsv is not on the class path; run scripts/gen-error-codes.sh");
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                int tab = line.indexOf('\t');
                if (line.startsWith("#") || tab < 0) {
                    continue;
                }
                int code = Integer.parseInt(line.substring(0, tab));
                String name = line.substring(tab + 1);
                codes.put(name, code);
                names.put(code, name);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        CODES = Collections.unmodifiableMap(codes);
        NAMES = Collections.unmodifiableMap(names);
    }

    private ErrorCodes() {
    }

    /** Returns the code of an error name, or null. */
    static Integer code(String name) {
        return CODES.get(name);
    }

    /** Returns the name of an error code, or null. */
    static String name(int code) {
        return NAMES.get(code);
    }
}
