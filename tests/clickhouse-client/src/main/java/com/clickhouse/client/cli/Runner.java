package com.clickhouse.client.cli;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs the statements of one shim invocation on one session, as {@code clickhouse-client} does.
 *
 * <p>Session state ({@code SET}, {@code USE}, temporary tables) must survive from one statement to the next. Both
 * backends use HTTP with one {@code session_id}; the server keeps the session after an error.
 *
 * <p>The shim sends nothing that {@code clickhouse-client} does not send, because some tests read their own queries
 * from {@code system.query_log}.
 */
final class Runner {

    private final Backend backend;
    private final OutputStream out;
    private final InvocationLog log;
    private final Map<String, String> params;

    /** The current database of the session. */
    private String database;
    /**
     * The command-line settings that go with each query, as clickhouse-client sends them. A SET of the same
     * setting in the test removes it from here, because the session then holds the value.
     */
    private final Map<String, String> settings;
    /** The value of the last {@code SET dialect}. */
    private String dialect;
    /** The session value of {@code ignore_format_null_for_explain} (default on). */
    private boolean ignoreFormatNullForExplain = true;
    /** The output of the current statement, until the runner knows whether it echoes the statement first. */
    private final TsvOutput result = new TsvOutput();

    Runner(Backend backend, CliOptions opts, Map<String, String> settings, OutputStream out, InvocationLog log) {
        this.backend = backend;
        this.out = out;
        this.log = log;
        this.params = opts.params;
        this.database = opts.database;
        this.settings = settings;
    }

    /** Runs the statements in order. Stops at the first statement whose result does not match its hint. */
    void run(List<Statement> statements) throws Exception {
        for (Statement st : statements) {
            runOne(st);
        }
    }

    private void runOne(Statement st) throws Exception {
        if (st.hint.parseError != null) {
            echo(st);
            out.flush();
            throw new Errors.HintException(st.hint.parseError, 0);
        }
        log.add("query", st.sql);
        result.reset();
        Exception err = null;
        try {
            execute(st.sql);
        } catch (Exception e) {
            err = e;
        }
        Integer code = Errors.serverCode(err);
        // clickhouse-client parses a statement before it echoes it, so it does not echo a statement with a syntax
        // error.
        if (code == null || code != Errors.SYNTAX_ERROR) {
            echo(st);
        }
        result.writeTo(out);
        out.flush();

        if (st.hint.expectsError() && err == null) {
            throw new Errors.HintException("expected " + st.hint.label() + ", but the query succeeded (query: "
                    + st.full + ")", 0);
        }
        if (st.hint.expectsError() && code != null) {
            if (!st.hint.matches(code)) {
                throw new Errors.HintException("expected " + st.hint.label() + ", but got "
                        + Errors.formatServerError(err, code) + " (query: " + st.full + ")", code);
            }
            log.add("expected_error_matched", st.hint.label());
            return;
        }
        if (err != null) {
            throw err;
        }
        afterSuccess(st.sql);
    }

    /** Writes the statement for the {@code echo} test hints. */
    private void echo(Statement st) throws IOException {
        if (st.echo) {
            out.write((st.full + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    private void execute(String sql) throws Exception {
        boolean other = otherDialect(sql);
        String query = sql;
        if (other) {
            // Over HTTP the server parses the request body as one query, and the parsers of other dialects do not
            // accept a trailing semicolon.
            query = StatementSplitter.trimRight(sql);
            if (query.endsWith(";")) {
                query = query.substring(0, query.length() - 1);
            }
        }
        try (Backend.Result rs = backend.execute(query, settings, database)) {
            if (rs != null) {
                writeRows(rs, sql, other);
            }
        }
    }

    private void writeRows(Backend.Result rs, String sql, boolean other) throws Exception {
        byte[] raw = rs.rawText();
        if (raw != null) {
            result.write(raw);
            return;
        }
        // The FORMAT clause is SQL; other dialects can use the word differently (the `format` pipe of LogsQL).
        OutputFormat format = other ? OutputFormat.DEFAULT : OutputFormat.of(sql);
        if (!format.supported) {
            throw new ShimException("output format " + format.name + " is not supported: the Java client reads"
                    + " results in a binary format, and the shim writes them only as TabSeparated");
        }
        boolean discard = format.discard;
        if (discard && ignoreFormatNullForExplain && "EXPLAIN".equals(SqlText.leadingKeyword(sql))) {
            // With ignore_format_null_for_explain (on by default), the server ignores `FORMAT Null` for EXPLAIN
            // and clickhouse-client prints the plan.
            discard = false;
        }
        List<String> names = rs.names();
        List<String> types = rs.types();
        List<TsvRenderer> renderers = new ArrayList<>(types.size());
        for (String t : types) {
            renderers.add(new TsvRenderer(t));
        }
        if (!discard && format.names) {
            writeHeader(names, format.raw);
        }
        if (!discard && format.types) {
            writeHeader(types, format.raw);
        }
        while (rs.next()) {
            for (int i = 0; i < renderers.size(); i++) {
                Object v = rs.get(i);
                if (discard) {
                    continue;
                }
                if (i > 0) {
                    result.write('\t');
                }
                try {
                    renderers.get(i).render(result, v, format.raw);
                } catch (RuntimeException e) {
                    throw new ShimException("column " + names.get(i) + ": " + e.getMessage(), e);
                }
            }
            if (!discard) {
                result.write('\n');
            }
        }
    }

    /**
     * Writes a header line (names or types). A Raw format writes it without escapes, as its rows: the server does
     * the same (TabSeparatedRowOutputFormat::writeLine).
     */
    private void writeHeader(List<String> values, boolean raw) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                result.write('\t');
            }
            if (raw) {
                result.write(values.get(i));
            } else {
                TsvRenderer.writeEscaped(result, values.get(i), false);
            }
        }
        result.write('\n');
    }

    /**
     * Tells if the session uses another query language than SQL ({@code SET dialect = 'kusto'}, 'prql', ...).
     */
    private boolean otherDialect(String sql) {
        return dialect != null && !dialect.equalsIgnoreCase("clickhouse")
                && !"SET".equals(SqlText.leadingKeyword(sql));
    }

    /** Updates the session state after a USE or SET statement. */
    private void afterSuccess(String sql) throws Exception {
        String keyword = SqlText.leadingKeyword(sql);
        if (keyword.equals("USE")) {
            String db = parseUseDatabase(sql, params);
            if (db == null) {
                throw new ShimException("cannot read the database name of \"" + sql + "\"");
            }
            database = db;
        } else if (keyword.equals("SET")) {
            boolean changed = false;
            for (Map.Entry<String, String> e : parseSetAssignments(sql).entrySet()) {
                if (settings.remove(e.getKey()) != null) {
                    changed = true;
                }
                if (e.getKey().equals("dialect")) {
                    dialect = e.getValue();
                } else if (e.getKey().equals("ignore_format_null_for_explain")) {
                    ignoreFormatNullForExplain = !isFalse(e.getValue());
                }
            }
            if (changed) {
                backend.settingsChanged(settings);
            }
        }
    }

    /** Tells if the value of a Bool setting in a SET statement is false ({@code 0} or {@code false}). */
    static boolean isFalse(String value) {
        String v = value.trim();
        return v.equals("0") || v.equalsIgnoreCase("false");
    }

    /**
     * Returns the database name of a USE statement, or null. A {@code {name:Identifier}} query parameter is
     * replaced by its value.
     */
    static String parseUseDatabase(String sql, Map<String, String> params) {
        int i = SqlText.skipWhitespaceAndComments(sql, 0) + "USE".length();
        String rest = sql.substring(Math.min(i, sql.length())).trim();
        if (rest.endsWith(";")) {
            rest = rest.substring(0, rest.length() - 1).trim();
        }
        int sp = rest.indexOf(' ');
        if (sp > 0 && rest.substring(0, sp).equalsIgnoreCase("DATABASE")) {
            rest = rest.substring(sp + 1).trim();
        }
        if (rest.isEmpty()) {
            return null;
        }
        char c = rest.charAt(0);
        if (c == '`' || c == '"') {
            return SqlText.unquoteIdentifier(rest);
        }
        if (c == '{') {
            String inner = rest.replaceAll("^\\{|}$", "");
            int colon = inner.indexOf(':');
            if (colon < 0) {
                return null;
            }
            return params.get(inner.substring(0, colon).trim());
        }
        for (int j = 0; j < rest.length(); j++) {
            if (!SqlText.isWordChar(rest.charAt(j))) {
                return null;
            }
        }
        return rest;
    }

    /**
     * Returns the settings that a {@code SET a = 1, b = 'x'} statement assigns, with their values (string values
     * without quotes).
     */
    static Map<String, String> parseSetAssignments(String sql) {
        int i = SqlText.skipWhitespaceAndComments(sql, 0) + "SET".length();
        String body = sql.substring(Math.min(i, sql.length())).trim();
        if (body.endsWith(";")) {
            body = body.substring(0, body.length() - 1);
        }
        Map<String, String> out = new LinkedHashMap<>();
        int depth = 0;
        int start = 0;
        for (int j = 0; j < body.length(); j++) {
            char c = body.charAt(j);
            if (c == '\'' || c == '"' || c == '`') {
                j = SqlText.skipQuoted(body, j) - 1;
            } else if (c == '(' || c == '[') {
                depth++;
            } else if (c == ')' || c == ']') {
                depth--;
            } else if (c == ',' && depth == 0) {
                addAssignment(out, body.substring(start, j));
                start = j + 1;
            }
        }
        addAssignment(out, body.substring(start));
        return out;
    }

    private static void addAssignment(Map<String, String> out, String part) {
        int eq = part.indexOf('=');
        if (eq < 0) {
            return;
        }
        String value = part.substring(eq + 1).trim();
        if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length() - 1);
        }
        out.put(part.substring(0, eq).trim(), value);
    }

    /** An error of the shim itself, not of the client or the server. */
    static final class ShimException extends Exception {
        ShimException(String message) {
            super(message);
        }

        ShimException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
