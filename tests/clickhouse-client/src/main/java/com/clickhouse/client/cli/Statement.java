package com.clickhouse.client.cli;

/**
 * One query of a multi-query script, split the way {@code clickhouse-client} splits it (see
 * {@code ClientBase::executeMultiQuery}).
 */
final class Statement {
    /**
     * The text to send to the server: from the first non-whitespace character (leading comments included) to the
     * end of the query, with the semicolon.
     */
    final String sql;
    /**
     * {@link #sql} plus the semicolon and the same-line trailing comment. The client reads the test hints from this
     * text and prints it for {@code echo}.
     */
    final String full;
    final TestHint hint;
    /** Tells if this statement is printed before it runs ({@code echo}, {@code echoOn}). */
    final boolean echo;

    Statement(String sql, String full, TestHint hint, boolean echo) {
        this.sql = sql;
        this.full = full;
        this.hint = hint;
        this.echo = echo;
    }

    @Override
    public String toString() {
        return sql;
    }
}
