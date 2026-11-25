package com.clickhouse.client.cli;

import java.util.List;
import java.util.Map;

/**
 * Runs statements through one client API on one session. Selected by {@code CLICKHOUSE_CLIENT_CLI_IMPL}.
 */
interface Backend extends AutoCloseable {

    /**
     * Runs one statement.
     *
     * @param sql the statement text, as {@code clickhouse-client} sends it
     * @param settings the command-line settings that go with the statement
     * @param database the current database of the session
     * @return the result set, or null when the statement has no result set
     */
    Result execute(String sql, Map<String, String> settings, String database) throws Exception;

    /** Called when the test changes a forwarded setting with SET: the setting no longer goes with each query. */
    default void settingsChanged(Map<String, String> settings) throws Exception {
    }

    @Override
    void close() throws Exception;

    /** A result set: column names and types from the metadata, and the values as the client returns them. */
    interface Result extends AutoCloseable {
        List<String> names();

        List<String> types();

        boolean next() throws Exception;

        /** Returns the value of column i (0-based) of the current row, with the client's normal type mapping. */
        Object get(int i) throws Exception;

        /**
         * For a pass-through backend: the raw response text, written as it is. Null for the typed backends.
         */
        default byte[] rawText() throws Exception {
            return null;
        }

        @Override
        void close() throws Exception;
    }
}
