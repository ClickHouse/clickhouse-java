package com.clickhouse.client.cli;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The {@code jdbc} backend: jdbc-v2 over HTTP, through {@link DriverManager}.
 *
 * <p>Every statement runs with {@link Statement#execute(String)} on one {@link Statement}. The values come from
 * {@link ResultSet#getObject(int)}, and the column types from {@link ResultSetMetaData#getColumnTypeName(int)}. The
 * session is the {@code session_id} server setting of the connection. The driver handles {@code USE} itself.
 *
 * <p>The command-line settings are connection properties ({@code clickhouse_setting_<name>}), so they go with each
 * request. When the test changes one of them with SET, the backend opens the connection again (with the same
 * session) without it, so that the session value counts. The query parameters of the command line
 * ({@code --param_<name>}) go as {@code param_<name>} server settings, which the HTTP interface reads as query
 * parameters.
 */
final class JdbcBackend implements Backend {

    private static final String SETTING_PREFIX = "clickhouse_setting_";

    private final String baseUrl;
    private final CliOptions opts;
    private final String sessionId;
    private final InvocationLog log;
    private Connection conn;
    private Statement stmt;
    private String database;

    JdbcBackend(CliOptions opts, int port, String sessionId, Map<String, String> settings, InvocationLog log)
            throws SQLException {
        this.baseUrl = "jdbc:clickhouse:" + (opts.secure ? "https" : "http") + "://" + opts.host + ":" + port + "/";
        this.opts = opts;
        this.sessionId = sessionId;
        this.log = log;
        this.database = opts.database;
        open(settings);
    }

    private void open(Map<String, String> settings) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", opts.user);
        props.setProperty("password", opts.password);
        props.setProperty(SETTING_PREFIX + "session_id", sessionId);
        for (Map.Entry<String, String> e : settings.entrySet()) {
            props.setProperty(SETTING_PREFIX + e.getKey(), e.getValue());
        }
        for (Map.Entry<String, String> e : opts.params.entrySet()) {
            props.setProperty(SETTING_PREFIX + "param_" + e.getKey(), e.getValue());
        }
        String url = baseUrl + database;
        log.add("jdbc_url", url);
        conn = DriverManager.getConnection(url, props);
        stmt = conn.createStatement();
    }

    @Override
    public void settingsChanged(Map<String, String> settings) throws Exception {
        close();
        open(settings);
    }

    @Override
    public Result execute(String sql, Map<String, String> settings, String currentDatabase) throws Exception {
        // The shim tracks the current database only to open the connection again in it.
        this.database = currentDatabase;
        if (!stmt.execute(sql)) {
            return null;
        }
        ResultSet rs = stmt.getResultSet();
        if (rs == null) {
            return null;
        }
        ResultSetMetaData meta = rs.getMetaData();
        List<String> names = new ArrayList<>();
        List<String> types = new ArrayList<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            names.add(meta.getColumnLabel(i));
            types.add(meta.getColumnTypeName(i));
        }
        return new Result() {
            @Override
            public List<String> names() {
                return names;
            }

            @Override
            public List<String> types() {
                return types;
            }

            @Override
            public boolean next() throws SQLException {
                return rs.next();
            }

            @Override
            public Object get(int i) throws SQLException {
                return rs.getObject(i + 1);
            }

            @Override
            public void close() throws SQLException {
                rs.close();
            }
        };
    }

    @Override
    public void close() throws SQLException {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
}
