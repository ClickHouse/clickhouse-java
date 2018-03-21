package ru.yandex.clickhouse;

import org.apache.http.impl.client.CloseableHttpClient;

import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import java.sql.*;
import java.util.*;


public class ClickHousePreparedStatementImpl extends ClickHousePreparedAbstractStatementImpl implements ClickHousePreparedStatement {
    private final List<String> sqlParts;
    private String[] binds;
    private boolean[] valuesQuote;
    private List<byte[]> batchRows = new ArrayList<byte[]>();

    public ClickHousePreparedStatementImpl(CloseableHttpClient client,
                                           ClickHouseConnection connection,
                                           ClickHouseProperties properties,
                                           String sql,
                                           TimeZone timezone) throws SQLException {
        super(client, connection, properties, timezone);

        this.sqlParts = parseSql(sql);
        createBinds();
    }

    private void createBinds() {
        this.binds = new String[this.sqlParts.size() - 1];
        this.valuesQuote = new boolean[this.sqlParts.size() - 1];
    }


    @Override
    protected void setParameter(int index, String value, boolean isQuote) {
        binds[index - 1] = value;
        valuesQuote[index - 1] = isQuote;
    }

    @Override
    public void clearParameters() {
        Arrays.fill(binds, null);
        Arrays.fill(valuesQuote, false);
    }

    static List<String> parseSql(String sql) throws SQLException {
        if (sql == null) {
            throw new SQLException("sql statement can't be null");
        }

        List<String> parts = new ArrayList<String>();

        boolean afterBackSlash = false, inQuotes = false, inBackQuotes = false;
        boolean inSingleLineComment = false;
        boolean inMultiLineComment = false;
        int partStart = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inSingleLineComment) {
                if (c == '\n') {
                    inSingleLineComment = false;
                }
            } else if (inMultiLineComment) {
                if (c == '*' && sql.length() > i + 1 && sql.charAt(i + 1) == '/') {
                    inMultiLineComment = false;
                    i++;
                }
            } else if (afterBackSlash) {
                afterBackSlash = false;
            } else if (c == '\\') {
                afterBackSlash = true;
            } else if (c == '\'') {
                inQuotes = !inQuotes;
            } else if (c == '`') {
                inBackQuotes = !inBackQuotes;
            } else if (!inQuotes && !inBackQuotes) {
                if (c == '?') {
                    parts.add(sql.substring(partStart, i));
                    partStart = i + 1;
                } else if (c == '-' && sql.length() > i + 1 && sql.charAt(i + 1) == '-') {
                    inSingleLineComment = true;
                    i++;
                } else if (c == '/' && sql.length() > i + 1 && sql.charAt(i + 1) == '*') {
                    inMultiLineComment = true;
                    i++;
                }
            }
        }
        parts.add(sql.substring(partStart, sql.length()));

        return parts;
    }

    @Override
    protected String getSqlQuery() throws SQLException {
        if (sqlParts.size() == 1) {
            return sqlParts.get(0);
        }
        checkParameters(binds);

        StringBuilder sb = new StringBuilder(sqlParts.get(0));
        for (int i = 1; i < sqlParts.size(); i++) {
            appendBoundValue(sb, i - 1);
            sb.append(sqlParts.get(i));
        }

        return sb.toString();
    }

    private void appendBoundValue(StringBuilder sb, int num) {
        if (valuesQuote[num]) {
            sb.append("'").append(binds[num]).append("'");
        } else if (binds[num].equals("\\N")) {
            sb.append("null");
        } else {
            sb.append(binds[num]);
        }
    }

    private byte[] buildBinds() throws SQLException {
        checkParameters(binds);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < binds.length; i++) {
            sb.append(binds[i]);
            sb.append(i < binds.length - 1 ? '\t' : '\n');
        }
        return sb.toString().getBytes(StreamUtils.UTF_8);
    }


    @Override
    public void addBatch() throws SQLException {
        batchRows.add(buildBinds());
        createBinds();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
