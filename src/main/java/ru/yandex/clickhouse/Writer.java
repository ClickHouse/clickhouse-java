package ru.yandex.clickhouse;

import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.InputStream;
import java.sql.SQLException;

class Writer extends ConfigurableApi<Writer> {

    private ClickHouseFormat format = ClickHouseFormat.TabSeparated;

    Writer(ClickHouseStatementImpl statement) {
        super(statement);
    }

    public Writer format(ClickHouseFormat format) {
        if (null == format) {
            throw new NullPointerException("Format can not be null");
        }
        this.format = format;
        return this;
    }

    private void send() throws SQLException
    {

    }

    public void sendStream(String sql, InputStream stream, ClickHouseFormat format) throws SQLException {
        Writer writer = null == format ? this : format(format);
        writer.send();
    }

    public void sendStreamToTable(String table, InputStream stream, ClickHouseFormat format) throws SQLException {
        Writer writer = null == format ? this : format(format);
        writer.send();
    }

    public void sendRowBinaryStream(String sql, ClickHouseStreamCallback callback) throws SQLException {
        format(ClickHouseFormat.RowBinary).send();
    }

    void sendNativeStream(String sql, ClickHouseStreamCallback callback) throws SQLException {
        format(ClickHouseFormat.Native).send();
    }
}
