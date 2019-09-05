package ru.yandex.clickhouse;

import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.InputStream;
import java.sql.SQLException;

import static ru.yandex.clickhouse.domain.ClickHouseFormat.*;

class Writer extends ConfigurableApi<Writer> {

    private ClickHouseFormat format = TabSeparated;

    private String table = null;
    private String sql = null;
    private InputStream stream = null;

    Writer(ClickHouseStatementImpl statement) {
        super(statement);
    }

    /**
     * Specifies format for further insert of data via send()
     */
    public Writer format(ClickHouseFormat format) {
        if (null == format) {
            throw new NullPointerException("Format can not be null");
        }
        this.format = format;
        return this;
    }

    /**
     * Set table name for data insertion
     *
     * @param table table name
     * @return this
     */
    public Writer table(String table) {
        this.sql = null;
        this.table = table;
        return this;
    }

    /**
     * Set SQL for data insertion
     *
     * @param sql in a form "INSERT INTO table_name [(X,Y,Z)] VALUES "
     * @return this
     */
    public Writer sql(String sql) {
        this.sql = sql;
        this.table = null;
        return this;
    }

    /**
     * Specifies data input stream
     */
    public Writer data(InputStream stream) {
        this.stream = stream;
        return this;
    }

    /**
     * Method to call, when Writer is fully configured
     */
    public void send() throws SQLException {
        try {
            String sql = buildSQL();
            if (null == stream) {
                throw new IllegalArgumentException("No input data specified");
            }
        } catch (IllegalArgumentException err) {
            throw new SQLException(err);
        }
    }

    /**
     * Allows to send stream of data to ClickHouse
     *
     * @param sql    in a form of "INSERT INTO table_name (X,Y,Z) VALUES "
     * @param data   where to read data from
     * @param format format of data in InputStream
     * @throws SQLException
     */
    public void send(String sql, InputStream data, ClickHouseFormat format) throws SQLException {
        format(format).data(data).sql(sql).send();
    }

    /**
     * Convenient method for importing the data into table
     *
     * @param table  table name
     * @param data   source data
     * @param format format of data in InputStream
     * @throws SQLException
     */
    public void sendToTable(String table, InputStream data, ClickHouseFormat format) throws SQLException {
        format(format).table(table).data(data).send();
    }

    /**
     * Allows to send binary data via invocation of stream callback
     */
    public void send(String sql, ClickHouseStreamCallback callback, ClickHouseFormat format) throws SQLException {
        if (!(RowBinary.equals(format) || Native.equals(format))) {
            throw new IllegalArgumentException("Sending data via stream callback is only available for RowBinary and Native formats");
        }

        format(format).sql(sql).send();
    }

    private String buildSQL() {
        if (null != table) {
            return "INSERT INTO " + table;
        } else if (null != sql) {
            return sql;
        } else {
            throw new IllegalArgumentException("Neither table nor SQL clause are specified");
        }
    }
}
