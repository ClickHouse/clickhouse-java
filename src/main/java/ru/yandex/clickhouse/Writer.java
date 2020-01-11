package ru.yandex.clickhouse;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.apache.http.HttpEntity;
import org.apache.http.entity.InputStreamEntity;

import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;
import ru.yandex.clickhouse.util.ClickHouseStreamHttpEntity;

import static ru.yandex.clickhouse.domain.ClickHouseFormat.Native;
import static ru.yandex.clickhouse.domain.ClickHouseFormat.RowBinary;
import static ru.yandex.clickhouse.domain.ClickHouseFormat.TabSeparated;

public final class Writer extends ConfigurableApi<Writer> {

    private ClickHouseFormat format = TabSeparated;

    private String table = null;
    private String sql = null;
    private InputStreamProvider streamProvider = null;

    Writer(ClickHouseStatementImpl statement) {
        super(statement);
    }

    /**
     * Specifies format for further insert of data via send()
     *
     * @param format
     *            the format of the data to upload
     * @return this writer instance
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
     * @param table
     *            name of the table to upload the data to
     * @return this writer instance
     */
    public Writer table(String table) {
        this.sql = null;
        this.table = table;
        return this;
    }

    /**
     * Set SQL for data insertion
     *
     * @param sql
     *            in a form "INSERT INTO table_name [(X,Y,Z)] VALUES "
     * @return this writer instance
     */
    public Writer sql(String sql) {
        this.sql = sql;
        this.table = null;
        return this;
    }

    /**
     * Specifies data input stream
     *
     * @param stream
     *            a stream providing the data to upload
     * @return this writer instance
     */
    public Writer data(InputStream stream) {
        streamProvider = new HoldingInputProvider(stream);
        return this;
    }

    /**
     * Specifies data input stream, and the format to use
     *
     * @param stream
     *            a stream providing the data to upload
     * @param format
     *            the format of the data to upload
     * @return this writer instance
     */
    public Writer data(InputStream stream, ClickHouseFormat format) {
        return format(format).data(stream);
    }

    /**
     * Shortcut method for specifying a file as an input
     *
     * @param input
     *            the file to upload
     * @return this writer instance
     */
    public Writer data(File input) {
        streamProvider = new FileInputProvider(input);
        return this;
    }

    /**
     * Shortcut method for specifying a file as an input and its format
     *
     * @param input
     *            the file to upload
     * @param format
     *            the format of the {@code file}
     * @return this writer instance
     */
    public Writer data(File input, ClickHouseFormat format) {
        return format(format).data(input);
    }

    /**
     * Method to call when this Writer is fully configured.
     *
     * @throws SQLException
     *             if the upload fails
     */
    public void send() throws SQLException {
        HttpEntity entity;
        try {
            InputStream stream;
            if (null == streamProvider || null == (stream = streamProvider.get())) {
                throw new IOException("No input data specified");
            }
            entity = new InputStreamEntity(stream);
        } catch (IOException err) {
            throw new SQLException(err);
        }
        send(entity);
    }

    private void send(HttpEntity entity) throws SQLException {
        statement.sendStream(this, entity);
    }

    /**
     * Allows to send stream of data to ClickHouse
     *
     * @param sql
     *            in a form of "INSERT INTO table_name (X,Y,Z) VALUES "
     * @param data
     *            where to read data from
     * @param format
     *            format of data in InputStream
     * @throws SQLException
     *             if the upload fails
     */
    public void send(String sql, InputStream data, ClickHouseFormat format) throws SQLException {
        sql(sql).data(data).format(format).send();
    }

    /**
     * Convenient method for importing the data into table
     *
     * @param table
     *            table name
     * @param data
     *            source data
     * @param format
     *            format of data in InputStream
     * @throws SQLException
     *             if the upload fails
     */
    public void sendToTable(String table, InputStream data, ClickHouseFormat format) throws SQLException {
        table(table).data(data).format(format).send();
    }

    /**
     * Sends the data in {@link ClickHouseFormat#RowBinary RowBinary} or in
     * {@link ClickHouseFormat#Native Native} format
     *
     * @param sql
     *            the SQL statement to execute
     * @param callback
     *            data source for the upload
     * @param format
     *            the format to use, either {@link ClickHouseFormat#RowBinary
     *            RowBinary} or {@link ClickHouseFormat#Native Native}
     * @throws SQLException
     *             if the upload fails
     */
    public void send(String sql, ClickHouseStreamCallback callback, ClickHouseFormat format) throws SQLException {
        if (!(RowBinary.equals(format) || Native.equals(format))) {
            throw new SQLException("Wrong binary format - only RowBinary and Native are supported");
        }

        format(format).sql(sql).send(new ClickHouseStreamHttpEntity(callback, statement.getConnection().getTimeZone(), statement.properties));
    }

    String getSql() {
        if (null != table) {
            return "INSERT INTO " + table + " FORMAT " + format;
        } else if (null != sql) {
            String result = sql;
            if (!ClickHouseFormat.containsFormat(result)) {
                result += " FORMAT " + format;
            }
            return result;
        } else {
            throw new IllegalArgumentException("Neither table nor SQL clause are specified");
        }
    }

    private interface InputStreamProvider {
        InputStream get() throws IOException;
    }

    private static final class FileInputProvider implements InputStreamProvider {
        private final File file;

        private FileInputProvider(File file) {
            this.file = file;
        }

        @Override
        public InputStream get() throws IOException {
            return new FileInputStream(file);
        }
    }

    private static final class HoldingInputProvider implements InputStreamProvider {
        private final InputStream stream;

        private HoldingInputProvider(InputStream stream) {
            this.stream = stream;
        }

        @Override
        public InputStream get() throws IOException {
            return stream;
        }
    }
}
