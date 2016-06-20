package ru.yandex.clickhouse;

import ru.yandex.clickhouse.copypaste.CHProperties;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhur on 19/02/16.
 */
public class CHDataSource implements DataSource {

    protected final static Pattern urlRegexp;

    static {
        urlRegexp = Pattern.compile("^jdbc:clickhouse://([a-zA-Z0-9.-]+):([0-9]+)(?:|/|/([a-zA-Z0-9_]+))$");
    }

    protected final static String DEFAULT_DATABASE = "default";

    protected final static CHDriver driver = new CHDriver();

    protected final String url;
    protected String host;
    protected int port;
    protected String database;

    PrintWriter printWriter;
    protected int loginTimeout = 0;

    private CHProperties properties;

    public CHDataSource(String url) {
        this(url, new CHProperties());
    }

    public CHDataSource(String url, Properties info) {
        this(url, new CHProperties(info));
    }

    public CHDataSource(String url, CHProperties properties) {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect clickhouse jdbc url: " + url);
        }
        this.url = url;

        Matcher m = urlRegexp.matcher(url);
        if (m.find()) {
            this.host = m.group(1);
            this.port = Integer.parseInt(m.group(2));
            if (m.group(3) != null) {
                this.database = m.group(3);
            } else {
                this.database = DEFAULT_DATABASE;
            }
        } else {
            throw new IllegalArgumentException("Incorrect clickhouse jdbc url: " + url);
        }
        this.properties = new CHProperties(properties);
        this.properties.setDatabase(database);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return driver.connect(url, properties);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return driver.connect(url, properties);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        printWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Not implemented");
    }
}
