package ru.yandex.clickhouse;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ClickHouseDataSource implements DataSource {

    protected final static Pattern urlRegexp = Pattern.compile("^jdbc:clickhouse://([a-zA-Z0-9.-]+|\\[[:.a-fA-F0-9]+\\]):([0-9]+)(?:|/|/([a-zA-Z0-9_]+))$");

    protected final static String DEFAULT_DATABASE = "default";

    protected final ClickHouseDriver driver = new ClickHouseDriver();

    protected final String url;
    protected String host;
    protected int port;
    protected String database;

    PrintWriter printWriter;
    protected int loginTimeout = 0;

    private ClickHouseProperties properties;

    public ClickHouseDataSource(String url) {
        this(url, new ClickHouseProperties());
    }

    public ClickHouseDataSource(String url, Properties info) {
        this(url, new ClickHouseProperties(info));
    }

    public ClickHouseDataSource(String url, ClickHouseProperties properties) {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url: " + url);
        }
        this.url = url;

        Matcher m = urlRegexp.matcher(url);
        if (m.find()) {
            this.host = m.group(1);
            this.port = Integer.parseInt(m.group(2));
            if (m.group(3) != null) {
                this.database = m.group(3);
            } else {
                this.database = properties.getDatabase() == null ? DEFAULT_DATABASE : properties.getDatabase();
            }
        } else {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url: " + url);
        }
        this.properties = new ClickHouseProperties(properties);
        this.properties.setDatabase(database);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return driver.connect(url, properties);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return driver.connect(url, properties.withCredentials(username, password));
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

    public ClickHouseProperties getProperties() {
        return properties;
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

    /**
     * Schedules connections cleaning at a rate. Turned off by default.
     * See https://hc.apache.org/httpcomponents-client-4.5.x/tutorial/html/connmgmt.html#d5e418
     *
     * @param rate
     * @param timeUnit
     * @return this
     */
    public ClickHouseDataSource withConnectionsCleaning(int rate, TimeUnit timeUnit){
        driver.scheduleConnectionsCleaning(rate, timeUnit);
        return this;
    }
}
