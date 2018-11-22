package ru.yandex.clickhouse;

import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ru.yandex.clickhouse.ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX;

/**
 * <p> Database for clickhouse jdbc connections.
 * <p> It has list of database urls.
 * For every {@link #getConnection() getConnection} invocation, it returns connection to random host from the list.
 * Furthermore, this class has method {@link #scheduleActualization(int, TimeUnit) scheduleActualization}
 * which test hosts for availability. By default, this option is turned off.
 */
public class BalancedClickhouseDataSource implements DataSource {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BalancedClickhouseDataSource.class);
    private static final Pattern URL_TEMPLATE = Pattern.compile(JDBC_CLICKHOUSE_PREFIX + "//([a-zA-Z0-9_:,.-]+)(/[a-zA-Z0-9_]+)?");

    private PrintWriter printWriter;
    private int loginTimeoutSeconds = 0;

    private final Random random = new Random(System.currentTimeMillis());
    private final List<String> allUrls;
    private volatile List<String> enabledUrls;

    private final ClickHouseProperties properties;
    private final ClickHouseDriver driver = new ClickHouseDriver();

    /**
     * create Datasource for clickhouse JDBC connections
     *
     * @param url address for connection to the database
     *            must have the next format {@code jdbc:clickhouse://<first-host>:<port>,<second-host>:<port>/<database> }
     *            for example, {@code jdbc:clickhouse://localhost:8123,localhost:8123/database }
     * @throws IllegalArgumentException if param have not correct format, or error happens when checking host availability
     */
    public BalancedClickhouseDataSource(final String url) {
        this(splitUrl(url), new ClickHouseProperties());
    }

    /**
     * create Datasource for clickhouse JDBC connections
     *
     * @param url        address for connection to the database
     * @param properties database properties
     * @see #BalancedClickhouseDataSource(String)
     */
    public BalancedClickhouseDataSource(final String url, Properties properties) {
        this(splitUrl(url), new ClickHouseProperties(properties));
    }

    /**
     * create Datasource for clickhouse JDBC connections
     *
     * @param url        address for connection to the database
     * @param properties database properties
     * @see #BalancedClickhouseDataSource(String)
     */
    public BalancedClickhouseDataSource(final String url, ClickHouseProperties properties) {
        this(splitUrl(url), properties);
    }

    private BalancedClickhouseDataSource(final List<String> urls) {
        this(urls, new ClickHouseProperties());
    }

    private BalancedClickhouseDataSource(final List<String> urls, Properties info) {
        this(urls, new ClickHouseProperties(info));
    }

    private BalancedClickhouseDataSource(final List<String> urls, ClickHouseProperties properties) {
        if (urls.isEmpty()) {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url list. It must be not empty");
        }

        try {
            ClickHouseProperties localProperties = ClickhouseJdbcUrlParser.parse(urls.get(0), properties.asProperties());
            localProperties.setHost(null);
            localProperties.setPort(-1);

            this.properties = localProperties;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }


        List<String> allUrls = new ArrayList<String>(urls.size());
        for (final String url : urls) {
            try {
                if (driver.acceptsURL(url)) {
                    allUrls.add(url);
                } else {
                    log.error("that url is has not correct format: {}", url);
                }
            } catch (SQLException e) {
                throw new IllegalArgumentException("error while checking url: " + url, e);
            }
        }

        if (allUrls.isEmpty()) {
            throw new IllegalArgumentException("there are no correct urls");
        }

        this.allUrls = Collections.unmodifiableList(allUrls);
        this.enabledUrls = this.allUrls;
    }

    static List<String> splitUrl(final String url) {
        Matcher m = URL_TEMPLATE.matcher(url);
        if (!m.matches()) {
            throw new IllegalArgumentException("Incorrect url");
        }
        String database = m.group(2);
        if (database == null) {
            database = "";
        }
        String[] hosts = m.group(1).split(",");
        final List<String> result = new ArrayList<String>(hosts.length);
        for (final String host : hosts) {
            result.add(JDBC_CLICKHOUSE_PREFIX + "//" + host + database);
        }
        return result;
    }


    private boolean ping(final String url) {
        try {
            driver.connect(url, properties).createStatement().execute("SELECT 1");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Checks if clickhouse on url is alive, if it isn't, disable url, else enable.
     *
     * @return number of avaliable clickhouse urls
     */
    public synchronized int actualize() {
        List<String> enabledUrls = new ArrayList<String>(allUrls.size());

        for (String url : allUrls) {
            log.debug("Pinging disabled url: {}", url);
            if (ping(url)) {
                log.debug("Url is alive now: {}", url);
                enabledUrls.add(url);
            } else {
                log.debug("Url is dead now: {}", url);
            }
        }

        this.enabledUrls = Collections.unmodifiableList(enabledUrls);
        return enabledUrls.size();
    }


    private String getAnyUrl() throws SQLException {
        List<String> localEnabledUrls = enabledUrls;
        if (localEnabledUrls.isEmpty()) {
            throw new SQLException("Unable to get connection: there are no enabled urls");
        }

        int index = random.nextInt(localEnabledUrls.size());
        return localEnabledUrls.get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        return driver.connect(getAnyUrl(), properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClickHouseConnection getConnection(String username, String password) throws SQLException {
        return driver.connect(getAnyUrl(), properties.withCredentials(username, password));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLogWriter(PrintWriter printWriter) throws SQLException {
        this.printWriter = printWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
//        throw new SQLFeatureNotSupportedException();
        loginTimeoutSeconds = seconds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    /**
     * {@inheritDoc}
     */
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * set time period of removing connections
     *
     * @param rate     value for time unit
     * @param timeUnit time unit for checking
     * @return this datasource with changed settings
     * @see ClickHouseDriver#scheduleConnectionsCleaning
     */
    public BalancedClickhouseDataSource withConnectionsCleaning(int rate, TimeUnit timeUnit) {
        driver.scheduleConnectionsCleaning(rate, timeUnit);
        return this;
    }

    /**
     * set time period for checking availability connections
     *
     * @param delay    value for time unit
     * @param timeUnit time unit for checking
     * @return this datasource with changed settings
     */
    public BalancedClickhouseDataSource scheduleActualization(int delay, TimeUnit timeUnit) {
        ClickHouseDriver.ScheduledConnectionCleaner.INSTANCE.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    actualize();
                } catch (Exception e) {
                    log.error("Unable to actualize urls", e);
                }
            }
        }, 0, delay, timeUnit);

        return this;
    }

    public List<String> getAllClickhouseUrls() {
        return allUrls;
    }

    public List<String> getEnabledClickHouseUrls() {
        return enabledUrls;
    }

    public List<String> getDisabledUrls() {
        List<String> enabledUrls = this.enabledUrls;
        if (!hasDisabledUrls()) {
            return Collections.emptyList();
        }
        List<String> disabledUrls = new ArrayList<String>(allUrls);
        disabledUrls.removeAll(enabledUrls);
        return disabledUrls;
    }

    public boolean hasDisabledUrls() {
        return allUrls.size() != enabledUrls.size();
    }
}
