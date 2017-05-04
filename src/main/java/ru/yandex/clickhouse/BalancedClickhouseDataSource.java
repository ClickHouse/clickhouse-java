package ru.yandex.clickhouse;

import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ru.yandex.clickhouse.ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX;

public class BalancedClickhouseDataSource implements DataSource {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BalancedClickhouseDataSource.class);
    private static final Pattern URL_TEMPLATE = Pattern.compile(JDBC_CLICKHOUSE_PREFIX + "//([a-zA-Z0-9_:,.]+)(/[a-zA-Z0-9_]+)?");

    private PrintWriter printWriter;
    private int loginTimeoutSeconds = 0;

    private final Random random = new Random(System.currentTimeMillis());
    private volatile List<String> disabledUrls = new ArrayList<String>();
    private volatile List<String> enabledUrls = new ArrayList<String>();

    private final ClickHouseProperties properties;
    private final ClickHouseDriver driver = new ClickHouseDriver();


    public BalancedClickhouseDataSource(final String url) {
        this(splitUrl(url), new ClickHouseProperties());
    }

    public BalancedClickhouseDataSource(final String url, Properties info) {
        this(splitUrl(url), new ClickHouseProperties(info));
    }

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


        List<String> enabledUrlList = new ArrayList<String>(urls.size());
        for (final String url : urls) {
            try {
                if (driver.acceptsURL(url)) {
                    enabledUrlList.add(url);
                } else {
                    log.error("that url is has not correct format: {}", url);
                }
            } catch (SQLException e) {
                throw new IllegalArgumentException("error while checking url: " + url, e);
            }
        }

        if (enabledUrlList.isEmpty()) {
            throw new IllegalArgumentException("there are no correct urls");
        }

        this.enabledUrls = enabledUrlList;
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
     * Checks if clickhouse on url is alive, if it isn't, disable url, else enable
     */
    void actualize() {
        int countOfUrls = enabledUrls.size() + disabledUrls.size();
        List<String> urls = new ArrayList<String>(countOfUrls);
        urls.addAll(enabledUrls);
        urls.addAll(disabledUrls);

        List<String> enabledUrlList = new ArrayList<String>(countOfUrls);
        List<String> disabledUrlList = new ArrayList<String>(countOfUrls);

        for (String url : urls) {
            log.debug("Pinging disabled url: {}", url);
            if (ping(url)) {
                log.debug("Url is alive now: {}", url);
                enabledUrlList.add(url);
            } else {
                log.debug("Url is dead now: {}", url);
                disabledUrlList.add(url);
            }
        }

        this.enabledUrls = enabledUrlList;
        this.disabledUrls = disabledUrlList;
    }


    private String getAnyUrl() {
        List<String> localEnabledUrls = enabledUrls;
        if (localEnabledUrls.isEmpty()) {
            throw new RuntimeException("Unable to get connection: there is no enabled urls");
        }

        int index = random.nextInt(localEnabledUrls.size());
        return localEnabledUrls.get(index);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return driver.connect(getAnyUrl(), properties);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return driver.connect(getAnyUrl(), properties.withCredentials(username, password));
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter printWriter) throws SQLException {
        this.printWriter = printWriter;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
//        throw new SQLFeatureNotSupportedException();
        loginTimeoutSeconds = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    public BalancedClickhouseDataSource withConnectionsCleaning(int rate, TimeUnit timeUnit) {
        driver.scheduleConnectionsCleaning(rate, timeUnit);
        return this;
    }

    public void scheduleActualization(int rate, TimeUnit timeUnit) {
        ClickHouseDriver.ScheduledConnectionCleaner.INSTANCE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    actualize();
                } catch (Exception e) {
                    log.error("Unable to actualize urls", e);
                }
            }
        }, 0, rate, timeUnit);
    }
}
