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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ru.yandex.clickhouse.ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX;

public class BalancedClickhouseDataSource implements DataSource {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BalancedClickhouseDataSource.class);
    private static final Pattern URL_TEMPLATE = Pattern.compile(JDBC_CLICKHOUSE_PREFIX + "//([a-zA-Z0-9_:,.]+)(/[a-zA-Z0-9_]+)?");

    protected PrintWriter printWriter;
    protected int loginTimeout = 0;
    protected final ClickHouseDriver driver = new ClickHouseDriver();

    private Random rnd = new Random();
    private Set<String> disabledUrls = new HashSet<String>();
    private Set<String> urls = new HashSet<String>();

    private ClickHouseProperties properties;

    public static List<String> splitUrl(final String url) {
        Matcher m = URL_TEMPLATE.matcher(url);
        if (!m.matches()) {
            throw new IllegalArgumentException("Incorrect url");
        }
        String host = m.group(1);
        String database = m.group(2);
        if (database == null) {
            database = "";
        }
        String[] hosts = host.split(",");
        final List<String> res = new ArrayList<String>(hosts.length);
        for (final String h : hosts) {
            res.add(JDBC_CLICKHOUSE_PREFIX + "//" + h + database);
        }
        return res;
    }


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
            this.properties = ClickhouseJdbcUrlParser.parse(urls.get(0), properties.asProperties());
            properties.setHost(null);
            properties.setPort(-1);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }

        for (final String url : urls) {
            addUrl(url);
        }
    }

    private void disableUrl(final String url) {
        synchronized (urls) {
            urls.remove(url);
            disabledUrls.add(url);
        }
    }

    private void removeUrl(final String url) {
        synchronized (urls) {
            urls.remove(url);
            disabledUrls.remove(url);
        }
    }

    private void addUrl(final String url) {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url. It must be not null");
        }
        synchronized (urls) {
            urls.add(url);
            disabledUrls.remove(url);
        }
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
    public void actualize() {
       final List<String> urlsToEnable = new ArrayList<String>();
       final List<String> urlsToDisable = new ArrayList<String>();
       for (final String url : disabledUrls) {
           log.debug("Pinging disabled url: " + url);
           if (ping(url)) {
               log.debug("Url is alive now: " + url);
               urlsToEnable.add(url);
           }
       }
        for (final String url : urls) {
            log.debug("Pinging enabled url: " + url);
            if (!ping(url)) {
                log.debug("Url is dead now: " + url);
                urlsToDisable.add(url);
            }
        }

        for (final String url: urlsToEnable) {
            addUrl(url);
        }

        for (final String url: urlsToDisable) {
            disableUrl(url);
        }
    }


    private String getAnyUrl() {
        final List<String> sources = new ArrayList<String>(urls);
        if (sources.isEmpty()) {
            throw new RuntimeException("Unable to get connection: there is no enabled urls");
        }
        final int idx = rnd.nextInt() % sources.size();
        return sources.get(idx);
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
        loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    public BalancedClickhouseDataSource withConnectionsCleaning(int rate, TimeUnit timeUnit) {
        driver.scheduleConnectionsCleaning(rate, timeUnit);
        return this;
    }

    public void scheduleActualization(int rate, TimeUnit timeUnit){
        BalancedClickhouseDataSource.ScheduledActualizer.INSTANCE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    actualize();
                } catch (Exception e){
                    log.error("Unable to actualize urls: " + e);
                }
            }
        }, 0, rate, timeUnit);
    }

    private static class ScheduledActualizer {
        private static final ScheduledExecutorService INSTANCE = Executors.newSingleThreadScheduledExecutor();
    }
}
