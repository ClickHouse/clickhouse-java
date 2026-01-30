package com.clickhouse.jdbc.dispatcher;

import com.clickhouse.jdbc.dispatcher.loader.DriverVersionManager;
import com.clickhouse.jdbc.dispatcher.proxy.ConnectionProxy;
import com.clickhouse.jdbc.dispatcher.strategy.NewestFirstRetryStrategy;
import com.clickhouse.jdbc.dispatcher.strategy.RetryContext;
import com.clickhouse.jdbc.dispatcher.strategy.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * JDBC Driver implementation that dispatches connections across multiple driver versions.
 * <p>
 * This driver loads multiple versions of another JDBC driver and provides failover
 * capabilities when connecting or executing queries. When one driver version fails,
 * it automatically tries the next version according to the configured retry strategy.
 * <p>
 * Usage:
 * <pre>
 * // Create the dispatcher driver
 * DispatcherDriver dispatcher = new DispatcherDriver("com.clickhouse.jdbc.ClickHouseDriver");
 *
 * // Load driver versions from JARs
 * dispatcher.loadDriver(new File("clickhouse-jdbc-0.4.0.jar"), "0.4.0");
 * dispatcher.loadDriver(new File("clickhouse-jdbc-0.5.0.jar"), "0.5.0");
 *
 * // Connect using the dispatcher
 * Connection conn = dispatcher.connect("jdbc:clickhouse://localhost:8123", new Properties());
 * </pre>
 *
 * The dispatcher URL format is:
 * <pre>
 * jdbc:dispatcher:&lt;underlying-url&gt;
 * </pre>
 *
 * For example: jdbc:dispatcher:jdbc:clickhouse://localhost:8123
 */
public class DispatcherDriver implements Driver {

    private static final Logger log = LoggerFactory.getLogger(DispatcherDriver.class);

    public static final String URL_PREFIX = "jdbc:dispatcher:";
    public static final int MAJOR_VERSION = 1;
    public static final int MINOR_VERSION = 0;

    private final DriverVersionManager versionManager;
    private RetryStrategy retryStrategy;
    private boolean registered = false;

    /**
     * Creates a new DispatcherDriver for the specified driver class.
     *
     * @param driverClassName the fully qualified class name of the JDBC driver to load
     */
    public DispatcherDriver(String driverClassName) {
        this(driverClassName, new NewestFirstRetryStrategy());
    }

    /**
     * Creates a new DispatcherDriver with a custom retry strategy.
     *
     * @param driverClassName the fully qualified class name of the JDBC driver to load
     * @param retryStrategy   the retry strategy to use for failover
     */
    public DispatcherDriver(String driverClassName, RetryStrategy retryStrategy) {
        this.versionManager = new DriverVersionManager(driverClassName);
        this.retryStrategy = retryStrategy;
    }

    /**
     * Creates a new DispatcherDriver with a custom version manager.
     *
     * @param versionManager the pre-configured version manager
     * @param retryStrategy  the retry strategy to use for failover
     */
    public DispatcherDriver(DriverVersionManager versionManager, RetryStrategy retryStrategy) {
        this.versionManager = versionManager;
        this.retryStrategy = retryStrategy;
    }

    /**
     * Loads a driver version from a JAR file.
     *
     * @param jarFile the JAR file containing the driver
     * @param version the version string
     * @return the loaded DriverVersion
     * @throws Exception if loading fails
     */
    public DriverVersion loadDriver(File jarFile, String version) throws Exception {
        return versionManager.loadDriver(jarFile, version);
    }

    /**
     * Loads a driver version from a URL.
     *
     * @param jarUrl  the URL of the JAR file
     * @param version the version string
     * @return the loaded DriverVersion
     * @throws Exception if loading fails
     */
    public DriverVersion loadDriver(URL jarUrl, String version) throws Exception {
        return versionManager.loadDriver(jarUrl, version);
    }

    /**
     * Loads all driver versions from a directory.
     *
     * @param directory the directory containing driver JARs
     * @return the number of drivers loaded
     */
    public int loadFromDirectory(File directory) {
        return versionManager.loadFromDirectory(directory);
    }

    /**
     * Sets the retry strategy for this driver.
     *
     * @param retryStrategy the new retry strategy
     */
    public void setRetryStrategy(RetryStrategy retryStrategy) {
        this.retryStrategy = retryStrategy;
    }

    /**
     * Gets the current retry strategy.
     *
     * @return the retry strategy
     */
    public RetryStrategy getRetryStrategy() {
        return retryStrategy;
    }

    /**
     * Gets the version manager.
     *
     * @return the DriverVersionManager
     */
    public DriverVersionManager getVersionManager() {
        return versionManager;
    }

    /**
     * Registers this driver with the DriverManager.
     *
     * @throws SQLException if registration fails
     */
    public synchronized void register() throws SQLException {
        if (!registered) {
            DriverManager.registerDriver(this);
            registered = true;
            log.info("DispatcherDriver registered with DriverManager");
        }
    }

    /**
     * Deregisters this driver from the DriverManager.
     *
     * @throws SQLException if deregistration fails
     */
    public synchronized void deregister() throws SQLException {
        if (registered) {
            DriverManager.deregisterDriver(this);
            registered = false;
            log.info("DispatcherDriver deregistered from DriverManager");
        }
    }

    // ==================== Driver Interface Implementation ====================

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        // Strip the dispatcher prefix if present
        String targetUrl = url;
        if (url.startsWith(URL_PREFIX)) {
            targetUrl = url.substring(URL_PREFIX.length());
        }

        if (versionManager.isEmpty()) {
            throw new SQLException("No driver versions loaded. Load at least one driver version before connecting.");
        }

        List<DispatcherException.VersionFailure> failures = new ArrayList<>();
        List<DriverVersion> versionsToTry = retryStrategy.getVersionsToTry(
                versionManager.getVersions(),
                RetryContext.forConnect(1)
        );

        log.debug("Attempting connection with {} driver versions", versionsToTry.size());

        for (int attempt = 0; attempt < versionsToTry.size(); attempt++) {
            DriverVersion version = versionsToTry.get(attempt);
            RetryContext context = RetryContext.forConnect(attempt + 1);

            try {
                log.debug("Trying driver version {} (attempt {})", version.getVersion(), attempt + 1);

                Driver driver = version.getDriver();
                Connection conn = driver.connect(targetUrl, info);

                if (conn == null) {
                    throw new SQLException("Driver returned null connection for URL: " + targetUrl);
                }

                retryStrategy.onSuccess(version, context);
                log.info("Connected successfully using driver version {}", version.getVersion());

                // Wrap in ConnectionProxy for failover support
                return new ConnectionProxy(
                        versionManager,
                        targetUrl,
                        info,
                        retryStrategy,
                        version,
                        conn
                );

            } catch (SQLException e) {
                log.warn("Connection failed with driver version {}: {}",
                        version.getVersion(), e.getMessage());
                failures.add(new DispatcherException.VersionFailure(version.getVersion(), e));
                retryStrategy.onFailure(version, context, e);

                // If this was the last version, throw the aggregated exception
                if (attempt == versionsToTry.size() - 1) {
                    throw new DispatcherException(
                            "All driver versions failed to connect to: " + targetUrl,
                            failures
                    );
                }
            }
        }

        throw new DispatcherException("No driver versions available for connection", failures);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            return false;
        }

        // Accept dispatcher URLs
        if (url.startsWith(URL_PREFIX)) {
            return true;
        }

        // Also accept URLs that any of our loaded drivers would accept
        for (DriverVersion version : versionManager.getVersions()) {
            try {
                if (version.getDriver().acceptsURL(url)) {
                    return true;
                }
            } catch (SQLException e) {
                log.debug("Error checking URL acceptance for version {}: {}",
                        version.getVersion(), e.getMessage());
            }
        }

        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        // Delegate to the newest driver version
        DriverVersion newest = versionManager.getNewestVersion();
        if (newest != null) {
            String targetUrl = url;
            if (url != null && url.startsWith(URL_PREFIX)) {
                targetUrl = url.substring(URL_PREFIX.length());
            }
            return newest.getDriver().getPropertyInfo(targetUrl, info);
        }
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        // Return true if all loaded drivers are JDBC compliant
        for (DriverVersion version : versionManager.getVersions()) {
            if (!version.getDriver().jdbcCompliant()) {
                return false;
            }
        }
        return !versionManager.isEmpty();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // Try to get parent logger from the newest driver version
        DriverVersion newest = versionManager.getNewestVersion();
        if (newest != null) {
            return newest.getDriver().getParentLogger();
        }
        throw new SQLFeatureNotSupportedException("No driver versions loaded");
    }

    @Override
    public String toString() {
        return "DispatcherDriver{" +
                "driverClassName='" + versionManager.getDriverClassName() + '\'' +
                ", loadedVersions=" + versionManager.size() +
                ", retryStrategy=" + retryStrategy.getName() +
                '}';
    }
}
