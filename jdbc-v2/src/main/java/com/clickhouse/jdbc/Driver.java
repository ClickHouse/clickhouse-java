package com.clickhouse.jdbc;


import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.JdbcConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JDBC driver for ClickHouse.
 */
public class Driver implements java.sql.Driver {
    private static final Logger log = LoggerFactory.getLogger(Driver.class);

    private static final String driverVersion;
    private static final int majorVersion;
    private static final int minorVersion;
    private final DataSourceImpl dataSource;

    public static String frameworksDetected = null;

    public static class FrameworksDetection {
        private static final List<String> FRAMEWORKS_TO_DETECT = Arrays.asList("apache.spark", "apache.flink", "apache.nifi");
        static volatile String frameworksDetected = null;

        private FrameworksDetection() {
        }

        public static String getFrameworksDetected() {
            if (frameworksDetected == null) {//Only detect frameworks once
                Set<String> inferredFrameworks = new LinkedHashSet<>();
                for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
                    for (String framework : FRAMEWORKS_TO_DETECT) {
                        if (ste.toString().contains(framework)) {
                            inferredFrameworks.add(String.format("(%s)", framework));
                        }
                    }
                }

                frameworksDetected = String.join("; ", inferredFrameworks);
            }

            return frameworksDetected;
        }
    }

    public static final String DRIVER_CLIENT_NAME = "jdbc-v2/";

    static {
        log.debug("Initializing ClickHouse JDBC driver V2");

        driverVersion = ClickHouseClientOption.readVersionFromResource("jdbc-v2-version.properties");
        log.debug("ClickHouse JDBC driver version: {}", driverVersion);

        int[] versions = parseVersion(driverVersion);

        majorVersion = versions[0];
        minorVersion = versions[1];

        //Load the driver
        //load(); //Commented out to avoid loading the driver multiple times, because we're referenced in V1
    }

    public Driver() {
        this.dataSource = null;
    }

    public Driver(DataSourceImpl dataSourceImpl) {
        this.dataSource = dataSourceImpl;
    }

    public static void load() {
        try {
            DriverManager.registerDriver(INSTANCE);
        } catch (SQLException e) {
            log.error("Failed to register ClickHouse JDBC driver", e);
        }
    }

    public static void unload() {
        try {
            DriverManager.deregisterDriver(INSTANCE);
        } catch (SQLException e) {
            log.error("Failed to deregister ClickHouse JDBC driver", e);
        }
    }


    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        return new ConnectionImpl(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return JdbcConfiguration.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new JdbcConfiguration(url, info).getDriverPropertyInfo().toArray(new DriverPropertyInfo[0]);
    }

    public static int getDriverMajorVersion() {
        return majorVersion;
    }

    @Override
    public int getMajorVersion() {
        return majorVersion;
    }

    public static int getDriverMinorVersion() {
        return minorVersion;
    }

    @Override
    public int getMinorVersion() {
        return minorVersion;
    }

    @Override
    public boolean jdbcCompliant() {
        // Mainly because of not supported Transactions.
        return false;
    }

    public static String chSettingKey(String key) {
        return ClientConfigProperties.serverSetting(key);
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    public static String getLibraryVersion() {
        return driverVersion;
    }

    private static final Driver INSTANCE = new Driver();

    public static int[] parseVersion(String version) {
        if (version != null) {
            final Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)");

            Matcher matcher = pattern.matcher(version);
            if (matcher.find()) {
                int major = Integer.parseInt(matcher.group(1));
                int minor = Integer.parseInt(matcher.group(2));
                int patch = Integer.parseInt(matcher.group(3));
                int majorVersion = (major << 16) | minor;
                return new int[]{majorVersion, patch};
            }
        }
        return new int[] { 0, 0 };
    }
}
