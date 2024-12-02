package com.clickhouse.jdbc;


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

/**
 * JDBC driver for ClickHouse.
 */
public class Driver implements java.sql.Driver {
    private static final Logger log = LoggerFactory.getLogger(Driver.class);
    public static final String driverVersion;
    private final DataSourceImpl dataSource;

    public static String frameworksDetected = null;
    public static class FrameworksDetection {
        private static final List<String> FRAMEWORKS_TO_DETECT = Arrays.asList("apache.spark");
        static volatile String frameworksDetected = null;

        private FrameworksDetection() {}
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

    static {
        log.debug("Initializing ClickHouse JDBC driver V2");
        String tempDriverVersion = Driver.class.getPackage().getImplementationVersion();
        //If the version is not available, set it to 1.0
        if (tempDriverVersion == null || tempDriverVersion.isEmpty()) {
            log.warn("ClickHouse JDBC driver version is not available");
            tempDriverVersion = "1.0";
        }

        driverVersion = tempDriverVersion;
        log.info("ClickHouse JDBC driver version: {}", driverVersion);

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
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            log.error("Failed to register ClickHouse JDBC driver", e);
        }
    }

    public static void unload() {
        try {
            DriverManager.deregisterDriver(new Driver());
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
        return JdbcConfiguration.getDriverPropertyInfo(info).toArray(new DriverPropertyInfo[0]);
    }

    public static int getDriverMajorVersion() {
        return Integer.parseInt(driverVersion.split("\\.")[0]);
    }

    @Override
    public int getMajorVersion() {
        //Convert the version string to an integer
        return Integer.parseInt(driverVersion.split("\\.")[0]);
    }

    public static int getDriverMinorVersion() {
        return Integer.parseInt(driverVersion.split("\\.")[1]);
    }

    @Override
    public int getMinorVersion() {
        //Convert the version string to an integer
        return Integer.parseInt(driverVersion.split("\\.")[1]);
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }
}
