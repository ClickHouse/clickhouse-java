package com.clickhouse.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

public class VersionSelectingDriver implements java.sql.Driver {
    private static final Logger log = LoggerFactory.getLogger(VersionSelectingDriver.class);
    private java.sql.Driver driver;

    static {
        load();
    }

    public static void load() {
        try {
            log.debug("Loading the 'proxy' JDBC driver.");
            java.sql.DriverManager.registerDriver(new VersionSelectingDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register ClickHouse JDBC driver", e);
        }
    }

    public static void unload() {
        try {
            log.debug("Unloading the 'proxy' JDBC driver.");
            java.sql.DriverManager.deregisterDriver(new VersionSelectingDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to deregister ClickHouse JDBC driver", e);
        }
    }

    public static boolean isV2() {
        return isV2(null);
    }

    public static boolean isV2(String url) {
        log.debug("Checking if V2 driver is requested");
        boolean v2Flag = Boolean.parseBoolean(System.getProperty("clickhouse.jdbc.v2", "false"));

        if (v2Flag || (url != null && url.contains("clickhouse.jdbc.v2=true"))) {
            log.debug("V2 driver is requested");
            return true;
        }

        return false;
    }


    private java.sql.Driver getDriver(String url) {
        if (driver != null) {
            return driver;
        }

        if (isV2(url)) {
            driver = new com.clickhouse.jdbc.Driver();
        } else {
            driver = new ClickHouseDriver();
        }

        return driver;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        java.sql.Driver driver = getDriver(url);
        return driver.connect(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        java.sql.Driver driver = getDriver(url);
        return driver.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        java.sql.Driver driver = getDriver(url);
        return driver.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return driver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return driver.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return driver.jdbcCompliant();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return driver.getParentLogger();
    }
}
