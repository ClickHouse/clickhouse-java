package com.clickhouse.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

@Deprecated
public class ClickHouseDriver implements java.sql.Driver {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseDriver.class);
    private java.sql.Driver driver;
    private boolean urlFlagSent;

    static {
        load();
    }

    public ClickHouseDriver() {
//        log.debug("Creating a new instance of the 'proxy' ClickHouseDriver");
        log.info("ClickHouse JDBC driver version: {}", ClickHouseDriver.class.getPackage().getImplementationVersion());
        urlFlagSent = false;
        this.driver = getDriver(null);
    }

    public static void load() {
        try {
            log.debug("Loading the 'proxy' JDBC driver into DriverManager.");
            java.sql.DriverManager.registerDriver(new ClickHouseDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register ClickHouse JDBC driver", e);
        }
    }

    public static void unload() {
        try {
            log.debug("Unloading the 'proxy' JDBC driver.");
            java.sql.DriverManager.deregisterDriver(new ClickHouseDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to deregister ClickHouse JDBC driver", e);
        }
    }


    public static boolean isV2() {
        return new ClickHouseDriver().isV2(null);
    }
    public boolean isV2(String url) {
        log.debug("Checking if V1 driver is requested. V2 is the default driver.");
        boolean v1Flag = Boolean.parseBoolean(System.getProperty("clickhouse.jdbc.v1", "false"));
        if (v1Flag) {
            log.info("V1 driver is requested through system property.");
            return false;
        }

        if (url != null && url.contains("clickhouse.jdbc.v")) {
            urlFlagSent = true;

            if (url.contains("clickhouse.jdbc.v1=true")) {
                log.info("V1 driver is requested through URL.");
                return false;
            } if (url.contains("clickhouse.jdbc.v2=false")) {
                log.info("V1 driver is requested through URL.");
                return false;
            } else {
                log.info("V2 driver is requested through URL.");
                return true;
            }
        }

        return true;
    }


    private java.sql.Driver getDriver(String url) {
        if (urlFlagSent && driver != null) {// if the URL flag was sent, we don't need to check the URL again
            return driver;
        }

        if (isV2(url)) {
            log.info("v2 driver");
            driver = new com.clickhouse.jdbc.Driver();
        } else {
            log.info("v1 driver");
            driver = new DriverV1();
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
