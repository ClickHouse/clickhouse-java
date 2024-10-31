package com.clickhouse.jdbc;

import java.sql.*;
import java.util.*;

import com.clickhouse.jdbc.internal.JdbcConfiguration;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * JDBC driver for ClickHouse.
 */
public class Driver implements java.sql.Driver {
    private static final Logger log = LoggerFactory.getLogger(Driver.class);
    public static final String driverVersion;

    static {
        String tempDriverVersion = Driver.class.getPackage().getImplementationVersion();
        //If the version is not available, set it to 1.0
        if (tempDriverVersion == null || tempDriverVersion.isEmpty()) {
            log.warn("ClickHouse JDBC driver version is not available");
            tempDriverVersion = "1.0";
        }

        driverVersion = tempDriverVersion;
        log.info("ClickHouse JDBC driver version: {}", driverVersion);

        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            log.error("Failed to register ClickHouse JDBC driver", e);
        }
    }


    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return new ConnectionImpl(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return JdbcConfiguration.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (!JdbcConfiguration.acceptsURL(url)) {
            return new DriverPropertyInfo[0];
        }
        return new JdbcConfiguration(url, info).getPropertyInfo();
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
