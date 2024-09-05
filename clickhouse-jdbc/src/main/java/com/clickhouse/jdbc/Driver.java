package com.clickhouse.jdbc;

import java.sql.*;
import java.util.*;

import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * JDBC driver for ClickHouse.
 */
public class Driver implements java.sql.Driver {
    private static final Logger log = LoggerFactory.getLogger(Driver.class);


    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
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
