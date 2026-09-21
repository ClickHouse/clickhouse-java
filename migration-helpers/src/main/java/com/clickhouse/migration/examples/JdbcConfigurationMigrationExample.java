package com.clickhouse.migration.examples;

import com.clickhouse.migration.config.ConfigurationMigrationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Example demonstrating how to migrate v1 JDBC configuration properties and connection URLs
 * to v2 format using {@link ConfigurationMigrationHelper} before establishing a JDBC connection.
 */
public class JdbcConfigurationMigrationExample {

    private static final Logger log = LoggerFactory.getLogger(JdbcConfigurationMigrationExample.class);

    /**
     * Converts legacy v1 connection properties to v2 format and creates a JDBC connection.
     *
     * @param url connection URL (e.g., "jdbc:clickhouse://localhost:8123/default")
     * @param v1Properties legacy properties containing v1 option names and un-prefixed server settings
     * @return active JDBC connection
     * @throws SQLException if a database access error occurs
     */
    public Connection createConnectionWithConvertedProperties(String url, Properties v1Properties) throws SQLException {
        // Convert v1 properties (un-prefixed server settings, renamed keys, custom_settings) to v2 format
        Properties v2Properties = ConfigurationMigrationHelper.convertProperties(v1Properties);

        // Connect using standard JDBC DriverManager with converted v2 properties
        return DriverManager.getConnection(url, v2Properties);
    }

    /**
     * Converts a legacy v1 connection URL (containing query parameters) to v2 format
     * and creates a JDBC connection.
     *
     * @param v1Url v1 connection URL containing query parameters (e.g. {@code "jdbc:clickhouse://localhost:8123/default?max_threads=8&connect_timeout=5000"})
     * @return active JDBC connection
     * @throws SQLException if a database access error occurs
     */
    public Connection createConnectionWithConvertedUrl(String v1Url) throws SQLException {
        // Convert v1 URL query parameters to v2 format (e.g. max_threads -> clickhouse_setting_max_threads)
        String v2Url = ConfigurationMigrationHelper.convertUrl(v1Url);

        // Connect using standard JDBC DriverManager with the converted v2 URL
        return DriverManager.getConnection(v2Url);
    }

    /**
     * Demonstrates complete workflow: migrating v1 configuration and executing a query with the v2 JDBC driver.
     *
     * @throws SQLException if a database access error occurs
     */
    public void executeQueryWithMigratedConfig() throws SQLException {
        // 1. Build legacy v1 properties
        Properties v1Props = new Properties();
        v1Props.setProperty("user", "default");
        v1Props.setProperty("password", "");
        v1Props.setProperty("connect_timeout", "10000"); // Renamed in v2 to connection_timeout
        v1Props.setProperty("max_threads", "4");         // Server setting in v1; needs clickhouse_setting_ prefix in v2
        v1Props.setProperty("custom_settings", "join_use_nulls=1"); // Legacy custom_settings property

        // 2. Convert to v2 properties
        Properties v2Props = ConfigurationMigrationHelper.convertProperties(v1Props);
        log.info("v2Props: {}", v2Props);

        // 3. Connect and execute query with try-with-resources
        String url = "jdbc:clickhouse://localhost:8123/default";
        try (Connection conn = DriverManager.getConnection(url, v2Props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {

            while (rs.next()) {
                int value = rs.getInt(1);
                // process result
            }
        }
    }

    /**
     * Main entry point demonstrating configuration migration execution.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        try {
            new JdbcConfigurationMigrationExample().executeQueryWithMigratedConfig();
        } catch (Exception e) {
            log.error("failed to query with migration config", e);
        }
    }
}
