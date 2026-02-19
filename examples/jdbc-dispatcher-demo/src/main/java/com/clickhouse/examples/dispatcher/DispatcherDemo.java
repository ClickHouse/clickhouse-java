package com.clickhouse.examples.dispatcher;

import com.clickhouse.jdbc.dispatcher.DispatcherDriver;
import com.clickhouse.jdbc.dispatcher.DispatcherException;
import com.clickhouse.jdbc.dispatcher.DriverVersion;
import com.clickhouse.jdbc.dispatcher.strategy.NewestFirstRetryStrategy;
import com.clickhouse.jdbc.dispatcher.strategy.RoundRobinRetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Demonstrates the JDBC Dispatcher functionality with multiple ClickHouse JDBC driver versions.
 * 
 * <p>This example shows how to:
 * <ul>
 *   <li>Load multiple driver versions from a directory</li>
 *   <li>Use different retry strategies</li>
 *   <li>Handle failover scenarios</li>
 *   <li>Register with DriverManager</li>
 * </ul>
 * 
 * <p>Before running, ensure you have a ClickHouse server running on localhost:8123.
 * You can start one using Docker:
 * <pre>
 * docker run -d -p 8123:8123 --name clickhouse clickhouse/clickhouse-server
 * </pre>
 */
public class DispatcherDemo {

    private static final Logger log = LoggerFactory.getLogger(DispatcherDemo.class);

    // Configuration - adjust these for your environment
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/default";
    private static final String DRIVERS_DIR = "drivers";
    private static final String DRIVER_CLASS_NAME = "com.clickhouse.jdbc.ClickHouseDriver";

    public static void main(String[] args) {
        log.info("=== JDBC Dispatcher Demo ===");
        log.info("");

        try {
            // Demo 1: Basic usage with directory loading
            demoBasicUsage();

            // Demo 2: Using different retry strategies
            demoRetryStrategies();

            // Demo 3: Using with DriverManager
            demoDriverManagerIntegration();

            // Demo 4: Inspecting loaded versions
            demoVersionInspection();

            log.info("");
            log.info("=== All demos completed successfully! ===");

        } catch (Exception e) {
            log.error("Demo failed", e);
            System.exit(1);
        }
    }

    /**
     * Demo 1: Basic usage - load drivers from directory and execute queries.
     */
    private static void demoBasicUsage() throws SQLException {
        log.info("--- Demo 1: Basic Usage ---");

        File driversDir = new File(DRIVERS_DIR);
        if (!driversDir.exists() || !driversDir.isDirectory()) {
            throw new IllegalStateException("Drivers directory not found: " + driversDir.getAbsolutePath() +
                    ". Run './gradlew downloadDrivers' first.");
        }

        // Create dispatcher and load all drivers from directory
        DispatcherDriver dispatcher = new DispatcherDriver(DRIVER_CLASS_NAME);
        int loadedCount = dispatcher.loadFromDirectory(driversDir);
        log.info("Loaded {} driver versions from {}", loadedCount, driversDir.getAbsolutePath());

        // Connect and execute a query
        Properties props = new Properties();
        props.setProperty("user", "default");
        props.setProperty("password", "");

        try (Connection conn = dispatcher.connect(CLICKHOUSE_URL, props);
             Statement stmt = conn.createStatement()) {

            // Query ClickHouse version
            try (ResultSet rs = stmt.executeQuery("SELECT version()")) {
                if (rs.next()) {
                    log.info("Connected to ClickHouse version: {}", rs.getString(1));
                }
            }

            // Execute a simple calculation
            try (ResultSet rs = stmt.executeQuery("SELECT 1 + 1 AS result, now() AS current_time")) {
                if (rs.next()) {
                    log.info("Query result: {} at {}", rs.getInt("result"), rs.getTimestamp("current_time"));
                }
            }

            log.info("Demo 1 completed successfully");
        }
        log.info("");
    }

    /**
     * Demo 2: Using different retry strategies.
     */
    private static void demoRetryStrategies() throws SQLException {
        log.info("--- Demo 2: Retry Strategies ---");

        File driversDir = new File(DRIVERS_DIR);

        // Strategy 1: NewestFirstRetryStrategy (default)
        log.info("Using NewestFirstRetryStrategy...");
        DispatcherDriver newestFirstDispatcher = new DispatcherDriver(
                DRIVER_CLASS_NAME,
                new NewestFirstRetryStrategy(3, true)
        );
        newestFirstDispatcher.loadFromDirectory(driversDir);
        executeSimpleQuery(newestFirstDispatcher, "NewestFirst");

        // Strategy 2: RoundRobinRetryStrategy
        log.info("Using RoundRobinRetryStrategy...");
        DispatcherDriver roundRobinDispatcher = new DispatcherDriver(
                DRIVER_CLASS_NAME,
                new RoundRobinRetryStrategy(3)
        );
        roundRobinDispatcher.loadFromDirectory(driversDir);

        // Execute multiple queries to demonstrate round-robin behavior
        for (int i = 1; i <= 3; i++) {
            executeSimpleQuery(roundRobinDispatcher, "RoundRobin-" + i);
        }

        log.info("Demo 2 completed successfully");
        log.info("");
    }

    /**
     * Demo 3: Integration with standard JDBC DriverManager.
     */
    private static void demoDriverManagerIntegration() throws SQLException {
        log.info("--- Demo 3: DriverManager Integration ---");

        File driversDir = new File(DRIVERS_DIR);

        // Create and configure dispatcher
        DispatcherDriver dispatcher = new DispatcherDriver(DRIVER_CLASS_NAME);
        dispatcher.loadFromDirectory(driversDir);

        // Register with DriverManager
        dispatcher.register();
        log.info("Dispatcher registered with DriverManager");

        try {
            // Use DriverManager with dispatcher URL prefix
            String dispatcherUrl = "jdbc:dispatcher:" + CLICKHOUSE_URL;
            try (Connection conn = DriverManager.getConnection(dispatcherUrl, "default", "")) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT 'Hello from DriverManager!' AS message")) {
                    if (rs.next()) {
                        log.info("DriverManager query result: {}", rs.getString("message"));
                    }
                }
            }
            log.info("Demo 3 completed successfully");
        } finally {
            // Always deregister when done
            dispatcher.deregister();
            log.info("Dispatcher deregistered from DriverManager");
        }
        log.info("");
    }

    /**
     * Demo 4: Inspecting loaded driver versions.
     */
    private static void demoVersionInspection() throws SQLException {
        log.info("--- Demo 4: Version Inspection ---");

        File driversDir = new File(DRIVERS_DIR);

        DispatcherDriver dispatcher = new DispatcherDriver(DRIVER_CLASS_NAME);
        dispatcher.loadFromDirectory(driversDir);

        // List all loaded versions
        log.info("Loaded driver versions:");
        for (DriverVersion version : dispatcher.getVersionManager().getVersions()) {
            log.info("  - Version {}: healthy={}, major={}, minor={}",
                    version.getVersion(),
                    version.isHealthy(),
                    version.getMajorVersion(),
                    version.getMinorVersion());
        }

        // Get newest version
        DriverVersion newest = dispatcher.getVersionManager().getNewestVersion();
        if (newest != null) {
            log.info("Newest version: {}", newest.getVersion());
        }

        // Demonstrate marking a version as unhealthy
        log.info("Marking version 0.7.2 as unhealthy for demonstration...");
        dispatcher.getVersionManager().markUnhealthy("0.7.2");

        // Show updated health status
        log.info("Updated version status:");
        for (DriverVersion version : dispatcher.getVersionManager().getVersions()) {
            log.info("  - Version {}: healthy={}", version.getVersion(), version.isHealthy());
        }

        // Connection should still work, using the healthy version
        Properties props = new Properties();
        props.setProperty("user", "default");
        props.setProperty("password", "");

        try (Connection conn = dispatcher.connect(CLICKHOUSE_URL, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 'Still working!' AS status")) {
            if (rs.next()) {
                log.info("Query with unhealthy version marked: {}", rs.getString("status"));
            }
        }

        log.info("Demo 4 completed successfully");
        log.info("");
    }

    /**
     * Helper method to execute a simple query and log the result.
     */
    private static void executeSimpleQuery(DispatcherDriver dispatcher, String label) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "default");
        props.setProperty("password", "");

        try (Connection conn = dispatcher.connect(CLICKHOUSE_URL, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT now64() AS timestamp")) {
            if (rs.next()) {
                log.info("  [{}] Query executed at: {}", label, rs.getString("timestamp"));
            }
        }
    }

    /**
     * Demonstrates error handling when all driver versions fail.
     * This method is not called by default as it requires a non-existent server.
     */
    @SuppressWarnings("unused")
    private static void demoFailoverHandling() {
        log.info("--- Demo: Failover Handling ---");

        File driversDir = new File(DRIVERS_DIR);
        DispatcherDriver dispatcher = new DispatcherDriver(DRIVER_CLASS_NAME);
        dispatcher.loadFromDirectory(driversDir);

        Properties props = new Properties();
        props.setProperty("user", "default");
        props.setProperty("password", "");

        // Try to connect to a non-existent server
        try {
            Connection conn = dispatcher.connect("jdbc:clickhouse://nonexistent:8123", props);
            conn.close();
        } catch (DispatcherException e) {
            log.error("All driver versions failed to connect:");
            for (DispatcherException.VersionFailure failure : e.getFailures()) {
                log.error("  - Version {} failed: {}",
                        failure.getVersion(),
                        failure.getException().getMessage());
            }
        } catch (SQLException e) {
            log.error("Connection failed", e);
        }
    }
}
