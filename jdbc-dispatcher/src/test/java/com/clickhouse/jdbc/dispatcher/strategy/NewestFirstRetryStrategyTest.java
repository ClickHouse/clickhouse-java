package com.clickhouse.jdbc.dispatcher.strategy;

import com.clickhouse.jdbc.dispatcher.DriverVersion;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import static org.testng.Assert.*;

/**
 * Unit tests for NewestFirstRetryStrategy.
 */
public class NewestFirstRetryStrategyTest {

    /**
     * Mock driver implementation for testing.
     */
    private static class MockDriver implements Driver {
        @Override
        public Connection connect(String url, Properties info) { return null; }
        @Override
        public boolean acceptsURL(String url) { return true; }
        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) { return new DriverPropertyInfo[0]; }
        @Override
        public int getMajorVersion() { return 1; }
        @Override
        public int getMinorVersion() { return 0; }
        @Override
        public boolean jdbcCompliant() { return true; }
        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return Logger.getLogger(MockDriver.class.getName());
        }
    }

    private DriverVersion createVersion(String version) {
        return new DriverVersion(version, new MockDriver(), getClass().getClassLoader());
    }

    @Test
    public void testNewestFirstOrdering() {
        NewestFirstRetryStrategy strategy = new NewestFirstRetryStrategy(10, false);

        List<DriverVersion> versions = Arrays.asList(
                createVersion("1.0.0"),
                createVersion("3.0.0"),
                createVersion("2.0.0")
        );

        List<DriverVersion> result = strategy.getVersionsToTry(versions, RetryContext.forConnect(1));

        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getVersion(), "3.0.0");
        assertEquals(result.get(1).getVersion(), "2.0.0");
        assertEquals(result.get(2).getVersion(), "1.0.0");
    }

    @Test
    public void testHealthyVersionsFirst() {
        NewestFirstRetryStrategy strategy = new NewestFirstRetryStrategy(10, true);

        DriverVersion v1 = createVersion("1.0.0");
        DriverVersion v2 = createVersion("2.0.0");
        DriverVersion v3 = createVersion("3.0.0");

        // Mark newest as unhealthy
        v3.setHealthy(false);

        List<DriverVersion> versions = Arrays.asList(v1, v2, v3);
        List<DriverVersion> result = strategy.getVersionsToTry(versions, RetryContext.forConnect(1));

        // Healthy versions first, then unhealthy
        assertEquals(result.get(0).getVersion(), "2.0.0");
        assertEquals(result.get(1).getVersion(), "1.0.0");
        assertEquals(result.get(2).getVersion(), "3.0.0");
    }

    @Test
    public void testMaxRetriesLimit() {
        NewestFirstRetryStrategy strategy = new NewestFirstRetryStrategy(2, false);

        List<DriverVersion> versions = Arrays.asList(
                createVersion("1.0.0"),
                createVersion("2.0.0"),
                createVersion("3.0.0"),
                createVersion("4.0.0")
        );

        List<DriverVersion> result = strategy.getVersionsToTry(versions, RetryContext.forConnect(1));

        assertEquals(result.size(), 2);
        assertEquals(result.get(0).getVersion(), "4.0.0");
        assertEquals(result.get(1).getVersion(), "3.0.0");
    }

    @Test
    public void testEmptyVersionsList() {
        NewestFirstRetryStrategy strategy = new NewestFirstRetryStrategy();

        List<DriverVersion> result = strategy.getVersionsToTry(
                Collections.emptyList(),
                RetryContext.forConnect(1)
        );

        assertTrue(result.isEmpty());
    }

    @Test
    public void testNullVersionsList() {
        NewestFirstRetryStrategy strategy = new NewestFirstRetryStrategy();

        List<DriverVersion> result = strategy.getVersionsToTry(null, RetryContext.forConnect(1));

        assertTrue(result.isEmpty());
    }

    @Test
    public void testOnFailureMarksUnhealthy() {
        NewestFirstRetryStrategy strategy = new NewestFirstRetryStrategy();
        DriverVersion version = createVersion("1.0.0");

        assertTrue(version.isHealthy());

        strategy.onFailure(version, RetryContext.forConnect(1), new SQLException("Test error"));

        assertFalse(version.isHealthy());
    }
}
