package com.clickhouse.jdbc.dispatcher;

import org.testng.annotations.Test;

import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import static org.testng.Assert.*;

/**
 * Unit tests for DriverVersion class.
 */
public class DriverVersionTest {

    /**
     * Mock driver implementation for testing.
     */
    private static class MockDriver implements Driver {
        @Override
        public java.sql.Connection connect(String url, Properties info) throws SQLException {
            return null;
        }

        @Override
        public boolean acceptsURL(String url) throws SQLException {
            return true;
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return true;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return Logger.getLogger(MockDriver.class.getName());
        }
    }

    @Test
    public void testVersionParsing() {
        Driver mockDriver = new MockDriver();
        ClassLoader cl = getClass().getClassLoader();

        DriverVersion v1 = new DriverVersion("1.2.3", mockDriver, cl);
        assertEquals(v1.getMajorVersion(), 1);
        assertEquals(v1.getMinorVersion(), 2);
        assertEquals(v1.getPatchVersion(), 3);

        DriverVersion v2 = new DriverVersion("0.5.0-SNAPSHOT", mockDriver, cl);
        assertEquals(v2.getMajorVersion(), 0);
        assertEquals(v2.getMinorVersion(), 5);
        assertEquals(v2.getPatchVersion(), 0);

        DriverVersion v3 = new DriverVersion("2.0", mockDriver, cl);
        assertEquals(v3.getMajorVersion(), 2);
        assertEquals(v3.getMinorVersion(), 0);
        assertEquals(v3.getPatchVersion(), 0);
    }

    @Test
    public void testVersionComparison() {
        Driver mockDriver = new MockDriver();
        ClassLoader cl = getClass().getClassLoader();

        List<DriverVersion> versions = new ArrayList<>();
        versions.add(new DriverVersion("1.0.0", mockDriver, cl));
        versions.add(new DriverVersion("2.0.0", mockDriver, cl));
        versions.add(new DriverVersion("1.5.0", mockDriver, cl));
        versions.add(new DriverVersion("0.9.0", mockDriver, cl));

        Collections.sort(versions);

        // Should be sorted in descending order (newest first)
        assertEquals(versions.get(0).getVersion(), "2.0.0");
        assertEquals(versions.get(1).getVersion(), "1.5.0");
        assertEquals(versions.get(2).getVersion(), "1.0.0");
        assertEquals(versions.get(3).getVersion(), "0.9.0");
    }

    @Test
    public void testHealthStatus() {
        Driver mockDriver = new MockDriver();
        ClassLoader cl = getClass().getClassLoader();

        DriverVersion v = new DriverVersion("1.0.0", mockDriver, cl);
        assertTrue(v.isHealthy());

        v.setHealthy(false);
        assertFalse(v.isHealthy());
        assertTrue(v.getLastFailureTime() > 0);

        v.setHealthy(true);
        assertTrue(v.isHealthy());
    }

    @Test
    public void testHealthCooldown() throws InterruptedException {
        Driver mockDriver = new MockDriver();
        ClassLoader cl = getClass().getClassLoader();

        DriverVersion v = new DriverVersion("1.0.0", mockDriver, cl);
        v.setHealthy(false);
        assertFalse(v.isHealthy());

        // Should not reset with 10 second cooldown
        assertFalse(v.resetHealthIfCooledDown(10000));
        assertFalse(v.isHealthy());

        // Should reset with 0 second cooldown (or wait)
        Thread.sleep(10);
        assertTrue(v.resetHealthIfCooledDown(1)); // 1ms cooldown
        assertTrue(v.isHealthy());
    }

    @Test
    public void testEqualsAndHashCode() {
        Driver mockDriver = new MockDriver();
        ClassLoader cl = getClass().getClassLoader();

        DriverVersion v1 = new DriverVersion("1.0.0", mockDriver, cl);
        DriverVersion v2 = new DriverVersion("1.0.0", mockDriver, cl);
        DriverVersion v3 = new DriverVersion("2.0.0", mockDriver, cl);

        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
        assertNotEquals(v1, v3);
    }
}
