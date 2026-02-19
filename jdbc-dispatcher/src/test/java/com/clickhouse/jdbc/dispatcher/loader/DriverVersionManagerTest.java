package com.clickhouse.jdbc.dispatcher.loader;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Unit tests for DriverVersionManager.
 */
public class DriverVersionManagerTest {

    @Test
    public void testExtractVersionFromFilename() {
        // Standard versions
        assertEquals(DriverVersionManager.extractVersionFromFilename("driver-1.2.3.jar"), "1.2.3");
        assertEquals(DriverVersionManager.extractVersionFromFilename("clickhouse-jdbc-0.4.6.jar"), "0.4.6");

        // With SNAPSHOT suffix
        assertEquals(DriverVersionManager.extractVersionFromFilename("driver-1.0.0-SNAPSHOT.jar"), "1.0.0-SNAPSHOT");

        // With underscore separator
        assertEquals(DriverVersionManager.extractVersionFromFilename("driver_2.0.0.jar"), "2.0.0");

        // Two-part version
        assertEquals(DriverVersionManager.extractVersionFromFilename("driver-1.0.jar"), "1.0");

        // Complex version with RC/beta
        assertEquals(DriverVersionManager.extractVersionFromFilename("driver-1.0.0-RC1.jar"), "1.0.0-RC1");
    }

    @Test
    public void testExtractVersionFromFilenameInvalid() {
        // No version in filename
        assertNull(DriverVersionManager.extractVersionFromFilename("driver.jar"));

        // Just text
        assertNull(DriverVersionManager.extractVersionFromFilename("noversion.jar"));
    }

    @Test
    public void testManagerConstruction() {
        DriverVersionManager manager = new DriverVersionManager("com.example.Driver");

        assertEquals(manager.getDriverClassName(), "com.example.Driver");
        assertTrue(manager.isEmpty());
        assertEquals(manager.size(), 0);
        assertNull(manager.getNewestVersion());
        assertTrue(manager.getVersions().isEmpty());
    }

    @Test
    public void testHealthCheckCooldown() {
        DriverVersionManager manager = new DriverVersionManager("com.example.Driver", 5000L);
        assertEquals(manager.getHealthCheckCooldownMs(), 5000L);

        // Default cooldown
        DriverVersionManager manager2 = new DriverVersionManager("com.example.Driver");
        assertEquals(manager2.getHealthCheckCooldownMs(), 60000L);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullDriverClassName() {
        new DriverVersionManager(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLoadFromNonDirectory() {
        DriverVersionManager manager = new DriverVersionManager("com.example.Driver");
        manager.loadFromDirectory(new java.io.File("/nonexistent/path"));
    }
}
