package com.clickhouse.jdbc.dispatcher;

import java.sql.Driver;
import java.util.Objects;

/**
 * Represents a loaded JDBC driver version with its associated metadata.
 * This class encapsulates a driver instance along with version information
 * that can be used for ordering and selection during failover.
 */
public class DriverVersion implements Comparable<DriverVersion> {

    private final String version;
    private final Driver driver;
    private final ClassLoader classLoader;
    private final int majorVersion;
    private final int minorVersion;
    private final int patchVersion;
    private volatile boolean healthy = true;
    private volatile long lastFailureTime = 0;

    /**
     * Creates a new DriverVersion with the specified version string and driver instance.
     *
     * @param version     the version string (e.g., "1.2.3")
     * @param driver      the loaded JDBC driver instance
     * @param classLoader the classloader used to load this driver
     */
    public DriverVersion(String version, Driver driver, ClassLoader classLoader) {
        this.version = Objects.requireNonNull(version, "version cannot be null");
        this.driver = Objects.requireNonNull(driver, "driver cannot be null");
        this.classLoader = Objects.requireNonNull(classLoader, "classLoader cannot be null");

        int[] parsed = parseVersion(version);
        this.majorVersion = parsed[0];
        this.minorVersion = parsed[1];
        this.patchVersion = parsed[2];
    }

    /**
     * Parses a version string into major, minor, and patch components.
     *
     * @param version the version string to parse
     * @return an array of [major, minor, patch]
     */
    private static int[] parseVersion(String version) {
        int[] result = new int[3];
        String[] parts = version.split("[.\\-]");
        for (int i = 0; i < Math.min(parts.length, 3); i++) {
            try {
                result[i] = Integer.parseInt(parts[i].replaceAll("[^0-9]", ""));
            } catch (NumberFormatException e) {
                result[i] = 0;
            }
        }
        return result;
    }

    public String getVersion() {
        return version;
    }

    public Driver getDriver() {
        return driver;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public int getPatchVersion() {
        return patchVersion;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
        if (!healthy) {
            this.lastFailureTime = System.currentTimeMillis();
        }
    }

    public long getLastFailureTime() {
        return lastFailureTime;
    }

    /**
     * Resets the health status after a cooldown period.
     *
     * @param cooldownMs the cooldown period in milliseconds
     * @return true if the health was reset, false otherwise
     */
    public boolean resetHealthIfCooledDown(long cooldownMs) {
        if (!healthy && System.currentTimeMillis() - lastFailureTime > cooldownMs) {
            healthy = true;
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(DriverVersion other) {
        // Compare versions: higher version comes first (descending order)
        int cmp = Integer.compare(other.majorVersion, this.majorVersion);
        if (cmp != 0) return cmp;
        cmp = Integer.compare(other.minorVersion, this.minorVersion);
        if (cmp != 0) return cmp;
        return Integer.compare(other.patchVersion, this.patchVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DriverVersion that = (DriverVersion) o;
        return version.equals(that.version);
    }

    @Override
    public int hashCode() {
        return version.hashCode();
    }

    @Override
    public String toString() {
        return "DriverVersion{" +
                "version='" + version + '\'' +
                ", healthy=" + healthy +
                ", majorVersion=" + majorVersion +
                ", minorVersion=" + minorVersion +
                ", patchVersion=" + patchVersion +
                '}';
    }
}
