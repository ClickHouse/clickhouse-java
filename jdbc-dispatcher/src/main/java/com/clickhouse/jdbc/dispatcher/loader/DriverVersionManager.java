package com.clickhouse.jdbc.dispatcher.loader;

import com.clickhouse.jdbc.dispatcher.DriverVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Driver;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Manages loading and organizing multiple JDBC driver versions.
 * <p>
 * This class is responsible for:
 * - Loading driver JARs from specified locations
 * - Extracting version information from JAR filenames
 * - Managing the lifecycle of loaded drivers
 * - Providing ordered access to driver versions
 */
public class DriverVersionManager {

    private static final Logger log = LoggerFactory.getLogger(DriverVersionManager.class);

    // Pattern to extract version from JAR filename (e.g., "driver-1.2.3.jar", "driver-1.2.3-SNAPSHOT.jar")
    private static final Pattern VERSION_PATTERN = Pattern.compile(
            ".*?[-_]?(\\d+\\.\\d+(?:\\.\\d+)?(?:[-._]\\w+)?)\\.jar$",
            Pattern.CASE_INSENSITIVE
    );

    private final String driverClassName;
    private final List<DriverVersion> versions = new CopyOnWriteArrayList<>();
    private final Map<String, DriverVersion> versionMap = new ConcurrentHashMap<>();
    private final long healthCheckCooldownMs;

    /**
     * Creates a new DriverVersionManager.
     *
     * @param driverClassName       the fully qualified class name of the driver
     * @param healthCheckCooldownMs cooldown period before retrying an unhealthy driver
     */
    public DriverVersionManager(String driverClassName, long healthCheckCooldownMs) {
        this.driverClassName = Objects.requireNonNull(driverClassName, "driverClassName cannot be null");
        this.healthCheckCooldownMs = healthCheckCooldownMs;
    }

    /**
     * Creates a new DriverVersionManager with default cooldown.
     *
     * @param driverClassName the fully qualified class name of the driver
     */
    public DriverVersionManager(String driverClassName) {
        this(driverClassName, 60000L); // 1 minute default cooldown
    }

    /**
     * Loads all driver JARs from the specified directory.
     *
     * @param directory the directory containing driver JARs
     * @return the number of drivers successfully loaded
     */
    public int loadFromDirectory(File directory) {
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("Path is not a directory: " + directory);
        }

        File[] jarFiles = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".jar"));
        if (jarFiles == null || jarFiles.length == 0) {
            log.warn("No JAR files found in directory: {}", directory);
            return 0;
        }

        int loaded = 0;
        for (File jarFile : jarFiles) {
            try {
                String version = extractVersionFromFilename(jarFile.getName());
                if (version != null) {
                    loadDriver(jarFile.toURI().toURL(), version);
                    loaded++;
                } else {
                    log.warn("Could not extract version from JAR filename: {}", jarFile.getName());
                }
            } catch (Exception e) {
                log.error("Failed to load driver from JAR: {}", jarFile, e);
            }
        }

        sortVersions();
        return loaded;
    }

    /**
     * Loads a driver from a specific JAR URL with the given version.
     *
     * @param jarUrl  the URL of the driver JAR
     * @param version the version string for this driver
     * @return the loaded DriverVersion
     * @throws Exception if loading fails
     */
    public DriverVersion loadDriver(URL jarUrl, String version) throws Exception {
        if (versionMap.containsKey(version)) {
            log.warn("Driver version {} already loaded, skipping", version);
            return versionMap.get(version);
        }

        log.info("Loading driver version {} from {}", version, jarUrl);

        IsolatedClassLoader classLoader = new IsolatedClassLoader(
                jarUrl,
                getClass().getClassLoader(),
                driverClassName,
                version
        );

        Driver driver = classLoader.loadDriver();
        DriverVersion driverVersion = new DriverVersion(version, driver, classLoader);

        versions.add(driverVersion);
        versionMap.put(version, driverVersion);
        sortVersions();

        log.info("Successfully loaded driver version {}: major={}, minor={}",
                version, driverVersion.getMajorVersion(), driverVersion.getMinorVersion());

        return driverVersion;
    }

    /**
     * Loads a driver from a JAR file.
     *
     * @param jarFile the driver JAR file
     * @param version the version string
     * @return the loaded DriverVersion
     * @throws Exception if loading fails
     */
    public DriverVersion loadDriver(File jarFile, String version) throws Exception {
        return loadDriver(jarFile.toURI().toURL(), version);
    }

    /**
     * Extracts version information from a JAR filename.
     *
     * @param filename the JAR filename
     * @return the extracted version string, or null if not found
     */
    public static String extractVersionFromFilename(String filename) {
        Matcher matcher = VERSION_PATTERN.matcher(filename);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * Gets all loaded driver versions, sorted by version (newest first).
     *
     * @return an unmodifiable list of driver versions
     */
    public List<DriverVersion> getVersions() {
        return Collections.unmodifiableList(versions);
    }

    /**
     * Gets all healthy driver versions, sorted by version (newest first).
     *
     * @return a list of healthy driver versions
     */
    public List<DriverVersion> getHealthyVersions() {
        // First, try to reset health for cooled-down drivers
        for (DriverVersion version : versions) {
            version.resetHealthIfCooledDown(healthCheckCooldownMs);
        }

        List<DriverVersion> healthy = new ArrayList<>();
        for (DriverVersion version : versions) {
            if (version.isHealthy()) {
                healthy.add(version);
            }
        }
        return healthy;
    }

    /**
     * Gets a specific driver version by version string.
     *
     * @param version the version string
     * @return the DriverVersion, or null if not found
     */
    public DriverVersion getVersion(String version) {
        return versionMap.get(version);
    }

    /**
     * Gets the newest (highest version) driver.
     *
     * @return the newest DriverVersion, or null if no versions are loaded
     */
    public DriverVersion getNewestVersion() {
        return versions.isEmpty() ? null : versions.get(0);
    }

    /**
     * Gets the newest healthy driver.
     *
     * @return the newest healthy DriverVersion, or null if none available
     */
    public DriverVersion getNewestHealthyVersion() {
        List<DriverVersion> healthy = getHealthyVersions();
        return healthy.isEmpty() ? null : healthy.get(0);
    }

    /**
     * Marks a driver version as unhealthy.
     *
     * @param version the version string
     */
    public void markUnhealthy(String version) {
        DriverVersion dv = versionMap.get(version);
        if (dv != null) {
            dv.setHealthy(false);
            log.warn("Marked driver version {} as unhealthy", version);
        }
    }

    /**
     * Marks a driver version as healthy.
     *
     * @param version the version string
     */
    public void markHealthy(String version) {
        DriverVersion dv = versionMap.get(version);
        if (dv != null) {
            dv.setHealthy(true);
            log.info("Marked driver version {} as healthy", version);
        }
    }

    /**
     * Returns the number of loaded driver versions.
     *
     * @return the count of loaded drivers
     */
    public int size() {
        return versions.size();
    }

    /**
     * Checks if any drivers are loaded.
     *
     * @return true if at least one driver is loaded
     */
    public boolean isEmpty() {
        return versions.isEmpty();
    }

    /**
     * Clears all loaded drivers.
     */
    public void clear() {
        versions.clear();
        versionMap.clear();
    }

    /**
     * Sorts versions so that the newest version comes first.
     */
    private void sortVersions() {
        Collections.sort(versions);
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public long getHealthCheckCooldownMs() {
        return healthCheckCooldownMs;
    }
}
