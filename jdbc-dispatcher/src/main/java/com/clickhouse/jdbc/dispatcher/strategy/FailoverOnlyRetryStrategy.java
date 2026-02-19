package com.clickhouse.jdbc.dispatcher.strategy;

import com.clickhouse.jdbc.dispatcher.DriverVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Retry strategy that uses a single preferred version until it fails, then fails over.
 * <p>
 * This strategy is useful when you want to stick to one version (typically the newest)
 * and only switch to an alternative when the primary fails. Once a failover occurs,
 * the strategy continues using the new version until it also fails.
 */
public class FailoverOnlyRetryStrategy implements RetryStrategy {

    private static final Logger log = LoggerFactory.getLogger(FailoverOnlyRetryStrategy.class);

    private final int maxRetries;
    private volatile DriverVersion preferredVersion;

    /**
     * Creates a FailoverOnlyRetryStrategy with default settings.
     */
    public FailoverOnlyRetryStrategy() {
        this(3);
    }

    /**
     * Creates a FailoverOnlyRetryStrategy with custom max retries.
     *
     * @param maxRetries maximum number of retries
     */
    public FailoverOnlyRetryStrategy(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    /**
     * Sets the preferred driver version to use.
     *
     * @param version the preferred version
     */
    public void setPreferredVersion(DriverVersion version) {
        this.preferredVersion = version;
    }

    /**
     * Gets the current preferred driver version.
     *
     * @return the preferred version, or null if not set
     */
    public DriverVersion getPreferredVersion() {
        return preferredVersion;
    }

    @Override
    public List<DriverVersion> getVersionsToTry(List<DriverVersion> availableVersions, RetryContext context) {
        if (availableVersions == null || availableVersions.isEmpty()) {
            return Collections.emptyList();
        }

        List<DriverVersion> result = new ArrayList<>();

        // If we have a healthy preferred version, try it first
        if (preferredVersion != null && preferredVersion.isHealthy()) {
            result.add(preferredVersion);
        }

        // Add other healthy versions as fallbacks (sorted by version, newest first)
        List<DriverVersion> sorted = new ArrayList<>(availableVersions);
        Collections.sort(sorted);

        for (DriverVersion v : sorted) {
            if (v.isHealthy() && !result.contains(v)) {
                result.add(v);
            }
        }

        // Add unhealthy versions as last resort
        for (DriverVersion v : sorted) {
            if (!v.isHealthy() && !result.contains(v)) {
                result.add(v);
            }
        }

        // Limit to max retries
        if (result.size() > maxRetries) {
            return new ArrayList<>(result.subList(0, maxRetries));
        }
        return result;
    }

    @Override
    public void onSuccess(DriverVersion version, RetryContext context) {
        // Update preferred version on success
        if (preferredVersion == null || !preferredVersion.equals(version)) {
            log.info("Setting preferred driver version to {} after successful operation",
                    version.getVersion());
            preferredVersion = version;
        }
    }

    @Override
    public void onFailure(DriverVersion version, RetryContext context, Throwable exception) {
        log.warn("Operation {} failed with driver version {}: {}",
                context.getOperationName(), version.getVersion(), exception.getMessage());
        version.setHealthy(false);

        // If this was our preferred version, we'll pick a new one on the next success
        if (preferredVersion != null && preferredVersion.equals(version)) {
            log.info("Preferred version {} marked unhealthy, will select new preferred on next success",
                    version.getVersion());
        }
    }

    @Override
    public int getMaxRetries() {
        return maxRetries;
    }

    @Override
    public String getName() {
        return "FailoverOnly";
    }

    @Override
    public String toString() {
        return "FailoverOnlyRetryStrategy{" +
                "maxRetries=" + maxRetries +
                ", preferredVersion=" + (preferredVersion != null ? preferredVersion.getVersion() : "none") +
                '}';
    }
}
