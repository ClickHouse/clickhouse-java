package com.clickhouse.jdbc.dispatcher.strategy;

import com.clickhouse.jdbc.dispatcher.DriverVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Retry strategy that tries the newest driver version first.
 * <p>
 * This strategy orders driver versions from newest to oldest, preferring
 * healthy drivers over unhealthy ones. If a driver fails, it moves to
 * the next newest version.
 */
public class NewestFirstRetryStrategy implements RetryStrategy {

    private static final Logger log = LoggerFactory.getLogger(NewestFirstRetryStrategy.class);

    private final int maxRetries;
    private final boolean skipUnhealthy;

    /**
     * Creates a NewestFirstRetryStrategy with default settings.
     */
    public NewestFirstRetryStrategy() {
        this(3, true);
    }

    /**
     * Creates a NewestFirstRetryStrategy with custom settings.
     *
     * @param maxRetries    maximum number of retries
     * @param skipUnhealthy whether to skip unhealthy drivers on first pass
     */
    public NewestFirstRetryStrategy(int maxRetries, boolean skipUnhealthy) {
        this.maxRetries = maxRetries;
        this.skipUnhealthy = skipUnhealthy;
    }

    @Override
    public List<DriverVersion> getVersionsToTry(List<DriverVersion> availableVersions, RetryContext context) {
        if (availableVersions == null || availableVersions.isEmpty()) {
            return Collections.emptyList();
        }

        // Make a copy and sort by version (newest first)
        List<DriverVersion> sorted = new ArrayList<>(availableVersions);
        Collections.sort(sorted);

        if (!skipUnhealthy) {
            // Return all versions in order
            return limitToMaxRetries(sorted);
        }

        // Separate healthy and unhealthy versions
        List<DriverVersion> healthy = new ArrayList<>();
        List<DriverVersion> unhealthy = new ArrayList<>();

        for (DriverVersion version : sorted) {
            if (version.isHealthy()) {
                healthy.add(version);
            } else {
                unhealthy.add(version);
            }
        }

        // Try healthy versions first, then unhealthy as fallback
        List<DriverVersion> result = new ArrayList<>();
        result.addAll(healthy);
        result.addAll(unhealthy);

        return limitToMaxRetries(result);
    }

    /**
     * Limits the list to the maximum number of retries.
     */
    private List<DriverVersion> limitToMaxRetries(List<DriverVersion> versions) {
        if (versions.size() <= maxRetries) {
            return versions;
        }
        return new ArrayList<>(versions.subList(0, maxRetries));
    }

    @Override
    public void onSuccess(DriverVersion version, RetryContext context) {
        log.debug("Operation {} succeeded with driver version {} on attempt {}",
                context.getOperationName(), version.getVersion(), context.getAttemptNumber());
    }

    @Override
    public void onFailure(DriverVersion version, RetryContext context, Throwable exception) {
        log.warn("Operation {} failed with driver version {} on attempt {}: {}",
                context.getOperationName(), version.getVersion(), context.getAttemptNumber(),
                exception.getMessage());

        // Mark the version as unhealthy after failure
        version.setHealthy(false);
    }

    @Override
    public int getMaxRetries() {
        return maxRetries;
    }

    @Override
    public String getName() {
        return "NewestFirst";
    }

    @Override
    public String toString() {
        return "NewestFirstRetryStrategy{" +
                "maxRetries=" + maxRetries +
                ", skipUnhealthy=" + skipUnhealthy +
                '}';
    }
}
