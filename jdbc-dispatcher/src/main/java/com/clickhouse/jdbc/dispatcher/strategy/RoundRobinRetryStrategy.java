package com.clickhouse.jdbc.dispatcher.strategy;

import com.clickhouse.jdbc.dispatcher.DriverVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Retry strategy that rotates through driver versions in round-robin fashion.
 * <p>
 * This strategy distributes load across all driver versions by starting with
 * a different version for each new operation. Healthy versions are preferred
 * over unhealthy ones.
 */
public class RoundRobinRetryStrategy implements RetryStrategy {

    private static final Logger log = LoggerFactory.getLogger(RoundRobinRetryStrategy.class);

    private final int maxRetries;
    private final AtomicInteger nextIndex = new AtomicInteger(0);

    /**
     * Creates a RoundRobinRetryStrategy with default settings.
     */
    public RoundRobinRetryStrategy() {
        this(3);
    }

    /**
     * Creates a RoundRobinRetryStrategy with custom max retries.
     *
     * @param maxRetries maximum number of retries
     */
    public RoundRobinRetryStrategy(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    @Override
    public List<DriverVersion> getVersionsToTry(List<DriverVersion> availableVersions, RetryContext context) {
        if (availableVersions == null || availableVersions.isEmpty()) {
            return Collections.emptyList();
        }

        int size = availableVersions.size();
        int startIdx = nextIndex.getAndUpdate(i -> (i + 1) % size);

        // Build ordered list starting from startIdx
        List<DriverVersion> result = new ArrayList<>(size);
        List<DriverVersion> healthy = new ArrayList<>();
        List<DriverVersion> unhealthy = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            DriverVersion v = availableVersions.get((startIdx + i) % size);
            if (v.isHealthy()) {
                healthy.add(v);
            } else {
                unhealthy.add(v);
            }
        }

        // Healthy first, then unhealthy
        result.addAll(healthy);
        result.addAll(unhealthy);

        // Limit to max retries
        if (result.size() > maxRetries) {
            return new ArrayList<>(result.subList(0, maxRetries));
        }
        return result;
    }

    @Override
    public void onSuccess(DriverVersion version, RetryContext context) {
        log.debug("Operation {} succeeded with driver version {} on attempt {} (round-robin)",
                context.getOperationName(), version.getVersion(), context.getAttemptNumber());
    }

    @Override
    public void onFailure(DriverVersion version, RetryContext context, Throwable exception) {
        log.warn("Operation {} failed with driver version {} on attempt {} (round-robin): {}",
                context.getOperationName(), version.getVersion(), context.getAttemptNumber(),
                exception.getMessage());
        version.setHealthy(false);
    }

    @Override
    public int getMaxRetries() {
        return maxRetries;
    }

    @Override
    public String getName() {
        return "RoundRobin";
    }

    @Override
    public String toString() {
        return "RoundRobinRetryStrategy{maxRetries=" + maxRetries + '}';
    }
}
