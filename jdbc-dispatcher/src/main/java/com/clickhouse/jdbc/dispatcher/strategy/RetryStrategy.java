package com.clickhouse.jdbc.dispatcher.strategy;

import com.clickhouse.jdbc.dispatcher.DriverVersion;

import java.util.List;

/**
 * Strategy interface for selecting which driver version to try next during failover.
 * <p>
 * Implementations of this interface define the order in which driver versions
 * are attempted when establishing connections or executing operations.
 */
public interface RetryStrategy {

    /**
     * Returns an ordered list of driver versions to try for the given operation.
     * The first element should be tried first, then the second on failure, etc.
     *
     * @param availableVersions all available driver versions (may include unhealthy ones)
     * @param context           optional context about the current operation
     * @return an ordered list of versions to attempt
     */
    List<DriverVersion> getVersionsToTry(List<DriverVersion> availableVersions, RetryContext context);

    /**
     * Called when an operation succeeds with a specific version.
     * Allows the strategy to track success patterns.
     *
     * @param version the version that succeeded
     * @param context the context of the successful operation
     */
    default void onSuccess(DriverVersion version, RetryContext context) {
        // Default implementation does nothing
    }

    /**
     * Called when an operation fails with a specific version.
     * Allows the strategy to track failure patterns and adjust behavior.
     *
     * @param version   the version that failed
     * @param context   the context of the failed operation
     * @param exception the exception that caused the failure
     */
    default void onFailure(DriverVersion version, RetryContext context, Throwable exception) {
        // Default implementation does nothing
    }

    /**
     * Returns the maximum number of retry attempts allowed.
     *
     * @return the maximum retry count
     */
    int getMaxRetries();

    /**
     * Returns the name of this retry strategy for logging and monitoring.
     *
     * @return the strategy name
     */
    String getName();
}
