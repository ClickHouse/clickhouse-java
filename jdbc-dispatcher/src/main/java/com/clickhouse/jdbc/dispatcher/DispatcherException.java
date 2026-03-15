package com.clickhouse.jdbc.dispatcher;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exception thrown when all driver versions have been exhausted during retry attempts.
 * <p>
 * This exception aggregates all the failures that occurred during the retry process,
 * allowing callers to understand what went wrong with each driver version.
 */
public class DispatcherException extends SQLException {

    private final List<VersionFailure> failures;

    /**
     * Creates a new DispatcherException with the given message and list of failures.
     *
     * @param message  the exception message
     * @param failures the list of version-specific failures
     */
    public DispatcherException(String message, List<VersionFailure> failures) {
        super(message);
        this.failures = new ArrayList<>(failures);
        
        // Set the cause to the first failure's exception if available
        if (!failures.isEmpty()) {
            initCause(failures.get(0).getException());
        }
    }

    /**
     * Creates a new DispatcherException with a single cause.
     *
     * @param message the exception message
     * @param cause   the underlying cause
     */
    public DispatcherException(String message, Throwable cause) {
        super(message, cause);
        this.failures = Collections.emptyList();
    }

    /**
     * Returns the list of failures that occurred during retry attempts.
     *
     * @return an unmodifiable list of version failures
     */
    public List<VersionFailure> getFailures() {
        return Collections.unmodifiableList(failures);
    }

    /**
     * Represents a failure that occurred with a specific driver version.
     */
    public static class VersionFailure {
        private final String version;
        private final Throwable exception;
        private final long timestampMs;

        public VersionFailure(String version, Throwable exception) {
            this.version = version;
            this.exception = exception;
            this.timestampMs = System.currentTimeMillis();
        }

        public String getVersion() {
            return version;
        }

        public Throwable getException() {
            return exception;
        }

        public long getTimestampMs() {
            return timestampMs;
        }

        @Override
        public String toString() {
            return "VersionFailure{version='" + version + "', error=" + exception.getMessage() + '}';
        }
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder(super.getMessage());
        if (!failures.isEmpty()) {
            sb.append(" [Failures: ");
            for (int i = 0; i < failures.size(); i++) {
                if (i > 0) sb.append(", ");
                VersionFailure f = failures.get(i);
                sb.append("v").append(f.getVersion()).append(": ").append(f.getException().getMessage());
            }
            sb.append("]");
        }
        return sb.toString();
    }
}
