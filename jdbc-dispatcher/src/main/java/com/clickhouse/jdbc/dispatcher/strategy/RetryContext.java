package com.clickhouse.jdbc.dispatcher.strategy;

import java.util.HashMap;
import java.util.Map;

/**
 * Context information for retry operations.
 * <p>
 * This class carries metadata about the current operation being performed,
 * which can be used by retry strategies to make informed decisions.
 */
public class RetryContext {

    /**
     * Types of operations that can trigger retries.
     */
    public enum OperationType {
        CONNECT,
        EXECUTE_QUERY,
        EXECUTE_UPDATE,
        PREPARE_STATEMENT,
        CALL_PROCEDURE,
        METADATA,
        TRANSACTION,
        OTHER
    }

    private final OperationType operationType;
    private final String operationName;
    private final int attemptNumber;
    private final long startTimeMs;
    private final Map<String, Object> attributes = new HashMap<>();

    /**
     * Creates a new RetryContext.
     *
     * @param operationType the type of operation
     * @param operationName a descriptive name for the operation
     * @param attemptNumber the current attempt number (1-based)
     */
    public RetryContext(OperationType operationType, String operationName, int attemptNumber) {
        this.operationType = operationType;
        this.operationName = operationName;
        this.attemptNumber = attemptNumber;
        this.startTimeMs = System.currentTimeMillis();
    }

    /**
     * Creates a context for a connection operation.
     *
     * @param attemptNumber the attempt number
     * @return a new RetryContext
     */
    public static RetryContext forConnect(int attemptNumber) {
        return new RetryContext(OperationType.CONNECT, "connect", attemptNumber);
    }

    /**
     * Creates a context for a query execution.
     *
     * @param sql           the SQL being executed
     * @param attemptNumber the attempt number
     * @return a new RetryContext
     */
    public static RetryContext forQuery(String sql, int attemptNumber) {
        RetryContext ctx = new RetryContext(OperationType.EXECUTE_QUERY, "executeQuery", attemptNumber);
        ctx.setAttribute("sql", sql);
        return ctx;
    }

    /**
     * Creates a context for an update execution.
     *
     * @param sql           the SQL being executed
     * @param attemptNumber the attempt number
     * @return a new RetryContext
     */
    public static RetryContext forUpdate(String sql, int attemptNumber) {
        RetryContext ctx = new RetryContext(OperationType.EXECUTE_UPDATE, "executeUpdate", attemptNumber);
        ctx.setAttribute("sql", sql);
        return ctx;
    }

    /**
     * Creates a new context for the next retry attempt.
     *
     * @return a new RetryContext with incremented attempt number
     */
    public RetryContext nextAttempt() {
        RetryContext next = new RetryContext(operationType, operationName, attemptNumber + 1);
        next.attributes.putAll(this.attributes);
        return next;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public String getOperationName() {
        return operationName;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getElapsedTimeMs() {
        return System.currentTimeMillis() - startTimeMs;
    }

    /**
     * Sets an attribute on this context.
     *
     * @param key   the attribute key
     * @param value the attribute value
     */
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    /**
     * Gets an attribute from this context.
     *
     * @param key the attribute key
     * @return the attribute value, or null if not set
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key) {
        return (T) attributes.get(key);
    }

    /**
     * Checks if this is the first attempt.
     *
     * @return true if this is attempt number 1
     */
    public boolean isFirstAttempt() {
        return attemptNumber == 1;
    }

    @Override
    public String toString() {
        return "RetryContext{" +
                "operationType=" + operationType +
                ", operationName='" + operationName + '\'' +
                ", attemptNumber=" + attemptNumber +
                ", elapsedMs=" + getElapsedTimeMs() +
                '}';
    }
}
