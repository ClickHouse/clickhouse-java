package com.clickhouse.logging;

import java.util.Locale;

/**
 * Log message with arguments and/or error.
 */
@Deprecated
public final class LogMessage {
    /**
     * Creates a log message with arguments. The latest argument could be a
     * {@code java.lang.Throwable} providing details like stack trace of an error.
     *
     * @param format    Object format, could be null
     * @param arguments arguments, could be null or empty
     * @return log message
     */
    public static LogMessage of(Object format, Object... arguments) {
        String message = String.valueOf(format);
        Throwable t = null;

        int len = arguments != null ? arguments.length : 0;
        if (len > 0) {
            Object lastArg = arguments[len - 1];
            if (lastArg instanceof Throwable) {
                t = (Throwable) lastArg;
            }

            message = String.format(Locale.ROOT, message, arguments);
        }

        return new LogMessage(message, t);
    }

    private final String message;
    private final Throwable throwable;

    /**
     * Default constructor.
     *
     * @param message non-null message
     * @param t       throwable
     */
    private LogMessage(String message, Throwable t) {
        this.message = message;
        this.throwable = t;
    }

    /**
     * Gets log message.
     *
     * @return non-null log message
     */
    public String getMessage() {
        return this.message;
    }

    /**
     * Gets error which may or may not be null.
     *
     * @return error, could be null
     */
    public Throwable getThrowable() {
        return this.throwable;
    }

    /**
     * Checks if error is available or not.
     *
     * @return true if there's error; false otherwise
     */
    public boolean hasThrowable() {
        return this.throwable != null;
    }
}
