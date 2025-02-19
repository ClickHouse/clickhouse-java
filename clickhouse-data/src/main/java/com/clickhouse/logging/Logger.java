package com.clickhouse.logging;

import java.util.function.Supplier;

/**
 * Unified logger. Pay attention that the {@code format} follows standard
 * {@link java.util.Formatter}.
 */
@Deprecated
public interface Logger {
    /**
     * Error message for providing null logger.
     */
    static final String ERROR_NULL_LOGGER = "Non-null logger is required";

    /**
     * Checks if logging level is {@code DEBUG} or above.
     *
     * @return true if DEBUG level is enabled; false otherwise
     */
    boolean isDebugEnabled();

    /**
     * Checks if logging level is {@code ERROR} or above.
     *
     * @return true if ERROR level is enabled; false otherwise
     */
    boolean isErrorEnabled();

    /**
     * Checks if logging level is {@code INFO} or above.
     *
     * @return true if INFO level is enabled; false otherwise
     */
    boolean isInfoEnabled();

    /**
     * Checks if logging level is {@code WARN} or above.
     *
     * @return true if WARN level is enabled; false otherwise
     */
    boolean isWarnEnabled();

    /**
     * Checks if logging level is {@code TRACE} or above.
     *
     * @return true if TRACE level is enabled; false otherwise
     */
    boolean isTraceEnabled();

    /**
     * Logs output of a custom function at the DEBUG level. The function will only
     * run when log level is DEBUG or lower.
     *
     * @param function custom function to run
     */
    void debug(Supplier<?> function);

    /**
     * Logs a message at the DEBUG level according to the specified format and
     * arguments.
     *
     * @param format    the format string
     * @param arguments a list of arguments, the last one could be a
     *                  {@link java.lang.Throwable}
     */
    void debug(Object format, Object... arguments);

    /**
     * Logs an error (see {@link java.lang.Throwable}) at the DEBUG level with an
     * accompanying message.
     *
     * @param message the message accompanying the error
     * @param t       the error to log
     */
    void debug(Object message, Throwable t);

    /**
     * Logs output of a custom function at the ERROR level. The function will only
     * run when log level is ERROR or lower.
     *
     * @param function custom function to run
     */
    void error(Supplier<?> function);

    /**
     * Logs a message at the ERROR level according to the specified format and
     * arguments.
     *
     * @param format    the format string
     * @param arguments a list of arguments, the last one could be a
     *                  {@link java.lang.Throwable}
     */
    void error(Object format, Object... arguments);

    /**
     * Logs an error (see {@link java.lang.Throwable}) at the ERROR level with an
     * accompanying message.
     *
     * @param message the message accompanying the error
     * @param t       the error to log
     */
    void error(Object message, Throwable t);

    /**
     * Logs output of a custom function at the INFO level. The function will only
     * run when log level is INFO or lower.
     *
     * @param function custom function to run
     */
    void info(Supplier<?> function);

    /**
     * Logs a message at the INFO level according to the specified format and
     * arguments.
     *
     * @param format    the format string
     * @param arguments a list of arguments, the last one could be a
     *                  {@link java.lang.Throwable}
     */
    void info(Object format, Object... arguments);

    /**
     * Logs an error (see {@link java.lang.Throwable}) at the INFO level with an
     * accompanying message.
     *
     * @param message the message accompanying the error
     * @param t       the error to log
     */
    void info(Object message, Throwable t);

    /**
     * Logs output of a custom function at the TRACE level. The function will only
     * run when log level is TRACE.
     *
     * @param function custom function to run
     */
    void trace(Supplier<?> function);

    /**
     * Logs a message at the TRACE level according to the specified format and
     * arguments.
     *
     * @param format    the format string
     * @param arguments a list of arguments, the last one could be a
     *                  {@link java.lang.Throwable}
     */
    void trace(Object format, Object... arguments);

    /**
     * Logs an error (see {@link java.lang.Throwable}) at the TRACE level with an
     * accompanying message.
     *
     * @param message the message accompanying the error
     * @param t       the error to log
     */
    void trace(Object message, Throwable t);

    /**
     * Logs output of a custom function at the WARN level. The function will only
     * run when log level is WARN or lower.
     *
     * @param function custom function to run
     */
    void warn(Supplier<?> function);

    /**
     * Logs a message at the WARN level according to the specified format and
     * arguments.
     *
     * @param format    the format string
     * @param arguments a list of arguments, the last one could be a
     *                  {@link java.lang.Throwable}
     */
    void warn(Object format, Object... arguments);

    /**
     * Logs an error (see {@link java.lang.Throwable}) at the WRAN level with an
     * accompanying message.
     *
     * @param message the message accompanying the error
     * @param t       the error to log
     */
    void warn(Object message, Throwable t);

    /**
     * Return logger implementation.
     *
     * @return implementation
     */
    Object unwrap();
}
