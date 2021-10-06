package com.clickhouse.client.logging;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseUtils;

/**
 * Unified factory class to get logger.
 */
@SuppressWarnings("squid:S1181")
public abstract class LoggerFactory {
    private static final LoggerFactory instance;

    static {
        instance = ClickHouseUtils.getService(LoggerFactory.class, () -> {
            LoggerFactory factory = null;

            try {
                if (org.slf4j.LoggerFactory.getILoggerFactory() != null) {
                    factory = new Slf4jLoggerFactory();
                }
            } catch (Throwable ignore) { // fall back to JDK
                factory = new JdkLoggerFactory();
            }

            return ClickHouseChecker.nonNull(factory, "factory");
        });
    }

    /**
     * Gets logger for the given class. Same as {@code getInstance().get(clazz)}.
     *
     * @param clazz class
     * @return logger for the given class
     */
    public static Logger getLogger(Class<?> clazz) {
        return instance.get(clazz);
    }

    /**
     * Gets logger for the given name. Same as {@code getInstance().get(name)}.
     *
     * @param name name
     * @return logger for the given name
     */
    public static Logger getLogger(String name) {
        return instance.get(name);
    }

    /**
     * Gets instance of the factory for creating logger.
     *
     * @return factory for creating logger
     */
    public static LoggerFactory getInstance() {
        return instance;
    }

    /**
     * Gets logger for the given class.
     *
     * @param clazz class
     * @return logger for the given class
     */
    public Logger get(Class<?> clazz) {
        return get(ClickHouseChecker.nonNull(clazz, "Class").getName());
    }

    /**
     * Gets logger for the given name.
     *
     * @param name name
     * @return logger for the given name
     */
    public abstract Logger get(String name);
}
