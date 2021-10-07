package com.clickhouse.client.logging;

/**
 * Adaptor of JDK logger factory.
 */
public class JdkLoggerFactory extends LoggerFactory {
    @Override
    public Logger get(String name) {
        return new JdkLogger(java.util.logging.Logger.getLogger(name));
    }
}
