package com.clickhouse.logging;

/**
 * Adaptor of JDK logger factory.
 */
@Deprecated
public class JdkLoggerFactory extends LoggerFactory {
    @Override
    public Logger get(String name) {
        return new JdkLogger(java.util.logging.Logger.getLogger(name));
    }
}
