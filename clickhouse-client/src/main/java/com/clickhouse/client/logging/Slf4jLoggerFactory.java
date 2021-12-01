package com.clickhouse.client.logging;

/**
 * Adaptor of slf4j logger factory.
 */
public class Slf4jLoggerFactory extends LoggerFactory {
    @Override
    public Logger get(Class<?> clazz) {
        return new Slf4jLogger(org.slf4j.LoggerFactory.getLogger(clazz));
    }

    @Override
    public Logger get(String name) {
        return new Slf4jLogger(org.slf4j.LoggerFactory.getLogger(name));
    }
}
