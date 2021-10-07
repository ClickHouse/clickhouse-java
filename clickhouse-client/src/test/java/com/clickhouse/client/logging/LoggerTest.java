package com.clickhouse.client.logging;

import org.testng.Assert;

public abstract class LoggerTest {
    protected Logger getLogger(Class<?> clazz) {
        return getLogger(clazz.getName());
    }

    protected Logger getLogger(String name) {
        return null;
    }

    protected void checkInstance(Class<?> clazz) {
        Logger logger1 = getLogger(LoggerTest.class);
        Logger logger2 = getLogger(LoggerTest.class.getName());

        Assert.assertTrue(clazz.isInstance(logger1));
        Assert.assertTrue(clazz.isInstance(logger2));

        Assert.assertEquals(logger1.unwrap(), logger2.unwrap());
    }

    protected void logMessage(Object message) {
        Logger logger = getLogger(LoggerTest.class);

        logger.trace(null);
        logger.trace(message);
        logger.debug(null);
        logger.debug(message);
        logger.info(null);
        logger.info(message);
        logger.warn(null);
        logger.warn(message);
        logger.error(null);
        logger.error(message);
    }

    protected void logWithFormat(Object message, Object... arguments) {
        Logger logger = getLogger(LoggerTest.class);

        Assert.assertTrue(arguments.length > 0);
        Assert.assertFalse(arguments[arguments.length - 1] instanceof Throwable);

        logger.trace(null, arguments);
        logger.trace(message, arguments);
        logger.debug(null, arguments);
        logger.debug(message, arguments);
        logger.info(null, arguments);
        logger.info(message, arguments);
        logger.warn(null, arguments);
        logger.warn(message, arguments);
        logger.error(null, arguments);
        logger.error(message, arguments);
    }

    protected void logThrowable(Object message, Throwable t) {
        Logger logger = getLogger(Slf4jLoggerTest.class);

        logger.trace(null, t);
        logger.trace(message, t);
        logger.debug(null, t);
        logger.debug(message, t);
        logger.info(null, t);
        logger.info(message, t);
        logger.warn(null, t);
        logger.warn(message, t);
        logger.error(null, t);
        logger.error(message, t);
    }

    protected void logWithFormatAndThrowable(Object message, Object... arguments) {
        Logger logger = getLogger(LoggerTest.class);

        Assert.assertTrue(arguments.length > 0);
        Assert.assertTrue(arguments[arguments.length - 1] instanceof Throwable);

        logger.trace(null, arguments);
        logger.trace(message, arguments);
        logger.debug(null, arguments);
        logger.debug(message, arguments);
        logger.info(null, arguments);
        logger.info(message, arguments);
        logger.warn(null, arguments);
        logger.warn(message, arguments);
        logger.error(null, arguments);
        logger.error(message, arguments);
    }
}
