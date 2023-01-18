package com.clickhouse.logging;

import java.util.Collections;
import org.testng.annotations.Test;

public class Slf4jLoggerTest extends LoggerTest {
    private final Slf4jLoggerFactory factory = new Slf4jLoggerFactory();

    @Override
    protected Logger getLogger(Class<?> clazz) {
        return factory.get(clazz);
    }

    @Override
    protected Logger getLogger(String name) {
        return factory.get(name);
    }

    @Test(groups = { "unit" })
    public void testInstantiation() {
        checkInstance(Slf4jLogger.class);
    }

    @Test(groups = { "unit" })
    public void testLogMessage() {
        logMessage(Collections.singletonMap("key", "value"));
    }

    @Test(groups = { "unit" })
    public void testLogWithFormat() {
        logWithFormat("msg %s %s %s %s", 1, 2.2, "3", new Object());
    }

    @Test(groups = { "unit" })
    public void testLogWithFunction() {
        logWithFunction(() -> Collections.singleton(2L));
    }

    @Test(groups = { "unit" })
    public void testLogThrowable() {
        logThrowable("msg", new Exception("test exception"));
    }

    @Test(groups = { "unit" })
    public void testLogWithFormatAndThrowable() {
        logWithFormatAndThrowable("msg %s %s %s %s", 1, 2.2, "3", new Object(), new Exception("test exception"));
    }
}
