package com.clickhouse.logging;

import java.util.Collections;
import org.testng.annotations.Test;

public class JdkLoggerTest extends LoggerTest {
    private final JdkLoggerFactory factory = new JdkLoggerFactory();

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
        checkInstance(JdkLogger.class);
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
        logWithFunction(() -> Collections.singleton(1));
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
