package com.clickhouse.logging;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LogMessageTest {
    @Test(groups = { "unit" })
    public void testMessageWithNoArgument() {
        String message = "test %s";
        LogMessage msg = LogMessage.of(message);
        Assert.assertEquals(message, msg.getMessage());
        Assert.assertNull(msg.getThrowable());

        msg = LogMessage.of(1);
        Assert.assertEquals("1", msg.getMessage());
        Assert.assertNull(msg.getThrowable());
    }

    @Test(groups = { "unit" })
    public void testMessageWithArguments() {
        LogMessage msg = LogMessage.of("test %s - %s", "test", 1);
        Assert.assertEquals("test test - 1", msg.getMessage());
        Assert.assertNull(msg.getThrowable());

        msg = LogMessage.of("test", "test", 1);
        Assert.assertEquals("test", msg.getMessage());
        Assert.assertNull(msg.getThrowable());
    }

    @Test(groups = { "unit" })
    public void testMessageWithThrowable() {
        Throwable t = new Exception();
        LogMessage msg = LogMessage.of("test", t);
        Assert.assertEquals("test", msg.getMessage());
        Assert.assertEquals(t, msg.getThrowable());

        msg = LogMessage.of("test %s", 1, t);
        Assert.assertEquals("test 1", msg.getMessage());
        Assert.assertEquals(t, msg.getThrowable());

        msg = LogMessage.of("test %d %s", 1, t);
        Assert.assertEquals("test 1 java.lang.Exception", msg.getMessage());
        Assert.assertEquals(t, msg.getThrowable());

        msg = LogMessage.of("test %d %s", 1, t, null);
        Assert.assertEquals("test 1 java.lang.Exception", msg.getMessage());
        Assert.assertEquals(msg.getThrowable(), null);
    }
}
