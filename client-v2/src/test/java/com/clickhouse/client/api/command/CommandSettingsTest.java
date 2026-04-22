package com.clickhouse.client.api.command;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CommandSettingsTest {
    @Test
    public void testClearSession() {
        CommandSettings settings = new CommandSettings();
        settings.setSessionId("test-session");
        settings.setSessionCheck(true);

        Assert.assertEquals(settings.getSessionId(), "test-session");
        Assert.assertEquals(settings.getSessionCheck(), Boolean.TRUE);

        settings.clearSession();

        Assert.assertNull(settings.getSessionId());
        Assert.assertEquals(settings.getSessionCheck(), Boolean.FALSE);
    }
}
