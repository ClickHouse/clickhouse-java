package com.clickhouse.client.api.insert;

import org.testng.Assert;
import org.testng.annotations.Test;

public class InsertSettingsTest {
    @Test
    public void testClearSession() {
        InsertSettings settings = new InsertSettings();
        settings.setSessionId("test-session");
        settings.setSessionCheck(true);

        Assert.assertEquals(settings.getSessionId(), "test-session");
        Assert.assertEquals(settings.getSessionCheck(), Boolean.TRUE);

        settings.clearSession();

        Assert.assertNull(settings.getSessionId());
        Assert.assertEquals(settings.getSessionCheck(), Boolean.FALSE);
    }
}
