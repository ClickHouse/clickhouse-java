package com.clickhouse.client.api.internal;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CommonSettingsTest {
    @Test(groups = {"integration"})
    public void testClearSession() {
        CommonSettings settings = new CommonSettings();
        settings.setSessionId("test-session");
        settings.setSessionCheck(true);

        Assert.assertEquals(settings.getSessionId(), "test-session");
        Assert.assertEquals(settings.getSessionCheck(), Boolean.TRUE);

        settings.clearSession();

        Assert.assertNull(settings.getSessionId());
        Assert.assertEquals(settings.getSessionCheck(), Boolean.FALSE);
    }
}
