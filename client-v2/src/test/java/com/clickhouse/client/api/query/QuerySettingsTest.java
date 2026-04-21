package com.clickhouse.client.api.query;

import org.testng.Assert;
import org.testng.annotations.Test;

public class QuerySettingsTest {
    @Test
    public void testClearSession() {
        QuerySettings settings = new QuerySettings();
        settings.setSessionId("test-session");
        settings.setSessionCheck(true);

        Assert.assertEquals(settings.getSessionId(), "test-session");
        Assert.assertEquals(settings.getSessionCheck(), Boolean.TRUE);

        settings.clearSession();

        Assert.assertNull(settings.getSessionId());
        Assert.assertEquals(settings.getSessionCheck(), Boolean.FALSE);
    }
    
    @Test
    public void testCopyConstructor() {
        QuerySettings settings = new QuerySettings();
        settings.setSessionId("test-session");
        settings.setSessionCheck(true);

        QuerySettings copy = new QuerySettings(settings);
        Assert.assertEquals(copy.getSessionId(), "test-session");
        Assert.assertEquals(copy.getSessionCheck(), Boolean.TRUE);
        
        QuerySettings nullCopy = new QuerySettings((QuerySettings) null);
        Assert.assertNull(nullCopy.getSessionId());
    }
}
