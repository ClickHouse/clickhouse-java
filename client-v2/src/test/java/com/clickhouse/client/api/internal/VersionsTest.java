package com.clickhouse.client.api.internal;

import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.util.VersionInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

public class VersionsTest {

    @Test
    public void testVersionsGenerated() {
        Assert.assertTrue(Versions.CLIENT.matches("\\d+\\.\\d+\\.\\d+.*"), Versions.CLIENT);

        VersionInfo versionInfo = VersionInfo.loadVersionInfo("org.apache.hc.client5",
                HttpClients.class.getClassLoader());
        Assert.assertNotNull(versionInfo);
        Assert.assertEquals(Versions.APACHE_HTTP_CLIENT, versionInfo.getRelease());
    }
}
