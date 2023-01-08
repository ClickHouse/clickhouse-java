package com.clickhouse.client;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.config.ClickHouseDefaults;

public class ClickHouseConfigTest {
    @Test(groups = { "unit" })
    public void testDefaultValues() {
        ClickHouseConfig config = new ClickHouseConfig(null, null, null, null, null);
        Assert.assertEquals(config.getClientName(),
                ClickHouseClientOption.CLIENT_NAME.getEffectiveDefaultValue());
        Assert.assertEquals(config.getDatabase(), ClickHouseDefaults.DATABASE.getEffectiveDefaultValue());

        Assert.assertEquals(config.getOption(ClickHouseDefaults.HOST),
                ClickHouseDefaults.HOST.getEffectiveDefaultValue());
        ClickHouseCredentials credentials = config.getDefaultCredentials();
        Assert.assertEquals(credentials.useAccessToken(), false);
        Assert.assertEquals(credentials.getUserName(), ClickHouseDefaults.USER.getEffectiveDefaultValue());
        Assert.assertEquals(credentials.getPassword(), ClickHouseDefaults.PASSWORD.getEffectiveDefaultValue());
        Assert.assertEquals(config.getFormat(), ClickHouseDefaults.FORMAT.getEffectiveDefaultValue());
        Assert.assertFalse(config.getMetricRegistry().isPresent());
    }

    @Test(groups = { "unit" })
    public void testCustomValues() {
        String clientName = "test client";
        String cluster = "test cluster";
        String database = "test_database";
        String host = "test.host";
        Integer port = 12345;
        Integer weight = -99;
        String user = "sa";
        String password = "welcome";
        Map<String, String> settings = new HashMap<>();
        settings.put("session_check", "1");
        settings.put("max_execution_time", "300");

        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        options.put(ClickHouseClientOption.CLIENT_NAME, clientName);
        options.put(ClickHouseClientOption.DATABASE, database);
        options.put(ClickHouseDefaults.HOST, host);
        options.put(ClickHouseDefaults.USER, "useless");
        options.put(ClickHouseDefaults.PASSWORD, "useless");
        options.put(ClickHouseClientOption.CUSTOM_SETTINGS, "session_check = 1, max_execution_time = 300");

        Object metricRegistry = new Object();

        ClickHouseConfig config = new ClickHouseConfig(options,
                ClickHouseCredentials.fromUserAndPassword(user, password), null, metricRegistry);
        Assert.assertEquals(config.getClientName(), clientName);
        Assert.assertEquals(config.getDatabase(), database);
        Assert.assertEquals(config.getOption(ClickHouseDefaults.HOST), host);
        Assert.assertEquals(config.getCustomSettings(), settings);

        ClickHouseCredentials credentials = config.getDefaultCredentials();
        Assert.assertEquals(credentials.useAccessToken(), false);
        Assert.assertEquals(credentials.getUserName(), user);
        Assert.assertEquals(credentials.getPassword(), password);
        Assert.assertEquals(config.getPreferredProtocols().size(), 0);
        Assert.assertEquals(config.getPreferredTags().size(), 0);
        Assert.assertEquals(config.getMetricRegistry().get(), metricRegistry);
    }

    @Test(groups = { "unit" })
    public void testClientInfo() throws UnknownHostException {
        ClickHouseConfig config = new ClickHouseConfig();
        Assert.assertEquals(config.getProductVersion(), "unknown");
        Assert.assertEquals(config.getProductRevision(), "unknown");
        Assert.assertEquals(config.getClientOsInfo(),
                System.getProperty("os.name") + "/" + System.getProperty("os.version"));
        Assert.assertEquals(config.getClientJvmInfo(),
                System.getProperty("java.vm.name") + "/" + System.getProperty("java.vendor.version"));
        Assert.assertEquals(config.getClientUser(), System.getProperty("user.name"));
        Assert.assertEquals(config.getClientHost(), InetAddress.getLocalHost().getHostName());

        Assert.assertEquals(ClickHouseClientOption.buildUserAgent(null, null),
                "ClickHouse-JavaClient/unknown (" + System.getProperty("os.name") + "/"
                        + System.getProperty("os.version") + "; " + System.getProperty("java.vm.name") + "/"
                        + System.getProperty("java.vendor.version") + "; rv:unknown)");
        Assert.assertEquals(ClickHouseClientOption.buildUserAgent(null, null),
                ClickHouseClientOption.buildUserAgent("", null));

        config = new ClickHouseConfig(
                Collections.singletonMap(ClickHouseClientOption.CLIENT_NAME, "custom client name"));
        Assert.assertEquals(config.getClientName(), "custom client name");
    }
}
