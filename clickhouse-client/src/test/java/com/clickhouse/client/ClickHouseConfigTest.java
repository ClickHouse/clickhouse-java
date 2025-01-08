package com.clickhouse.client;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.config.ClickHouseOption;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        Matcher versioMatcher = Pattern.compile("(^|\\\\.[\\\\d]+)+.*").matcher(config.getProductVersion());
        Assert.assertTrue(versioMatcher.matches());
        Assert.assertEquals(config.getProductRevision(), "unknown");
        Assert.assertEquals(config.getClientOsInfo(),
                System.getProperty("os.name") + "/" + System.getProperty("os.version"));
        Assert.assertEquals(config.getClientJvmInfo(),
                System.getProperty("java.vm.name") + "/" + System.getProperty("java.vendor.version",
                        System.getProperty("java.vm.version", System.getProperty("java.version", "unknown"))));
        Assert.assertEquals(config.getClientUser(), System.getProperty("user.name"));
        Assert.assertEquals(config.getClientHost(), InetAddress.getLocalHost().getHostName());

        Assert.assertEquals(ClickHouseClientOption.buildUserAgent(null, null),
                "ClickHouse-JavaClient/"+ ClickHouseClientOption.PRODUCT_VERSION + " (" + System.getProperty("java.vm.name") + "/"
                        + System.getProperty("java.vendor.version",
                                System.getProperty("java.vm.version", System.getProperty("java.version", "unknown")))
                        + ")");
        Assert.assertEquals(ClickHouseClientOption.buildUserAgent(null, null),
                ClickHouseClientOption.buildUserAgent("", null));

        config = new ClickHouseConfig(
                Collections.singletonMap(ClickHouseClientOption.CLIENT_NAME, "custom client name"));
        Assert.assertEquals(config.getClientName(), "custom client name");
    }

    @Test(groups = { "unit" })
    public void testClientOptions() {
        Assert.assertTrue(ClickHouseConfig.ClientOptions.INSTANCE.customOptions.isEmpty(),
                "Should NOT have any custom option");
        Assert.assertFalse(ClickHouseConfig.ClientOptions.INSTANCE.sensitiveOptions.isEmpty(),
                "Should have at least one sensitive option");
        Assert.assertEquals(ClickHouseConfig.ClientOptions.INSTANCE.sensitiveOptions.get("sslkey"),
                ClickHouseClientOption.SSL_KEY);
    }


    @Test
    public void testCustomBufferSizes() {
        final int writeBuffSize = 5 * ClickHouseConfig.DEFAULT_MAX_BUFFER_SIZE;
        final int readBuffSize = 6 * ClickHouseConfig.DEFAULT_MAX_BUFFER_SIZE;
        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        options.put(ClickHouseClientOption.WRITE_BUFFER_SIZE, writeBuffSize);
        options.put(ClickHouseClientOption.READ_BUFFER_SIZE, readBuffSize);
        options.put(ClickHouseClientOption.REQUEST_CHUNK_SIZE, writeBuffSize);
        ClickHouseConfig configWithDefaultMax = new ClickHouseConfig(options);

        Assert.assertEquals(configWithDefaultMax.getWriteBufferSize(), ClickHouseConfig.DEFAULT_MAX_BUFFER_SIZE);
        Assert.assertEquals(configWithDefaultMax.getReadBufferSize(), ClickHouseConfig.DEFAULT_MAX_BUFFER_SIZE);

        final int customMaxBufferSize = 100 * ClickHouseConfig.DEFAULT_MAX_BUFFER_SIZE;
        options.put(ClickHouseClientOption.MAX_BUFFER_SIZE, customMaxBufferSize);
        ClickHouseConfig configWithCustomMax = new ClickHouseConfig(options);

        Assert.assertEquals(configWithCustomMax.getWriteBufferSize(), writeBuffSize);
        Assert.assertEquals(configWithCustomMax.getReadBufferSize(), readBuffSize);
        Assert.assertEquals(configWithCustomMax.getMaxBufferSize(), customMaxBufferSize);

        // Test defaults
        options.clear();
        options.put(ClickHouseClientOption.WRITE_BUFFER_SIZE, -1);
        options.put(ClickHouseClientOption.READ_BUFFER_SIZE, -1);
        options.put(ClickHouseClientOption.REQUEST_CHUNK_SIZE, -1);
        ClickHouseConfig config = new ClickHouseConfig(options);

        Assert.assertEquals(config.getWriteBufferSize(), config.getBufferSize());
        Assert.assertEquals(config.getReadBufferSize(), config.getBufferSize());
    }
}
