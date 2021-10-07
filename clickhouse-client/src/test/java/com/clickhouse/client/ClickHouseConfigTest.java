package com.clickhouse.client;

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseConfigOption;
import com.clickhouse.client.config.ClickHouseDefaults;

public class ClickHouseConfigTest {
        @Test(groups = { "unit" })
        public void testDefaultValues() {
                ClickHouseConfig config = new ClickHouseConfig(null, null, null, null, null);
                Assert.assertEquals(config.getClientName(),
                                ClickHouseClientOption.CLIENT_NAME.getEffectiveDefaultValue());
                Assert.assertEquals(config.getDatabase(), ClickHouseDefaults.DATABASE.getEffectiveDefaultValue());

                Assert.assertEquals(config.getOption(ClickHouseDefaults.CLUSTER),
                                ClickHouseDefaults.CLUSTER.getEffectiveDefaultValue());
                Assert.assertEquals(config.getOption(ClickHouseDefaults.HOST),
                                ClickHouseDefaults.HOST.getEffectiveDefaultValue());
                Assert.assertEquals(config.getOption(ClickHouseDefaults.PORT),
                                ClickHouseDefaults.PORT.getEffectiveDefaultValue());
                Assert.assertEquals(config.getOption(ClickHouseDefaults.WEIGHT),
                                ClickHouseDefaults.WEIGHT.getEffectiveDefaultValue());
                ClickHouseCredentials credentials = config.getDefaultCredentials();
                Assert.assertEquals(credentials.useAccessToken(), false);
                Assert.assertEquals(credentials.getUserName(), ClickHouseDefaults.USER.getEffectiveDefaultValue());
                Assert.assertEquals(credentials.getPassword(), ClickHouseDefaults.PASSWORD.getEffectiveDefaultValue());
                Assert.assertEquals(config.getFormat().name(), ClickHouseDefaults.FORMAT.getEffectiveDefaultValue());
                Assert.assertFalse(config.getMetricRegistry().isPresent());
        }

        @Test(groups = { "unit" })
        public void testCustomValues() throws Exception {
                String clientName = "test client";
                String cluster = "test cluster";
                String database = "test_database";
                String host = "test.host";
                Integer port = 12345;
                Integer weight = -99;
                String user = "sa";
                String password = "welcome";

                Map<ClickHouseConfigOption, Object> options = new HashMap<>();
                options.put(ClickHouseClientOption.CLIENT_NAME, clientName);
                options.put(ClickHouseDefaults.CLUSTER, cluster);
                options.put(ClickHouseClientOption.DATABASE, database);
                options.put(ClickHouseDefaults.HOST, host);
                options.put(ClickHouseDefaults.PORT, port);
                options.put(ClickHouseDefaults.WEIGHT, weight);
                options.put(ClickHouseDefaults.USER, "useless");
                options.put(ClickHouseDefaults.PASSWORD, "useless");

                Object metricRegistry = new Object();

                ClickHouseConfig config = new ClickHouseConfig(options,
                                ClickHouseCredentials.fromUserAndPassword(user, password), null, metricRegistry);
                Assert.assertEquals(config.getClientName(), clientName);
                Assert.assertEquals(config.getDatabase(), database);
                Assert.assertEquals(config.getOption(ClickHouseDefaults.CLUSTER), cluster);
                Assert.assertEquals(config.getOption(ClickHouseDefaults.HOST), host);
                Assert.assertEquals(config.getOption(ClickHouseDefaults.PORT), port);
                Assert.assertEquals(config.getOption(ClickHouseDefaults.WEIGHT), weight);

                ClickHouseCredentials credentials = config.getDefaultCredentials();
                Assert.assertEquals(credentials.useAccessToken(), false);
                Assert.assertEquals(credentials.getUserName(), user);
                Assert.assertEquals(credentials.getPassword(), password);
                Assert.assertEquals(config.getPreferredProtocols().size(), 0);
                Assert.assertEquals(config.getPreferredTags().size(), 0);
                Assert.assertEquals(config.getMetricRegistry().get(), metricRegistry);
        }
}
