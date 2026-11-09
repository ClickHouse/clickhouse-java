package com.clickhouse.jdbc;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.jdbc.internal.JdbcConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class ClientNameTest {
    @DataProvider(name = "clientNames")
    public Object[][] clientNames() {
        return new Object[][] {
                {"my-app/1.0", "(apache.spark)", false},
                {"", "(apache.spark)", false},
                {null, "(apache.spark)", false},
                {"my-app/1.0", "", false},
                {"", "", false},
                {null, "", false},
                {"my-app/1.0", "(apache.spark)", true},
                {"", "(apache.spark)", true},
                {null, "(apache.spark)", true}
        };
    }

    @Test(dataProvider = "clientNames")
    public void updateClientNamePreservesFrameworks(String appName, String frameworks, boolean disabled) throws Exception {
        String previousFrameworks = Driver.FrameworksDetection.frameworksDetected;
        try {
            Driver.FrameworksDetection.frameworksDetected = frameworks;
            Properties properties = new Properties();
            properties.setProperty("disable_frameworks_detection", Boolean.toString(disabled));
            JdbcConfiguration config = new JdbcConfiguration("jdbc:clickhouse://localhost:8123", properties);
            try (Client client = config.applyClientProperties(new Client.Builder()).build()) {
                config.updateUserClient(appName, client);
                String expected = appName == null || appName.isEmpty() ? "" : appName + " ";
                expected += JdbcConfiguration.getDefaultClientName();
                if (!disabled && !frameworks.isEmpty()) {
                    expected += " (" + frameworks + ")";
                }
                Field configuration = Client.class.getDeclaredField("configuration");
                configuration.setAccessible(true);
                assertEquals(((Map<?, ?>) configuration.get(client)).get(ClientConfigProperties.CLIENT_NAME.getKey()), expected);
            }
        } finally {
            Driver.FrameworksDetection.frameworksDetected = previousFrameworks;
        }
    }
}
