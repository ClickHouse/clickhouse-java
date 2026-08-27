package com.clickhouse.migration.examples;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

public class JdbcConfigurationMigrationExampleTest {

    @Test
    public void testExampleMethods() throws Exception {
        JdbcConfigurationMigrationExample example = new JdbcConfigurationMigrationExample();

        Properties v1Props = new Properties();
        v1Props.setProperty("user", "default");
        v1Props.setProperty("connect_timeout", "5000");
        v1Props.setProperty("max_threads", "8");

        // Verify conversion logic works as demonstrated in example
        String v1Url = "jdbc:clickhouse://localhost:8123/default?connect_timeout=5000&max_threads=8";
        String convertedUrl = com.clickhouse.migration.config.ConfigurationMigrationHelper.convertUrl(v1Url);
        Assert.assertEquals(convertedUrl, "jdbc:clickhouse://localhost:8123/default?connection_timeout=5000&clickhouse_setting_max_threads=8");

        Properties convertedProps = com.clickhouse.migration.config.ConfigurationMigrationHelper.convertProperties(v1Props);
        Assert.assertEquals(convertedProps.getProperty("connection_timeout"), "5000");
        Assert.assertEquals(convertedProps.getProperty("clickhouse_setting_max_threads"), "8");
    }
}
