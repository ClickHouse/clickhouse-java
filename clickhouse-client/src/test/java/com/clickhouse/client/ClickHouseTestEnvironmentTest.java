package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ClickHouseTestEnvironmentTest {
    @DataProvider(name = "containerModes")
    public static Object[][] containerModes() {
        return new Object[][] {
                { null, ClickHouseTestEnvironment.ContainerMode.SINGLE },
                { "", ClickHouseTestEnvironment.ContainerMode.SINGLE },
                { "single", ClickHouseTestEnvironment.ContainerMode.SINGLE },
                { "SINGLE", ClickHouseTestEnvironment.ContainerMode.SINGLE },
                { "cluster", ClickHouseTestEnvironment.ContainerMode.CLUSTER },
                { " none ", ClickHouseTestEnvironment.ContainerMode.NONE }
        };
    }

    @Test(dataProvider = "containerModes", groups = { "unit" })
    public void testContainerMode(String raw, ClickHouseTestEnvironment.ContainerMode expected) {
        ClickHouseTestEnvironment environment = ClickHouseTestEnvironment.from(env(ClickHouseTestEnvironment.CONTAINER, raw));
        Assert.assertEquals(environment.getContainerMode(), expected);
    }

    @Test(groups = { "unit" })
    public void testClickHouseVersionSystemPropertyIsRejected() {
        Properties properties = new Properties();
        properties.setProperty(ClickHouseTestEnvironment.REMOVED_VERSION_PROPERTY, "24.8");
        try {
            ClickHouseTestEnvironment.rejectRemovedOptions(properties);
            Assert.fail("clickhouseVersion must be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("clickhouseVersion"));
            Assert.assertTrue(expected.getMessage().contains("TEST_CLICKHOUSE_IMAGE_VERSION"));
        }
    }

    @Test(groups = { "unit" })
    public void testAbsentClickHouseVersionPropertyIsAllowed() {
        ClickHouseTestEnvironment.rejectRemovedOptions(new Properties());
        ClickHouseTestEnvironment.rejectRemovedOptions(null);
    }

    @Test(groups = { "unit" })
    public void testRejectsUnknownContainerMode() {
        try {
            ClickHouseTestEnvironment.from(env(ClickHouseTestEnvironment.CONTAINER, "cloud"));
            Assert.fail("cloud is not a container mode");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("single, cluster, or none"));
        }
    }

    @DataProvider(name = "images")
    public static Object[][] images() {
        return new Object[][] {
                { null, null, "clickhouse/clickhouse-server", "" },
                { null, "24.8", "clickhouse/clickhouse-server:24.8", "24.8" },
                { null, "latest", "clickhouse/clickhouse-server:latest", "" },
                { "repo/clickhouse:24.3", "25.8", "repo/clickhouse:24.3", "24.3" },
                { "repo/clickhouse:24.3@sha256:abc", null, "repo/clickhouse:24.3@sha256:abc", "24.3" },
                { "repo/clickhouse@sha256:abc", null, "repo/clickhouse@sha256:abc", "" }
        };
    }

    @Test(dataProvider = "images", groups = { "unit" })
    public void testImageRef(String image, String version, String expectedRef, String expectedVersion) {
        Map<String, String> values = new HashMap<String, String>();
        if (image != null) {
            values.put(ClickHouseTestEnvironment.IMAGE, image);
        }
        if (version != null) {
            values.put(ClickHouseTestEnvironment.IMAGE_VERSION, version);
        }
        ClickHouseTestEnvironment environment = ClickHouseTestEnvironment.from(values);
        Assert.assertEquals(environment.getImageRef(), expectedRef);
        Assert.assertEquals(environment.getClickHouseVersion(), expectedVersion);
    }

    @Test(groups = { "unit" })
    public void testVersionFallback() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(ClickHouseTestEnvironment.VERSION, "26.9");
        ClickHouseTestEnvironment environment = ClickHouseTestEnvironment.from(values);
        Assert.assertEquals(environment.getImageRef(), "clickhouse/clickhouse-server:26.9");
        Assert.assertEquals(environment.getClickHouseVersion(), "26.9");

        // IMAGE_VERSION takes precedence over VERSION
        values.put(ClickHouseTestEnvironment.IMAGE_VERSION, "25.8");
        environment = ClickHouseTestEnvironment.from(values);
        Assert.assertEquals(environment.getImageRef(), "clickhouse/clickhouse-server:25.8");
        Assert.assertEquals(environment.getClickHouseVersion(), "25.8");
    }

    @Test(groups = { "unit" })
    public void testOldClickHouseImageInstallsTzdata() {
        ClickHouseTestEnvironment environment = ClickHouseTestEnvironment.from(
                env(ClickHouseTestEnvironment.IMAGE_VERSION, "21.3"));
        Assert.assertEquals(environment.getAdditionalPackages(), "tzdata");

        Map<String, String> values = env(ClickHouseTestEnvironment.IMAGE_VERSION, "21.3");
        values.put(ClickHouseTestEnvironment.ADDITIONAL_PACKAGES, "ca-certificates");
        Assert.assertEquals(ClickHouseTestEnvironment.from(values).getAdditionalPackages(),
                "ca-certificates tzdata");
    }

    @Test(groups = { "unit" })
    public void testCurrentImageDoesNotInstallTzdata() {
        ClickHouseTestEnvironment environment = ClickHouseTestEnvironment.from(
                env(ClickHouseTestEnvironment.IMAGE_VERSION, "24.8"));
        Assert.assertNull(environment.getAdditionalPackages());
    }

    @Test(groups = { "unit" })
    public void testConnectionDefaults() {
        ClickHouseTestEnvironment environment = ClickHouseTestEnvironment.from(Collections.<String, String>emptyMap());
        Assert.assertEquals(environment.getHost(), "localhost");
        Assert.assertEquals(environment.getUser(), "default");
        Assert.assertEquals(environment.getPassword(), "test_default_password");
        Assert.assertFalse(environment.isSecure());
        Assert.assertEquals(environment.getTimezone(), "UTC");
        Assert.assertTrue(environment.isLocalDatabase());
        Assert.assertTrue(environment.getDatabase().startsWith("clickhouse_java_"));
    }

    @Test(groups = { "unit" })
    public void testConnectionOverrides() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(ClickHouseTestEnvironment.HOST, "cloud.example");
        values.put(ClickHouseTestEnvironment.USER, "demo");
        values.put(ClickHouseTestEnvironment.PASSWORD, "secret");
        values.put(ClickHouseTestEnvironment.SECURE, "true");
        values.put(ClickHouseTestEnvironment.CONTAINER, "none");
        values.put("TEST_CLICKHOUSE_HTTP_PORT", "9440");
        ClickHouseTestEnvironment environment = ClickHouseTestEnvironment.from(values);

        Assert.assertEquals(environment.getHost(), "cloud.example");
        Assert.assertEquals(environment.getUser(), "demo");
        Assert.assertEquals(environment.getPassword(), "secret");
        Assert.assertTrue(environment.isSecure());
        Assert.assertEquals(environment.getContainerMode(), ClickHouseTestEnvironment.ContainerMode.NONE);
        Assert.assertEquals(environment.portOverride(ClickHouseProtocol.HTTP), "9440");
    }

    @Test(groups = { "unit" })
    public void testExternalDatabaseMustUseTestPrefix() {
        try {
            ClickHouseTestEnvironment.from(env(ClickHouseTestEnvironment.DATABASE, "default"));
            Assert.fail("external database name must be rejected");
        } catch (RuntimeException expected) {
            Assert.assertTrue(expected.getMessage().contains("clickhouse_java_test_"));
        }
    }

    @Test(groups = { "unit" })
    public void testExternalDatabase() {
        ClickHouseTestEnvironment environment = ClickHouseTestEnvironment.from(
                env(ClickHouseTestEnvironment.DATABASE, "clickhouse_java_test_ci"));
        Assert.assertFalse(environment.isLocalDatabase());
        Assert.assertEquals(environment.getDatabase(), "clickhouse_java_test_ci");
    }

    private static Map<String, String> env(String key, String value) {
        Map<String, String> values = new HashMap<String, String>();
        if (value != null) {
            values.put(key, value);
        }
        return values;
    }
}
