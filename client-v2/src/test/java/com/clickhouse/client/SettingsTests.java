package com.clickhouse.client;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.Session;
import com.clickhouse.client.api.enums.SSLMode;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.internal.SslContextProvider;
import com.clickhouse.client.api.query.QuerySettings;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.KeyStore;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = {"unit"})
public class SettingsTests {

    @Test
    void testClientSettings() {
        List<String> source = Arrays.asList("ROL1", "ROL2,☺", "Rol,3,3");
        String listA = ClientConfigProperties.commaSeparated(source);
        List<String> listB = ClientConfigProperties.valuesFromCommaSeparated(listA);
        Assert.assertEquals(listB, source);
    }

    @Test
    void testSSLModeFromValue() {
        // Every constant resolves from its exact name and is case-insensitive.
        for (SSLMode mode : SSLMode.values()) {
            Assert.assertEquals(SSLMode.fromValue(mode.name()), mode);
            Assert.assertEquals(SSLMode.fromValue(mode.name().toLowerCase()), mode);
            Assert.assertEquals(SSLMode.fromValue(mode.name().toUpperCase()), mode);
        }

        // VERIFY_CA matches only with the underscore - matching does not normalize separators.
        Assert.assertEquals(SSLMode.fromValue("verify_ca"), SSLMode.VERIFY_CA);
        Assert.assertEquals(SSLMode.fromValue("Verify_Ca"), SSLMode.VERIFY_CA);
        Assert.assertThrows(IllegalArgumentException.class, () -> SSLMode.fromValue("verifyca"));

        // Unknown and null values are rejected.
        Assert.assertThrows(IllegalArgumentException.class, () -> SSLMode.fromValue("insecure"));
        Assert.assertThrows(IllegalArgumentException.class, () -> SSLMode.fromValue(""));
        Assert.assertThrows(IllegalArgumentException.class, () -> SSLMode.fromValue(null));
    }

    @Test
    void testSslContextFromKeyStore() throws Exception {
        SslContextProvider provider = new SslContextProvider();
        final String type = "PKCS12";
        final String password = "secret";
        File trustStore = createEmptyTrustStore(type, password);
        try {
            // Happy path: a readable trust store with the right password yields a usable TLS context.
            SSLContext ctx = provider.builder().trustStore(trustStore.getAbsolutePath(), password, type).build();
            Assert.assertNotNull(ctx);
            Assert.assertEquals(ctx.getProtocol(), "TLS");

            // Wrong password fails the integrity check.
            Assert.assertThrows(ClientMisconfigurationException.class,
                    () -> provider.builder().trustStore(trustStore.getAbsolutePath(), "wrong", type).build());

            // Missing file.
            Assert.assertThrows(ClientMisconfigurationException.class,
                    () -> provider.builder().trustStore(trustStore.getAbsolutePath() + ".missing", password, type).build());

            // Unknown keystore type.
            Assert.assertThrows(ClientMisconfigurationException.class,
                    () -> provider.builder().trustStore(trustStore.getAbsolutePath(), password, "NOT_A_TYPE").build());
        } finally {
            Files.deleteIfExists(trustStore.toPath());
        }
    }

    private static File createEmptyTrustStore(String type, String password) throws Exception {
        KeyStore ks = KeyStore.getInstance(type);
        ks.load(null, null);
        File file = File.createTempFile("client-v2-truststore", "." + type.toLowerCase());
        file.deleteOnExit();
        try (OutputStream out = Files.newOutputStream(file.toPath())) {
            ks.store(out, password.toCharArray());
        }
        return file;
    }

    @Test
    void testMergeSettings() {
        {
            QuerySettings settings1 = new QuerySettings().setQueryId("test1").httpHeader("key1", "value1");
            QuerySettings settings2 = new QuerySettings().httpHeader("key1", "value2");

            QuerySettings merged = QuerySettings.merge(settings1, settings2);
            Assert.assertNotSame(merged, settings1);
            Assert.assertNotSame(merged, settings2);

            Assert.assertEquals(merged.getAllSettings().get(ClientConfigProperties.httpHeader("key1")), "value2");
        }
        {
            InsertSettings settings1 = new InsertSettings().setQueryId("test1").httpHeader("key1", "value1");
            InsertSettings settings2 = new InsertSettings().httpHeader("key1", "value2").setInputStreamCopyBufferSize(200000);

            InsertSettings merged = InsertSettings.merge(settings1, settings2);
            Assert.assertNotSame(merged, settings1);
            Assert.assertNotSame(merged, settings2);

            Assert.assertEquals(merged.getInputStreamCopyBufferSize(), settings2.getInputStreamCopyBufferSize());
            Assert.assertEquals(merged.getAllSettings().get(ClientConfigProperties.httpHeader("key1")), "value2");
        }
    }

    @Test
    void testQuerySettingsSpecific() throws Exception {
        {
            final QuerySettings settings = new QuerySettings();
            settings.setUseTimeZone("America/Los_Angeles");
            Assert.assertThrows(IllegalArgumentException.class, () -> settings.setUseServerTimeZone(true));
            settings.resetOption(ClientConfigProperties.USE_TIMEZONE.getKey());
            settings.setUseServerTimeZone(true);
        }

        {
            final QuerySettings settings = new QuerySettings();
            settings.setUseServerTimeZone(true);
            Assert.assertTrue(settings.getUseServerTimeZone());
            Assert.assertThrows(IllegalArgumentException.class, () -> settings.setUseTimeZone("America/Los_Angeles"));
        }

        {
            final QuerySettings settings = new QuerySettings();
            settings.setDatabase("test_db1");
            Assert.assertEquals(settings.getDatabase(), "test_db1");
        }

        {
            final QuerySettings settings = new QuerySettings();
            settings.setReadBufferSize(10000);
            Assert.assertEquals(settings.getReadBufferSize(), 10000);

            Assert.assertThrows(IllegalArgumentException.class, () -> settings.setReadBufferSize(1000));
        }

        {
            final QuerySettings settings = new QuerySettings();
            int val = 10000;
            settings.setMaxExecutionTime(val);
            Assert.assertEquals(settings.getMaxExecutionTime(), val);
            Assert.assertEquals(settings.getAllSettings().get(
                    ClientConfigProperties.serverSetting(ServerSettings.MAX_EXECUTION_TIME)), String.valueOf(val));
        }

        {
            final QuerySettings settings = new QuerySettings();
            settings.setDBRoles(Arrays.asList("role1", "role2"));
            Assert.assertEquals(settings.getDBRoles(), Arrays.asList("role1", "role2"));
            settings.setDBRoles(Collections.emptyList());
            Assert.assertEquals(settings.getDBRoles(), Collections.emptyList());
        }

        {
            final QuerySettings settings = new QuerySettings();
            settings.logComment("comment1");
            Assert.assertEquals(settings.getLogComment(), "comment1");
            settings.logComment("comment2");
            Assert.assertEquals(settings.getLogComment(), "comment2");
            settings.logComment(null);
            Assert.assertNull(settings.getLogComment());
        }

        {
            final QuerySettings settings = new QuerySettings();
            Assert.assertEquals(settings.getNetworkTimeout().intValue(),
                    (Integer) ClientConfigProperties.SOCKET_OPERATION_TIMEOUT.getDefObjVal());
            settings.setNetworkTimeout(10, ChronoUnit.SECONDS);
            Assert.assertEquals(settings.getNetworkTimeout(), TimeUnit.SECONDS.toMillis(10));
        }

        {
            final QuerySettings settings = new QuerySettings();
            settings.setSessionId("session-1");
            settings.setSessionCheck(true);
            settings.setSessionTimeout(30);
            settings.setSessionTimezone("Asia/Tokyo");
            Assert.assertEquals(settings.getSessionId(), "session-1");
            Assert.assertTrue(settings.getSessionCheck());
            Assert.assertEquals(settings.getSessionTimeout().intValue(), 30);
            Assert.assertEquals(settings.getSessionTimezone(), "Asia/Tokyo");
            Assert.assertThrows(IllegalArgumentException.class, () -> settings.setSessionId(""));
            Assert.assertThrows(IllegalArgumentException.class, () -> settings.setSessionTimeout(0));
            Assert.assertThrows(IllegalArgumentException.class, () -> settings.setSessionTimezone(""));
        }

        {
            final Session session = new Session()
                    .setSessionId("session-use-1")
                    .setSessionCheck(true)
                    .setSessionTimeout(45)
                    .setSessionTimezone("Europe/Berlin");
            final QuerySettings settings = new QuerySettings().use(session);
            Assert.assertEquals(settings.getSessionId(), "session-use-1");
            Assert.assertTrue(settings.getSessionCheck());
            Assert.assertEquals(settings.getSessionTimeout().intValue(), 45);
            Assert.assertEquals(settings.getSessionTimezone(), "Europe/Berlin");
        }

        {
            final QuerySettings settings = new QuerySettings();
            settings.setSessionId("session-clear-1");
            settings.setSessionCheck(true);
            settings.setSessionTimeout(60);
            settings.setSessionTimezone("America/New_York");
            Assert.assertNotNull(settings.getSessionId());
            Assert.assertNotNull(settings.getSessionCheck());
            Assert.assertNotNull(settings.getSessionTimeout());
            Assert.assertNotNull(settings.getSessionTimezone());

            settings.clearSession();

            Assert.assertNull(settings.getSessionId(), "clearSession() must remove session_id");
            Assert.assertNull(settings.getSessionCheck(), "clearSession() must remove session_check");
            Assert.assertNull(settings.getSessionTimeout(), "clearSession() must remove session_timeout");
            // session_timezone is not session-management state; it is preserved across clearSession().
            Assert.assertEquals(settings.getSessionTimezone(), "America/New_York",
                    "clearSession() must not remove session_timezone");

            // Non-session settings are unaffected.
            settings.setDatabase("db1");
            settings.clearSession();
            Assert.assertEquals(settings.getDatabase(), "db1");
        }
    }

    @Test
    public void testInsertSettingsSpecific() throws Exception {
        {
            final InsertSettings settings = new InsertSettings();
            settings.setDatabase("test_db1");
            Assert.assertEquals(settings.getDatabase(), "test_db1");
        }

        {
            final InsertSettings settings = new InsertSettings();
            Assert.assertFalse(settings.isClientCompressionEnabled());
            settings.compressClientRequest(true);
            Assert.assertTrue(settings.isClientCompressionEnabled());
            Assert.assertTrue(settings.isClientRequestEnabled());
        }


        {
            final InsertSettings settings = new InsertSettings();
            settings.httpHeader("key1", "value1");
            Assert.assertEquals(settings.getAllSettings().get(ClientConfigProperties.httpHeader("key1")), "value1");
            settings.httpHeader("key1", "value2");
            Assert.assertEquals(settings.getAllSettings().get(ClientConfigProperties.httpHeader("key1")), "value2");
        }


        {
            final InsertSettings settings = new InsertSettings();
            settings.serverSetting("key1", "value1");
            Assert.assertEquals(settings.getAllSettings().get(ClientConfigProperties.serverSetting("key1")), "value1");
            settings.serverSetting("key1", "value2");
            Assert.assertEquals(settings.getAllSettings().get(ClientConfigProperties.serverSetting("key1")), "value2");
        }

        {
            final InsertSettings settings = new InsertSettings();
            settings.setDBRoles(Arrays.asList("role1", "role2"));
            Assert.assertEquals(settings.getDBRoles(), Arrays.asList("role1", "role2"));
            settings.setDBRoles(Collections.emptyList());
            Assert.assertEquals(settings.getDBRoles(), Collections.emptyList());
        }

        {
            final InsertSettings settings = new InsertSettings();
            settings.logComment("comment1");
            Assert.assertEquals(settings.getLogComment(), "comment1");
            settings.logComment("comment2");
            Assert.assertEquals(settings.getLogComment(), "comment2");
            settings.logComment(null);
            Assert.assertNull(settings.getLogComment());
        }

        {
            final InsertSettings settings = new InsertSettings();
            Assert.assertEquals(settings.getNetworkTimeout().intValue(),
                    (Integer) ClientConfigProperties.SOCKET_OPERATION_TIMEOUT.getDefObjVal());
            settings.setNetworkTimeout(10, ChronoUnit.SECONDS);
            Assert.assertEquals(settings.getNetworkTimeout(), TimeUnit.SECONDS.toMillis(10));
        }

        {
            final InsertSettings settings = new InsertSettings();
            settings.setSessionId("session-2");
            settings.setSessionCheck(false);
            settings.setSessionTimeout(45);
            settings.setSessionTimezone("Europe/Paris");
            Assert.assertEquals(settings.getSessionId(), "session-2");
            Assert.assertFalse(settings.getSessionCheck());
            Assert.assertEquals(settings.getSessionTimeout().intValue(), 45);
            Assert.assertEquals(settings.getSessionTimezone(), "Europe/Paris");
            Assert.assertThrows(IllegalArgumentException.class, () -> settings.setSessionId(""));
            Assert.assertThrows(IllegalArgumentException.class, () -> settings.setSessionTimeout(-1));
            Assert.assertThrows(IllegalArgumentException.class, () -> settings.setSessionTimezone(""));
        }

        {
            final Session session = new Session()
                    .setSessionId("session-use-2")
                    .setSessionCheck(false)
                    .setSessionTimeout(50)
                    .setSessionTimezone("Europe/Paris");
            final InsertSettings settings = new InsertSettings().use(session);
            Assert.assertEquals(settings.getSessionId(), "session-use-2");
            Assert.assertFalse(settings.getSessionCheck());
            Assert.assertEquals(settings.getSessionTimeout().intValue(), 50);
            Assert.assertEquals(settings.getSessionTimezone(), "Europe/Paris");
        }

        {
            final InsertSettings settings = new InsertSettings();
            settings.setSessionId("session-clear-2");
            settings.setSessionCheck(true);
            settings.setSessionTimeout(90);
            settings.setSessionTimezone("Asia/Tokyo");
            Assert.assertNotNull(settings.getSessionId());
            Assert.assertNotNull(settings.getSessionCheck());
            Assert.assertNotNull(settings.getSessionTimeout());
            Assert.assertNotNull(settings.getSessionTimezone());

            settings.clearSession();

            Assert.assertNull(settings.getSessionId(), "clearSession() must remove session_id");
            Assert.assertNull(settings.getSessionCheck(), "clearSession() must remove session_check");
            Assert.assertNull(settings.getSessionTimeout(), "clearSession() must remove session_timeout");
            // session_timezone is not session-management state; it is preserved across clearSession().
            Assert.assertEquals(settings.getSessionTimezone(), "Asia/Tokyo",
                    "clearSession() must not remove session_timezone");

            // Non-session settings are unaffected.
            settings.setDatabase("db2");
            settings.clearSession();
            Assert.assertEquals(settings.getDatabase(), "db2");
        }
    }
}
