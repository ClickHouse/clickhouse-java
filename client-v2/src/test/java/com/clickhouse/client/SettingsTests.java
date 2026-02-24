package com.clickhouse.client;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.QuerySettings;
import org.testng.Assert;
import org.testng.annotations.Test;

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
    }
}
