package com.clickhouse.client.http;

import java.util.UUID;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseRequestManager;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseVersion;
import com.clickhouse.client.ClientIntegrationTest;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseHealthCheckMethod;
import com.clickhouse.client.data.ClickHouseStringValue;
import com.clickhouse.client.http.config.ClickHouseHttpOption;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseHttpClientTest extends ClientIntegrationTest {
    @Override
    protected ClickHouseProtocol getProtocol() {
        return ClickHouseProtocol.HTTP;
    }

    @Override
    protected Class<? extends ClickHouseClient> getClientClass() {
        return ClickHouseHttpClient.class;
    }

    @Test(groups = "integration")
    @Override
    public void testSession() throws Exception {
        super.testSession();

        ClickHouseNode server = getServer();
        String sessionId = ClickHouseRequestManager.getInstance().createSessionId();
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> req = client.connect(server).session(sessionId, true)
                    .option(ClickHouseHttpOption.CUSTOM_PARAMS, "session_check=0,max_query_size=1000")
                    .transaction(null)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
            try (ClickHouseResponse resp = req.copy()
                    .query("create temporary table test_session(a String)engine=Memory as select '1'")
                    .executeAndWait()) {
                // ignore
            }
            try (ClickHouseResponse resp = req.copy().query("select * from test_session").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asInteger(), 1);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testPing() throws Exception {
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP)) {
            Assert.assertTrue(client.ping(getServer(), 3000));
        }

        try (ClickHouseClient client = ClickHouseClient.builder()
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .option(ClickHouseHttpOption.WEB_CONTEXT, "a/b").build()) {
            Assert.assertTrue(client.ping(getServer(), 3000));
        }

        try (ClickHouseClient client = ClickHouseClient.builder()
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .option(ClickHouseHttpOption.WEB_CONTEXT, "a/b")
                .option(ClickHouseClientOption.HEALTH_CHECK_METHOD, ClickHouseHealthCheckMethod.PING).build()) {
            Assert.assertFalse(client.ping(getServer(), 3000));
        }

        try (ClickHouseClient client = ClickHouseClient.builder()
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .option(ClickHouseHttpOption.WEB_CONTEXT, "/")
                .option(ClickHouseClientOption.HEALTH_CHECK_METHOD, ClickHouseHealthCheckMethod.PING).build()) {
            Assert.assertTrue(client.ping(getServer(), 3000));
        }

        try (ClickHouseClient client = ClickHouseClient.builder()
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .option(ClickHouseClientOption.HEALTH_CHECK_METHOD, ClickHouseHealthCheckMethod.PING)
                .removeOption(ClickHouseHttpOption.WEB_CONTEXT).build()) {
            Assert.assertTrue(client.ping(getServer(), 3000));
        }
    }

    @Test // (groups = "integration")
    public void testTransaction() throws Exception {
        testAbortTransaction();
        testNewTransaction();
        testJoinTransaction();
        testCommitTransaction();
        testRollbackTransaction();
        testTransactionSnapshot();
        testTransactionTimeout();
        testImplicitTransaction();
    }

    @Test // (groups = "integration")
    public void testSslClientAuth() throws Exception {
        // NPE on JDK 8:
        // java.lang.NullPointerException
        // at sun.security.provider.JavaKeyStore.convertToBytes(JavaKeyStore.java:822)
        // at
        // sun.security.provider.JavaKeyStore.engineSetKeyEntry(JavaKeyStore.java:271)
        // at
        // sun.security.provider.JavaKeyStore$JKS.engineSetKeyEntry(JavaKeyStore.java:57)
        // at
        // sun.security.provider.KeyStoreDelegator.engineSetKeyEntry(KeyStoreDelegator.java:117)
        // at
        // sun.security.provider.JavaKeyStore$DualFormatJKS.engineSetKeyEntry(JavaKeyStore.java:71)
        // at java.security.KeyStore.setKeyEntry(KeyStore.java:1140)
        // at
        // com.clickhouse.client.config.ClickHouseDefaultSslContextProvider.getKeyStore(ClickHouseDefaultSslContextProvider.java:105)
        ClickHouseNode server = getSecureServer(ClickHouseProtocol.HTTP);
        try (ClickHouseClient client = getSecureClient();
                ClickHouseResponse response = client.connect(server).query("select 123").executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asInteger(), 123);
        }
    }

    @Test(groups = { "integration" })
    @Override
    public void testMutation() throws Exception {
        super.testMutation();

        ClickHouseNode server = getServer();
        ClickHouseClient.send(server, "drop table if exists test_http_mutation",
                "create table test_http_mutation(a String, b Nullable(Int64))engine=Memory").get();
        try (ClickHouseClient client = getClient();
                ClickHouseResponse response = client.connect(server).set("send_progress_in_http_headers", 1)
                        .query("insert into test_http_mutation select toString(number), number from numbers(1)")
                        .execute().get()) {
            ClickHouseResponseSummary summary = response.getSummary();
            Assert.assertEquals(summary.getWrittenRows(), 1);
        }
    }

    @Test(groups = { "integration" })
    public void testLogComment() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        String uuid = UUID.randomUUID().toString();
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseRequest<?> request = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
            try (ClickHouseResponse resp = request
                    .query("select version()").execute().get()) {
                if (!ClickHouseVersion.of(resp.firstRecord().getValue(0).asString()).check("[21.2,)")) {
                    return;
                }
            }
            try (ClickHouseResponse resp = request
                    .option(ClickHouseClientOption.LOG_LEADING_COMMENT, true)
                    .query("-- select something\r\nselect 1", uuid).execute().get()) {
            }

            try (ClickHouseResponse resp = request
                    .option(ClickHouseClientOption.LOG_LEADING_COMMENT, true)
                    .query("SYSTEM FLUSH LOGS", uuid).execute().get()) {
            }

            try (ClickHouseResponse resp = request
                    .option(ClickHouseClientOption.LOG_LEADING_COMMENT, true)
                    .query(ClickHouseParameterizedQuery
                            .of(request.getConfig(), "select log_comment from system.query_log where query_id = :qid"))
                    .params(ClickHouseStringValue.of(uuid)).execute().get()) {
                int counter = 0;
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue(0).asString(), "select something");
                    counter++;
                }
                Assert.assertEquals(counter, 2);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testPost() throws Exception {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (ClickHouseClient client = ClickHouseClient.builder()
                .defaultCredentials(ClickHouseCredentials.fromUserAndPassword("foo", "bar")).build()) {
            // why no detailed error message for this: "select 1，2"
            try (ClickHouseResponse resp = client.connect(server).compressServerResponse(false)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query("select 1,2").execute().get()) {
                int count = 0;
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue(0).asInteger(), 1);
                    Assert.assertEquals(r.getValue(1).asInteger(), 2);
                    count++;
                }

                Assert.assertEquals(count, 1);
            }

            // reuse connection
            try (ClickHouseResponse resp = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query("select 3,4").execute().get()) {
                int count = 0;
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue(0).asInteger(), 3);
                    Assert.assertEquals(r.getValue(1).asInteger(), 4);
                    count++;
                }

                Assert.assertEquals(count, 1);
            }
        }
    }
}
