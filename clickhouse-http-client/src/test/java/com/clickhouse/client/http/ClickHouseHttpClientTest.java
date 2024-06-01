package com.clickhouse.client.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNode.Status;
import com.clickhouse.client.ClickHouseNodes;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseRequestManager;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.ClientIntegrationTest;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.data.value.ClickHouseStringValue;

import eu.rekawek.toxiproxy.ToxiproxyClient;

import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseHttpClientTest extends ClientIntegrationTest {
    @DataProvider(name = "requestCompressionMatrix")
    protected Object[][] getRequestCompressionMatrix() {
        return new Object[][] {
                { ClickHouseCompression.NONE, -2, 2, 1 },
                { ClickHouseCompression.LZ4, -2, 19, 1 }, // [0, 18]
                // { ClickHouseCompression.SNAPPY, -2, 33, 1024 }, // [1 * 1024, 32 * 1024]
                { ClickHouseCompression.ZSTD, -2, 23, 1 }, // [0, 22]
        };
    }

    @DataProvider(name = "mixedCompressionMatrix")
    protected Object[][] getMixedCompressionMatrix() {
        // ClickHouse Code: 638. DB::Exception: hadoop snappy decode
        // error:INVALID_INPUT. (SNAPPY_UNCOMPRESS_FAILED)
        ClickHouseCompression[] supportedRequestCompression = {
                ClickHouseCompression.NONE,
                ClickHouseCompression.LZ4,
                ClickHouseCompression.ZSTD };
        ClickHouseCompression[] supportedResponseCompression = {
                ClickHouseCompression.NONE,
                ClickHouseCompression.BROTLI,
                ClickHouseCompression.BZ2,
                ClickHouseCompression.DEFLATE,
                ClickHouseCompression.GZIP,
                ClickHouseCompression.LZ4,
                ClickHouseCompression.XZ,
                ClickHouseCompression.ZSTD };
        Object[][] matrix = new Object[supportedRequestCompression.length * supportedResponseCompression.length][];
        int i = 0;
        for (ClickHouseCompression reqComp : supportedRequestCompression) {
            for (ClickHouseCompression respComp : supportedResponseCompression) {
                matrix[i++] = new Object[] { reqComp, respComp };
            }
        }
        return matrix;
    }

    @Override
    protected ClickHouseProtocol getProtocol() {
        return ClickHouseProtocol.HTTP;
    }

    @Override
    protected Class<? extends ClickHouseClient> getClientClass() {
        return ClickHouseHttpClient.class;
    }

    @Override
    protected Map<ClickHouseOption, Serializable> getClientOptions() {
        return Collections.singletonMap(ClickHouseHttpOption.CONNECTION_PROVIDER,
                HttpConnectionProvider.HTTP_URL_CONNECTION);
    }

    @Test(groups = { "integration" })
    public void testNothing() throws Exception {
    }

    @Test(groups = "integration")
    public void testAuthentication() throws ClickHouseException {
        String sql = "select currentUser()";
        try (ClickHouseClient client = getClient(
                new ClickHouseConfig(null, ClickHouseCredentials.fromUserAndPassword("dba", "dba"), null, null));
                ClickHouseResponse response = newRequest(client, getServer())
                        // .option(ClickHouseHttpOption.CUSTOM_PARAMS, "user=dba,password=incorrect")
                        .query(sql).executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asString(), "dba");
        }

        try (ClickHouseClient client = getClient();
                ClickHouseResponse response = newRequest(client, getServer())
                        .option(ClickHouseHttpOption.CUSTOM_HEADERS, "Authorization=Basic ZGJhOmRiYQ==")
                        // .option(ClickHouseHttpOption.CUSTOM_PARAMS, "user=dba,password=incorrect")
                        .query(sql).executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asString(), "dba");
        }

        try (ClickHouseClient client = getClient();
                ClickHouseResponse response = newRequest(client, getServer(ClickHouseNode
                        .of("http://localhost?custom_http_headers=aUthorization%3DBasic%20ZGJhOmRiYQ%3D%3D")))
                        .query(sql).executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asString(), "dba");
        }
    }

    @Test(groups = "integration")
    public void testUserAgent() throws Exception {
        final ClickHouseNode server = getServer();
        final String sql = "select :uuid(String)";

        String uuid = UUID.randomUUID().toString();
        try (ClickHouseClient client = getClient();
                ClickHouseResponse response = newRequest(client, server)
                        .query(ClickHouseParameterizedQuery.of(client.getConfig(), sql))
                        .params(ClickHouseStringValue.of(uuid))
                        .executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asString(), uuid);
        }
        ClickHouseClient.send(server, "SYSTEM FLUSH LOGS").get();
        try (ClickHouseClient client = getClient();
                ClickHouseResponse response = newRequest(client, server)
                        .query("select http_user_agent from system.query_log where query='select ''" + uuid + "'''")
                        .executeAndWait()) {
            String result = response.firstRecord().getValue(0).asString();
            Assert.assertTrue(result.startsWith(client.getConfig().getProductName()));
            Assert.assertTrue(result.indexOf("Http") > 0);
        }

        uuid = UUID.randomUUID().toString();
        try (ClickHouseClient client = getClient();
                ClickHouseResponse response = newRequest(client, server)
                        .option(ClickHouseClientOption.PRODUCT_NAME, "MyCustomClient")
                        .query(ClickHouseParameterizedQuery.of(client.getConfig(), sql))
                        .params(ClickHouseStringValue.of(uuid))
                        .executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asString(), uuid);
        }
        ClickHouseClient.send(server, "SYSTEM FLUSH LOGS").get();
        try (ClickHouseClient client = getClient();
             ClickHouseResponse response = newRequest(client, server)
                     .query("select http_user_agent from system.query_log where query='select ''" + uuid + "'''")
                     .executeAndWait()) {
            String result = response.firstRecord().getValue(0).asString();
            Assert.assertTrue(result.startsWith("MyCustomClient"));
            Assert.assertTrue(result.indexOf("Http") > 0);
        }

        uuid = UUID.randomUUID().toString();
        try (ClickHouseClient client = getClient();
             ClickHouseResponse response = newRequest(client, server)
                     .option(ClickHouseClientOption.CLIENT_NAME, "MyCustomClient")
                     .query(ClickHouseParameterizedQuery.of(client.getConfig(), sql))
                     .params(ClickHouseStringValue.of(uuid))
                     .executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asString(), uuid);
        }
        ClickHouseClient.send(server, "SYSTEM FLUSH LOGS").get();
        try (ClickHouseClient client = getClient();
             ClickHouseResponse response = newRequest(client, server)
                     .query("select http_user_agent from system.query_log where query='select ''" + uuid + "'''")
                     .executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asString(), "MyCustomClient");
        }
    }

    @Override
    @Test(groups = "integration")
    public void testSession() throws ClickHouseException {
        super.testSession();

        ClickHouseNode server = getServer();
        String sessionId = ClickHouseRequestManager.getInstance().createSessionId();
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> req = newRequest(client, server).session(sessionId, true)
                    .option(ClickHouseHttpOption.CUSTOM_PARAMS, "session_check=0,max_query_size=1000")
                    .option(ClickHouseHttpOption.CONNECTION_PROVIDER, HttpConnectionProvider.APACHE_HTTP_CLIENT)
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

    @Test(groups = "integration")
    public void testPing() {
        try (ClickHouseClient client = ClickHouseClient.builder().options(getClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP)).build()) {
            Assert.assertTrue(client.ping(getServer(), 3000));
        }

        try (ClickHouseClient client = ClickHouseClient.builder().options(getClientOptions())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP)).build()) {
            ClickHouseNodes nodes = ClickHouseNodes.of("http://notthere," + getServer().getBaseUri());
            ClickHouseNode nonExistingNode = nodes.getNodes().get(0);
            nodes.update(nonExistingNode, Status.FAULTY);

            Assert.assertFalse(client.ping(nonExistingNode, 3000));
        }
    }

    @Test // (groups = "integration")
    public void testTransaction() throws ClickHouseException {
        testAbortTransaction();
        testNewTransaction();
        testJoinTransaction();
        testCommitTransaction();
        testRollbackTransaction();
        testTransactionSnapshot();
        testTransactionTimeout();
        testImplicitTransaction();
    }

    @Test(groups = {"integration"})
    public void testSslRootCertificateClientAuth() throws ClickHouseException {
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
        ClickHouseNode server = getSecureServer(ClickHouseProtocol.fromUriScheme("https"));
        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        options.put(ClickHouseClientOption.SSL, true);
        options.put(ClickHouseClientOption.SSL_MODE, ClickHouseSslMode.STRICT);
        options.put(ClickHouseClientOption.SSL_ROOT_CERTIFICATE, "containers/clickhouse-server/certs/localhost.crt");
        try (ClickHouseClient client = getSecureClient(new ClickHouseConfig(options));
             ClickHouseResponse response = newRequest(client, server)
                     .query("select 123").executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asInteger(), 123);
        }
    }


    @Test(groups = {"integration"})
    public void testTrustStoreSSLClientAuth() throws ClickHouseException {
        ClickHouseNode server = getSecureServer(ClickHouseProtocol.fromUriScheme("https"));
        Map<ClickHouseOption, Serializable> options = new HashMap<>();
        options.put(ClickHouseClientOption.SSL, true);
        options.put(ClickHouseClientOption.TRUST_STORE, "containers/clickhouse-server/certs/KeyStore.jks");
        options.put(ClickHouseClientOption.KEY_STORE_PASSWORD, "iloveclickhouse");
        try (ClickHouseClient client = getSecureClient(new ClickHouseConfig(options));
             ClickHouseResponse response = newRequest(client, server)
                     .query("select 123").executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asInteger(), 123);
        }
    }

    @Test(groups = {"integration"})
    public void testCreateTableAsSelect() throws ClickHouseException {
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists test_create_table_as_select");
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> request = newRequest(client, server);
            try (ClickHouseResponse resp = request.write()
                    .external(ClickHouseExternalTable.builder().name("myExtTable").addColumn("s", "String")
                            .addColumn("i", "Int32").content(ClickHouseInputStream.of("one,1\ntwo,2"))
                            .format(ClickHouseFormat.CSV).build())
                    .query("create table test_create_table_as_select engine=Memory as select * from myExtTable")
                    .executeAndWait()) {
                // ignore
            }

            try (ClickHouseResponse resp = request.format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query("select * from test_create_table_as_select order by i").executeAndWait()) {
                String[] array = new String[]{"one", "two"};
                int count = 0;
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue("i").asInteger(), count + 1);
                    Assert.assertEquals(r.getValue("s").asString(), array[count]);
                    count++;
                }
                Assert.assertEquals(count, array.length);
            }
        }
    }

    @Override
    @Test(groups = {"integration"})
    public void testMutation() throws ClickHouseException {
        super.testMutation();

        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists test_http_mutation",
                "create table test_http_mutation(a String, b Nullable(Int64))engine=Memory");
        try (ClickHouseClient client = getClient();
             ClickHouseResponse response = newRequest(client, server)
                     .set("send_progress_in_http_headers", 1)
                     .query("insert into test_http_mutation select toString(number), number from numbers(1)")
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            Assert.assertEquals(summary.getWrittenRows(), 1L);
        }
    }

    @Test(groups = {"integration"})
    public void testLogComment() throws ClickHouseException, IOException {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        String uuid = UUID.randomUUID().toString();
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseRequest<?> request = newRequest(client, server)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
            try (ClickHouseResponse resp = request
                    .query("select version()").executeAndWait()) {
                if (!ClickHouseVersion.of(resp.firstRecord().getValue(0).asString()).check("[21.2,)")) {
                    return;
                }
            }
            try (ClickHouseResponse resp = request
                    .option(ClickHouseClientOption.LOG_LEADING_COMMENT, true)
                    .query("-- select something\r\nselect 1", uuid).executeAndWait()) {
            }

            try (ClickHouseResponse resp = request
                    .option(ClickHouseClientOption.LOG_LEADING_COMMENT, true)
                    .query("SYSTEM FLUSH LOGS", uuid).executeAndWait()) {
            }

            try (ClickHouseResponse resp = request
                    .option(ClickHouseClientOption.LOG_LEADING_COMMENT, true)
                    .query(ClickHouseParameterizedQuery
                            .of(request.getConfig(), "select log_comment from system.query_log where query_id = :qid"))
                    .params(ClickHouseStringValue.of(uuid)).executeAndWait()) {
                int counter = 0;
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue(0).asString(), "select something");
                    counter++;
                }
                Assert.assertEquals(counter, 2);
            }
        }
    }

    @Test(groups = {"integration"})
    public void testPost() throws ClickHouseException {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (ClickHouseClient client = ClickHouseClient.builder().options(getClientOptions())
                .defaultCredentials(ClickHouseCredentials.fromUserAndPassword("foo", "bar")).build()) {
            // why no detailed error message for this: "select 1，2"
            try (ClickHouseResponse resp = newRequest(client, server).compressServerResponse(false)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query("select 1,2").executeAndWait()) {
                int count = 0;
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue(0).asInteger(), 1);
                    Assert.assertEquals(r.getValue(1).asInteger(), 2);
                    count++;
                }

                Assert.assertEquals(count, 1);
            }

            // reuse connection
            try (ClickHouseResponse resp = newRequest(client, server)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query("select 3,4").executeAndWait()) {
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

    @Test(groups = {"integration"})
    public void testProxyConnection() throws ClickHouseException, IOException {
        ToxiproxyContainer toxiproxy = null;
        if (!ClickHouseServerForTest.hasProxyAddress()) {
            toxiproxy = new ToxiproxyContainer(ClickHouseServerForTest.getProxyImage())
                    .withNetwork(ClickHouseServerForTest.getNetwork());
            toxiproxy.start();

            ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            toxiproxyClient.createProxy("clickhouse", "0.0.0.0:8666",
                    ClickHouseServerForTest.hasClickHouseContainer()
                            ? "clickhouse:" + ClickHouseProtocol.HTTP.getDefaultPort()
                            : ClickHouseServerForTest.getClickHouseAddress(ClickHouseProtocol.HTTP, true));
        }

        try {
            String proxyHost = toxiproxy != null ? toxiproxy.getHost() : ClickHouseServerForTest.getProxyHost();
            int proxyPort = toxiproxy != null ? toxiproxy.getMappedPort(8666) : ClickHouseServerForTest.getProxyPort();

            Map<String, String> options = new HashMap<>();
            // without any proxy options
            try (ClickHouseClient client = ClickHouseClient.builder().options(getClientOptions())
                    .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP)).build()) {
                ClickHouseNode server = getServer(ClickHouseProtocol.HTTP, options);
                Assert.assertTrue(client.ping(server, 30000), "Can not ping");
                Assert.assertEquals(
                        client.read(server).query("select 5").executeAndWait().firstRecord().getValue(0).asString(),
                        "5");
            }

            options.put("proxy_type", "HTTP");
            // without hostname and port of the proxy server
            try (ClickHouseClient client = ClickHouseClient.builder().options(getClientOptions())
                    .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP)).build()) {
                ClickHouseNode server = getServer(ClickHouseProtocol.HTTP, options);
                Assert.assertFalse(client.ping(server, 30000), "Ping should fail due to incomplete proxy options");
                Assert.assertThrows(ClickHouseException.class,
                        () -> client.read(server).query("select 1").executeAndWait());
            }

            // without proxy_port
// Disable tests for ping via proxy
//            options.put("proxy_host", proxyHost);
//            try (ClickHouseClient client = ClickHouseClient.builder().options(getClientOptions())
//                    .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP)).build()) {
//                ClickHouseNode server = getServer(ClickHouseProtocol.HTTP, options);
//                Assert.assertFalse(client.ping(server, 30000), "Ping should fail due to incomplete proxy options");
//                Assert.assertThrows(ClickHouseException.class,
//                        () -> client.read(server).query("select 1").executeAndWait());
//            }
//
//            options.put("proxy_port", Integer.toString(proxyPort));
//            try (ClickHouseClient client = ClickHouseClient.builder().options(getClientOptions())
//                    .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP)).build()) {
//                ClickHouseNode server = getServer(ClickHouseProtocol.HTTP, options);
//                Assert.assertTrue(client.ping(server, 30000), "Can not ping via proxy");
//                Assert.assertEquals(
//                        client.read(server).query("select 6").executeAndWait().firstRecord().getValue(0).asString(),
//                        "6");
//            }
        } finally {
            if (toxiproxy != null) {
                toxiproxy.stop();
            }
        }
    }
    @Test(groups = "integration")
    public void testDecompressWithLargeChunk() throws ClickHouseException, IOException, ExecutionException, InterruptedException {
        ClickHouseNode server = getServer();

        String tableName = "test_decompress_with_large_chunk";

        String tableColumns = String.format("id Int64, raw String");
        sendAndWait(server, "drop table if exists " + tableName,
                "create table " + tableName + " (" + tableColumns + ")engine=Memory");

        long numRows = 1;
        String content = StringUtils.repeat("*", 50000);
        try {
            try (ClickHouseClient client = getClient()) {
                ClickHouseRequest.Mutation request = client.read(server)
                        .write()
                        .table(tableName)
                        .decompressClientRequest(true)
                        //.option(ClickHouseClientOption.USE_BLOCKING_QUEUE, "true")
                        .format(ClickHouseFormat.RowBinary);
                ClickHouseConfig config = request.getConfig();
                CompletableFuture<ClickHouseResponse> future;

                try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                        .createPipedOutputStream(config)) {
                    // start the worker thread which transfer data from the input into ClickHouse
                    future = request.data(stream.getInputStream()).execute();
                    // write bytes into the piped stream
                    LongStream.range(0, numRows).forEachOrdered(
                            n -> {
                                try {
                                    BinaryStreamUtils.writeInt64(stream, n);
                                    BinaryStreamUtils.writeString(stream, content);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    );

                    // We need to close the stream before getting a response
                    stream.close();
                    try (ClickHouseResponse response = future.get()) {
                        ClickHouseResponseSummary summary = response.getSummary();
                        Assert.assertEquals(summary.getWrittenRows(), numRows, "Num of written rows");
                    }
                }

            }
        } catch (Exception e) {
            Throwable th = e.getCause();
//            if (th instanceof ClickHouseException) {
//                ClickHouseException ce = (ClickHouseException) th;
//                Assert.assertEquals(73, ce.getErrorCode(), "It's Code: 73. DB::Exception: Unknown format RowBinaryWithDefaults. a server that not support the format");
//            } else {
            Assert.assertTrue(false, e.getMessage());
//            }
        }

    }

    @Test(groups = {"integration"})
    public void testLongHttpHeaderReferer() throws ClickHouseException {
        super.testMutation();

        StringBuilder referer = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            referer.append(i);
        }
        ClickHouseNode server = getServer();
        try (ClickHouseClient client = getClient();
             ClickHouseResponse response = newRequest(client, server)
                     .option(ClickHouseHttpOption.CUSTOM_HEADERS, "Referer=" + referer)
                     .set("send_progress_in_http_headers", 1)
                     .query("select 1")
                     .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            Assert.assertEquals(summary.getReadRows(), 1L);
        }
    }

    @Test(groups = {"integration"})
    public void testReadingBinaryFromRespose() throws Exception {
        final ClickHouseNode server = getServer();
        String tableName = "test_protobuf_format";
        String tableColumns = String.format("id Int64, raw String");
        sendAndWait(server, "drop table if exists " + tableName,
                "create table " + tableName + " (" + tableColumns + ") engine=MergeTree order by tuple()");

        try (ClickHouseClient client = getClient()) {
            ClickHouseResponse response = client.read(server).query("select structureToProtobufSchema ('column1 String, column2 UInt32')")
                    .format(ClickHouseFormat.RawBLOB)
                    .executeAndWait();

            try (InputStream responseBody = response.getInputStream()) {
                byte[] buffer = new byte[responseBody.available()];
                Assert.assertTrue(responseBody.read(buffer) > 0);
                String protoSchema = new String(buffer, StandardCharsets.UTF_8);
                Assert.assertEquals(protoSchema, "syntax = \"proto3\";\n" +
                        "\n" +
                        "message Message\n" +
                        "{\n" +
                        "    bytes column1 = 1;\n" +
                        "    uint32 column2 = 2;\n" +
                        "}");

            } finally {
                response.close();
            }
        }

    }
}
