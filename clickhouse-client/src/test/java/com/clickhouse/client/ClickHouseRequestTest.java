package com.clickhouse.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.client.ClickHouseRequest.Mutation;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.config.ClickHouseConfigChangeListener;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.value.ClickHouseBigIntegerValue;
import com.clickhouse.data.value.ClickHouseByteValue;
import com.clickhouse.data.value.ClickHouseDateTimeValue;
import com.clickhouse.data.value.ClickHouseFloatValue;
import com.clickhouse.data.value.ClickHouseIntegerValue;
import com.clickhouse.data.value.ClickHouseStringValue;

public class ClickHouseRequestTest {
    @Test(groups = { "unit" })
    public void testBuild() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());
        Assert.assertNotNull(request);

        ClickHouseConfig config = request.getConfig();
        List<String> stmts = request.getStatements();
        Assert.assertEquals(config, request.getConfig());
        Assert.assertEquals(stmts, request.getStatements());
        Assert.assertEquals(stmts.size(), 0);

        String db = "db";
        String table = "test";
        String sql = "select 1";

        request.table(table);
        Assert.assertEquals(config, request.getConfig());
        Assert.assertNotEquals(stmts, request.getStatements());
        Assert.assertEquals(request.getStatements().size(), 1);
        Assert.assertEquals(request.getStatements().get(0), "SELECT * FROM " + table);

        request.query(sql);
        Assert.assertEquals(config, request.getConfig());
        Assert.assertEquals(request.getStatements().get(0), sql);

        request.use(db);
        Assert.assertNotEquals(config, request.getConfig()); // because new option being added
        Assert.assertEquals(request.getConfig().getDatabase(), db);
        Assert.assertEquals(request.getStatements().size(), 1);
        Assert.assertEquals(request.getStatements().get(0), sql);

        Mutation m = request.write();
        Assert.assertEquals(m.getConfig().getDatabase(), db);
        Assert.assertEquals(m.getStatements().size(), 0);

        m.removeOption(ClickHouseClientOption.DATABASE).table(table);
        Assert.assertEquals(m.getStatements().size(), 1);
        Assert.assertEquals(m.getStatements().get(0), "INSERT INTO " + table);

        m.query(sql = "delete from test where id = 1");
        Assert.assertEquals(m.getStatements().size(), 1);
        Assert.assertEquals(m.getStatements().get(0), sql);
    }

    @Test(groups = { "unit" })
    public void testCredentials() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());
        Assert.assertNotNull(request.getConfig().getDefaultCredentials());
        Assert.assertEquals(request.getConfig().getDefaultCredentials().getUserName(),
                ClickHouseDefaults.USER.getDefaultValue());
        Assert.assertEquals(request.getConfig().getDefaultCredentials().getPassword(),
                ClickHouseDefaults.PASSWORD.getDefaultValue());

        final String user = "somebody";
        final String password = "seCrets";
        request = ClickHouseClient.newInstance().read(ClickHouseNode.builder()
                .credentials(ClickHouseCredentials.fromUserAndPassword(user, password)).build());
        Assert.assertNotNull(request.getConfig().getDefaultCredentials());
        Assert.assertEquals(request.getConfig().getDefaultCredentials().getUserName(), user);
        Assert.assertEquals(request.getConfig().getDefaultCredentials().getPassword(), password);

        request = ClickHouseClient.newInstance()
                .read(ClickHouseNode.of("tcp://localhost/default?user=" + user + "&password=" + password));
        Assert.assertNotNull(request.getConfig().getDefaultCredentials());
        Assert.assertEquals(request.getConfig().getDefaultCredentials().getUserName(), user);
        Assert.assertEquals(request.getConfig().getDefaultCredentials().getPassword(), password);
    }

    @Test(groups = { "unit" })
    public void testConfigChangeListener() {
        final ClickHouseConfig config = new ClickHouseConfig();
        final List<Object[]> changedOptions = new ArrayList<>();
        final List<Object[]> changedProperties = new ArrayList<>();
        final List<Object[]> changedSettings = new ArrayList<>();
        ClickHouseConfigChangeListener<ClickHouseRequest<?>> listener = new ClickHouseConfigChangeListener<ClickHouseRequest<?>>() {
            @Override
            public void optionChanged(ClickHouseRequest<?> source, ClickHouseOption option,
                    Serializable oldValue, Serializable newValue) {
                changedOptions.add(new Object[] { source, option, oldValue, newValue });
            }

            @Override
            public void propertyChanged(ClickHouseRequest<?> source, String property, Object oldValue,
                    Object newValue) {
                changedProperties.add(new Object[] { source, property, oldValue, newValue });
            }

            @Override
            public void settingChanged(ClickHouseRequest<?> source, String setting, Serializable oldValue,
                    Serializable newValue) {
                changedSettings.add(new Object[] { source, setting, oldValue, newValue });
            }
        };
        final ClickHouseParameterizedQuery select3 = ClickHouseParameterizedQuery.of(config, "select 3");
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());
        request.setChangeListener(listener);
        Assert.assertTrue(changedOptions.isEmpty(), "Should have no option changed");
        Assert.assertTrue(changedProperties.isEmpty(), "Should have no property changed");
        Assert.assertTrue(changedSettings.isEmpty(), "Should have no setting changed");
        request.option(ClickHouseClientOption.ASYNC, false);
        request.format(ClickHouseFormat.Arrow);
        request.option(ClickHouseClientOption.FORMAT, ClickHouseFormat.Avro);
        request.removeOption(ClickHouseClientOption.BUFFER_SIZE);
        request.removeOption(ClickHouseClientOption.ASYNC);
        request.query("select 1");
        request.query("select 2", "id=2");
        request.query(select3);
        request.reset();
        request.format(ClickHouseFormat.TSV);
        Assert.assertEquals(changedOptions.toArray(new Object[0]),
                new Object[][] {
                        new Object[] { request, ClickHouseClientOption.ASYNC, null, false },
                        new Object[] { request, ClickHouseClientOption.FORMAT, null,
                                ClickHouseFormat.Arrow },
                        new Object[] { request, ClickHouseClientOption.FORMAT,
                                ClickHouseFormat.Arrow,
                                ClickHouseFormat.Avro },
                        new Object[] { request, ClickHouseClientOption.ASYNC, false, null },
                        new Object[] { request, ClickHouseClientOption.FORMAT,
                                ClickHouseFormat.Avro, null } });
        Assert.assertEquals(changedProperties.toArray(new Object[0]), new Object[][] {
                { request, ClickHouseRequest.PROP_QUERY, null, "select 1" },
                { request, ClickHouseRequest.PROP_QUERY, "select 1", "select 2" },
                { request, ClickHouseRequest.PROP_QUERY_ID, null, "id=2" },
                { request, ClickHouseRequest.PROP_PREPARED_QUERY, null, select3 },
                { request, ClickHouseRequest.PROP_QUERY, "select 2", "select 3" },
                { request, ClickHouseRequest.PROP_QUERY_ID, "id=2", null },
                { request, ClickHouseRequest.PROP_QUERY, "select 3", null },
                { request, ClickHouseRequest.PROP_PREPARED_QUERY, select3, null },
        });
        changedOptions.clear();

        request.setChangeListener(listener);
        request.set("a", 1);
        request.set("b", "2");
        request.set("b", 3);
        request.removeSetting("c");
        request.removeSetting("a");
        request.reset();
        request.set("a", 2);
        Assert.assertEquals(changedSettings.toArray(new Object[0]),
                new Object[][] {
                        new Object[] { request, "a", null, 1 },
                        new Object[] { request, "b", null, "2" },
                        new Object[] { request, "b", "2", 3 },
                        new Object[] { request, "a", 1, null },
                        new Object[] { request, "b", 3, null } });
        changedSettings.clear();

        request.setChangeListener(listener);
        Assert.assertEquals(request.copy().changeListener, request.changeListener);
        Assert.assertNull(request.seal().changeListener, "Listener should never be copied");
    }

    @Test(groups = { "unit" })
    public void testServerListener() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());
        final List<Object[]> serverChanges = new ArrayList<>();
        request.setServerListener(
                (currentServer, newServer) -> serverChanges
                        .add(new Object[] { currentServer, newServer }));
        ClickHouseNode s11 = ClickHouseNode.of("http://node1");
        ClickHouseNode s12 = ClickHouseNode.of("grpc://node1/system");
        ClickHouseNode s21 = ClickHouseNode.of("tcp://node2");
        ClickHouseNode s22 = ClickHouseNode.of("https://node2");
        request.changeServer(request.getServer(), s11);
        Assert.assertEquals(serverChanges.toArray(new Object[0]),
                new Object[][] { { ClickHouseNode.DEFAULT, s11 } });
        request.changeServer(ClickHouseNode.DEFAULT, s12);
        Assert.assertEquals(serverChanges.toArray(new Object[0]),
                new Object[][] { { ClickHouseNode.DEFAULT, s11 } });
        request.changeServer(s11, s21);
        Assert.assertEquals(serverChanges.toArray(new Object[0]),
                new Object[][] { { ClickHouseNode.DEFAULT, s11 }, { s11, s21 } });
        request.reset();
        request.changeServer(s21, s22);
        Assert.assertEquals(serverChanges.toArray(new Object[0]),
                new Object[][] { { ClickHouseNode.DEFAULT, s11 }, { s11, s21 } });
    }

    @Test(groups = { "unit" })
    public void testCopy() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());
        request.compressServerResponse(true, ClickHouseCompression.BROTLI, 2);
        request.decompressClientRequest(true, ClickHouseCompression.ZSTD, 5);
        request.external(ClickHouseExternalTable.builder().content(new ByteArrayInputStream(new byte[0]))
                .build());
        request.format(ClickHouseFormat.Avro);
        request.table("table1", "query_id1");
        request.query("select :a", UUID.randomUUID().toString());
        request.params("a");
        request.session(UUID.randomUUID().toString(), true, 120);
        request.set("key", "value");
        request.use("db1");

        ClickHouseRequest<?> copy = request.copy();
        Assert.assertFalse(copy.isSealed(), "Should NOT be sealed");
        Assert.assertFalse(copy == request, "Should be two different instances");
        Assert.assertEquals(copy.namedParameters, request.namedParameters);
        Assert.assertEquals(copy.options, request.options);
        Assert.assertEquals(copy.queryId, request.queryId);
        Assert.assertEquals(copy.getSessionId(), request.getSessionId());
        Assert.assertEquals(copy.sql, request.sql);
        Assert.assertEquals(copy.getPreparedQuery(), request.getPreparedQuery());

        copy = copy.write();
        Assert.assertFalse(copy.isSealed(), "Should NOT be sealed");
        Assert.assertFalse(copy == request, "Should be two different instances");
        Assert.assertTrue(copy.namedParameters.isEmpty(), "Named parameters should be empty");
        Assert.assertEquals(copy.options, request.options);
        Assert.assertNull(copy.queryId, "Query ID should be null");
        Assert.assertEquals(copy.getSessionId(), request.getSessionId());
        Assert.assertNull(copy.sql, "SQL should be null");

        ClickHouseRequest<?> newCopy = copy;
        Assert.assertThrows(IllegalArgumentException.class, () -> newCopy.getPreparedQuery());

        copy.external(ClickHouseExternalTable.builder().content(new ByteArrayInputStream(new byte[0])).build());
        copy.table("table1", "query_id1");
        copy.query("select :a", request.queryId);
        copy.params("a");

        Assert.assertFalse(copy.isSealed(), "Should NOT be sealed");
        Assert.assertFalse(copy == request, "Should be two different instances");
        Assert.assertEquals(copy.namedParameters, request.namedParameters);
        Assert.assertEquals(copy.options, request.options);
        Assert.assertEquals(copy.queryId, request.queryId);
        Assert.assertEquals(copy.getSessionId(), request.getSessionId());
        Assert.assertEquals(copy.sql, request.sql);
        Assert.assertEquals(copy.getPreparedQuery(), request.getPreparedQuery());
    }

    @Test(groups = { "unit" })
    public void testFormat() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());
        Assert.assertEquals(request.getFormat(),
                (ClickHouseFormat) ClickHouseDefaults.FORMAT.getEffectiveDefaultValue());
        request.format(ClickHouseFormat.TabSeparatedRawWithNamesAndTypes);
        Assert.assertEquals(request.getFormat(), ClickHouseFormat.TabSeparatedRawWithNamesAndTypes);
        Assert.assertEquals(request.getFormat().defaultInputFormat(), ClickHouseFormat.TabSeparatedRaw);
        request.format(ClickHouseFormat.ArrowStream);
        Assert.assertEquals(request.getFormat(), ClickHouseFormat.ArrowStream);
        Assert.assertEquals(request.getFormat().defaultInputFormat(), ClickHouseFormat.ArrowStream);
        request.format(null);
        Assert.assertEquals(request.getFormat(),
                (ClickHouseFormat) ClickHouseDefaults.FORMAT.getEffectiveDefaultValue());
        Assert.assertEquals(request.getFormat().defaultInputFormat(),
                ((ClickHouseFormat) ClickHouseDefaults.FORMAT.getEffectiveDefaultValue())
                        .defaultInputFormat());
        request.format(ClickHouseFormat.Arrow);
        Assert.assertEquals(request.getFormat(), ClickHouseFormat.Arrow);
        Assert.assertEquals(request.getFormat().defaultInputFormat(), ClickHouseFormat.Arrow);
    }

    @Test(groups = { "unit" })
    public void testGetSetting() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance()
                .read("http://localhost?custom_settings=a%3D1%2Cb%3D2");
        Assert.assertEquals(request.getSetting("a", boolean.class), true);
        Assert.assertEquals(request.getSetting("a", Boolean.class), true);
        Assert.assertEquals(request.getSetting("a", false), true);
        Assert.assertEquals(request.getSetting("a", int.class), 1);
        Assert.assertEquals(request.getSetting("a", Integer.class), 1);
        Assert.assertEquals(request.getSetting("a", 9), 1);
        Assert.assertEquals(request.getSetting("b", "3"), "2");
        // request.settings(null);
        request.clearSettings();
        Assert.assertTrue(request.getSettings().isEmpty());
        Assert.assertEquals(request.getSetting("b", 9), 9);
    }

    @Test(groups = { "unit" })
    public void testInputData() throws IOException {
        Mutation request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build()).write();
        Assert.assertEquals(request.getConfig().getFormat(), ClickHouseDataConfig.DEFAULT_FORMAT);
        Assert.assertEquals(request.getConfig().getRequestCompressAlgorithm(), ClickHouseCompression.NONE);
        Assert.assertEquals(request.getConfig().getRequestCompressLevel(),
                ClickHouseDataConfig.DEFAULT_WRITE_COMPRESS_LEVEL);
        Assert.assertEquals(request.getConfig().getResponseCompressAlgorithm(), ClickHouseCompression.LZ4);
        Assert.assertEquals(request.getConfig().getResponseCompressLevel(),
                ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL);
        Assert.assertFalse(request.hasInputStream());

        request.data("/non-existing-file/" + UUID.randomUUID().toString()); // unrecognized file
        Assert.assertEquals(request.getConfig().getFormat(), ClickHouseDataConfig.DEFAULT_FORMAT);
        Assert.assertEquals(request.getConfig().getRequestCompressAlgorithm(), ClickHouseCompression.NONE);
        Assert.assertEquals(request.getConfig().getRequestCompressLevel(),
                ClickHouseDataConfig.DEFAULT_WRITE_COMPRESS_LEVEL);
        Assert.assertEquals(request.getConfig().getResponseCompressAlgorithm(), ClickHouseCompression.LZ4);
        Assert.assertEquals(request.getConfig().getResponseCompressLevel(),
                ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL);
        Assert.assertTrue(request.hasInputStream());

        Assert.assertThrows(IllegalArgumentException.class,
                () -> request.data("/non-existing-file/" + UUID.randomUUID().toString() + ".csv.gz"));

        File tmp = ClickHouseUtils.createTempFile(null, ".csv.gz");
        request.data(tmp.getAbsolutePath());
        Assert.assertEquals(request.getConfig().getFormat(), ClickHouseFormat.CSV);
        Assert.assertEquals(request.getConfig().getRequestCompressAlgorithm(), ClickHouseCompression.GZIP);
        Assert.assertEquals(request.getConfig().getRequestCompressLevel(),
                ClickHouseDataConfig.DEFAULT_WRITE_COMPRESS_LEVEL);
        Assert.assertEquals(request.getConfig().getResponseCompressAlgorithm(), ClickHouseCompression.LZ4);
        Assert.assertEquals(request.getConfig().getResponseCompressLevel(),
                ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL);
        Assert.assertTrue(request.hasInputStream());

        request.data(ClickHousePassThruStream.of(ClickHouseInputStream.empty(), ClickHouseCompression.BROTLI, 2,
                ClickHouseFormat.Arrow));
        Assert.assertEquals(request.getConfig().getFormat(), ClickHouseFormat.Arrow);
        Assert.assertEquals(request.getConfig().getRequestCompressAlgorithm(), ClickHouseCompression.BROTLI);
        Assert.assertEquals(request.getConfig().getRequestCompressLevel(), 2);
        Assert.assertEquals(request.getConfig().getResponseCompressAlgorithm(), ClickHouseCompression.LZ4);
        Assert.assertEquals(request.getConfig().getResponseCompressLevel(),
                ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL);
        Assert.assertTrue(request.hasInputStream());

        request.data(new FileInputStream(tmp));
        Assert.assertEquals(request.getConfig().getFormat(), ClickHouseFormat.Arrow);
        Assert.assertEquals(request.getConfig().getRequestCompressAlgorithm(), ClickHouseCompression.BROTLI);
        Assert.assertEquals(request.getConfig().getRequestCompressLevel(), 2);
        Assert.assertEquals(request.getConfig().getResponseCompressAlgorithm(), ClickHouseCompression.LZ4);
        Assert.assertEquals(request.getConfig().getResponseCompressLevel(),
                ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL);
        Assert.assertTrue(request.hasInputStream());

        request.data(ClickHousePassThruStream.of(ClickHouseInputStream.empty(), ClickHouseCompression.XZ, 3,
                ClickHouseFormat.ArrowStream).newInputStream(64, null));
        Assert.assertEquals(request.getConfig().getFormat(), ClickHouseFormat.ArrowStream);
        Assert.assertEquals(request.getConfig().getRequestCompressAlgorithm(), ClickHouseCompression.XZ);
        Assert.assertEquals(request.getConfig().getRequestCompressLevel(), 3);
        Assert.assertEquals(request.getConfig().getResponseCompressAlgorithm(), ClickHouseCompression.LZ4);
        Assert.assertEquals(request.getConfig().getResponseCompressLevel(),
                ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL);
        Assert.assertTrue(request.hasInputStream());
    }

    @Test(groups = { "unit" })
    public void testInputStreamAndCustomWriter() throws IOException {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());
        Assert.assertFalse(request.hasInputStream());
        Assert.assertFalse(request.getInputStream().isPresent());
        Assert.assertFalse(request.getWriter().isPresent());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String uuid = UUID.randomUUID().toString();
        Mutation m = request.write();
        m.data(w -> w.write(uuid.getBytes(StandardCharsets.US_ASCII)));
        Assert.assertTrue(m.hasInputStream());
        try (ClickHouseOutputStream o = ClickHouseOutputStream.of(out)) {
            m.getWriter().get().write(o);
        }
        Assert.assertEquals(new String(out.toByteArray(), StandardCharsets.US_ASCII), uuid);
        out = new ByteArrayOutputStream();
        try (ClickHouseOutputStream o = ClickHouseOutputStream.of(out)) {
            m.getInputStream().get().pipe(o);
        }
        Assert.assertEquals(new String(out.toByteArray(), StandardCharsets.US_ASCII), uuid);

        m.reset();
        Assert.assertFalse(request.hasInputStream());
        Assert.assertFalse(request.getInputStream().isPresent());
        Assert.assertFalse(request.getWriter().isPresent());

        out = new ByteArrayOutputStream();
        m.data(new ByteArrayInputStream(uuid.getBytes(StandardCharsets.US_ASCII)));
        Assert.assertTrue(m.hasInputStream());
        try (ClickHouseOutputStream o = ClickHouseOutputStream.of(out)) {
            m.getWriter().get().write(o);
        }
        Assert.assertEquals(new String(out.toByteArray(), StandardCharsets.US_ASCII), uuid);
        // unlike ClickHouseWriter, InputStream cannot be reused
        m.data(new ByteArrayInputStream(uuid.getBytes(StandardCharsets.US_ASCII)));
        out = new ByteArrayOutputStream();
        try (ClickHouseOutputStream o = ClickHouseOutputStream.of(out)) {
            m.getInputStream().get().pipe(o);
        }
        Assert.assertEquals(new String(out.toByteArray(), StandardCharsets.US_ASCII), uuid);
    }

    @Test(groups = { "unit" })
    public void testNamedParameters() {
        // String sql = "select xxx from xxx settings max_execution_time =
        // :max_execution_time";
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());

        String sql = "select :a,:b,:a";
        request.query(sql).params("1", "2");
        Assert.assertEquals(request.getStatements(false).get(0), "select 1,2,1");
    }

    @Test(groups = { "unit" })
    public void testOptions() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());

        Assert.assertEquals(request.options, Collections.emptyMap());
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.ASYNC.getKey(), "false");
        props.setProperty(ClickHouseClientOption.DATABASE.getKey(), "mydb");
        props.setProperty(ClickHouseClientOption.CLIENT_NAME.getKey(), "new");
        props.setProperty(ClickHouseClientOption.FORMAT.getKey(), "CapnProto");
        request.options(props);

        Assert.assertEquals(request.options.size(), 4);
        Assert.assertEquals(request.options.get(ClickHouseClientOption.ASYNC), false);
        Assert.assertEquals(request.options.get(ClickHouseClientOption.DATABASE), "mydb");
        Assert.assertEquals(request.options.get(ClickHouseClientOption.CLIENT_NAME), "new");
        Assert.assertEquals(request.options.get(ClickHouseClientOption.FORMAT), ClickHouseFormat.CapnProto);

        request.freezeOptions().option(ClickHouseClientOption.ASYNC, true).removeOption(ClickHouseClientOption.FORMAT);
        Assert.assertEquals(request.options.get(ClickHouseClientOption.ASYNC), false);
        Assert.assertEquals(request.options.get(ClickHouseClientOption.FORMAT), ClickHouseFormat.CapnProto);
        request.unfreezeOptions().option(ClickHouseClientOption.ASYNC, true)
                .removeOption(ClickHouseClientOption.FORMAT);
        Assert.assertEquals(request.options.get(ClickHouseClientOption.ASYNC), true);
        Assert.assertEquals(request.options.get(ClickHouseClientOption.FORMAT), null);
    }

    @Test(groups = { "unit" })
    public void testParams() {
        String sql = "select :one as one, :two as two, * from my_table where key=:key and arr[:idx] in numbers(:range)";
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build())
                .query(sql);
        Assert.assertEquals(request.getQuery(), sql);
        request.params(ClickHouseByteValue.of(Byte.MIN_VALUE));
        Assert.assertEquals(request.getQuery(), sql);
        Assert.assertEquals(request.getStatements(false).size(), 1);
        Assert.assertEquals(request.getStatements(false).get(0),
                "select -128 as one, NULL as two, * from my_table where key=NULL and arr[NULL] in numbers(NULL)");

        request.params(ClickHouseStringValue.of(""),
                ClickHouseDateTimeValue.of("2012-12-12 12:23:34.56789", 2,
                        ClickHouseValues.UTC_TIMEZONE),
                ClickHouseStringValue.of("key"), ClickHouseIntegerValue.of(1),
                ClickHouseBigIntegerValue.of(BigInteger.TEN));
        Assert.assertEquals(request.getQuery(), sql);
        Assert.assertEquals(request.getStatements(false).size(), 1);
        Assert.assertEquals(request.getStatements(false).get(0),
                "select '' as one, '2012-12-12 12:23:34.56789' as two, * from my_table where key='key' and arr[1] in numbers(10)");

        Map<String, String> params = new HashMap<>();
        params.put("one", ClickHouseFloatValue.of(1.0F).toSqlExpression());
        request.params(params);
        Assert.assertEquals(request.getQuery(), sql);
        Assert.assertEquals(request.getStatements(false).size(), 1);
        Assert.assertEquals(request.getStatements(false).get(0),
                "select 1.0 as one, NULL as two, * from my_table where key=NULL and arr[NULL] in numbers(NULL)");

        params.put("one", ClickHouseStringValue.of("").toSqlExpression());
        params.put("two",
                ClickHouseDateTimeValue
                        .of("2012-12-12 12:23:34.56789", 2, ClickHouseValues.UTC_TIMEZONE)
                        .toSqlExpression());
        params.put("key", ClickHouseStringValue.of("key").toSqlExpression());
        params.put("some", ClickHouseBigIntegerValue.of(BigInteger.ONE).toSqlExpression());
        params.put("idx", ClickHouseIntegerValue.of(1).toSqlExpression());
        params.put("range", ClickHouseBigIntegerValue.of(BigInteger.TEN).toSqlExpression());
        request.params(params);
        Assert.assertEquals(request.getQuery(), sql);
        Assert.assertEquals(request.getStatements(false).size(), 1);
        Assert.assertEquals(request.getStatements(false).get(0),
                "select '' as one, '2012-12-12 12:23:34.56789' as two, * from my_table where key='key' and arr[1] in numbers(10)");
    }

    @Test(groups = { "unit" })
    public void testSeal() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());
        request.compressServerResponse(true, ClickHouseCompression.BROTLI, 2);
        request.decompressClientRequest(true, ClickHouseCompression.ZSTD, 5);
        request.external(ClickHouseExternalTable.builder().content(new ByteArrayInputStream(new byte[0]))
                .build());
        request.format(ClickHouseFormat.Avro);
        request.table("table1", "query_id1");
        request.query("select :a", UUID.randomUUID().toString());
        request.params("a");
        request.session(UUID.randomUUID().toString(), true, 120);
        request.set("key", "value");
        request.use("db1");

        ClickHouseRequest<?> sealed = request.seal();
        Assert.assertTrue(sealed.isSealed(), "Should be sealed");
        Assert.assertFalse(sealed == request, "Should be two different instances");
        Assert.assertEquals(sealed.namedParameters, request.namedParameters);
        Assert.assertEquals(sealed.options, request.options);
        Assert.assertEquals(sealed.queryId, request.queryId);
        Assert.assertEquals(sealed.getSessionId(), request.getSessionId());
        Assert.assertEquals(sealed.sql, request.sql);
        Assert.assertEquals(sealed.getPreparedQuery(), request.getPreparedQuery());

        Assert.assertThrows(IllegalStateException.class, () -> sealed.write());
    }

    @Test(groups = { "unit" })
    public void testSession() {
        String sessionId = UUID.randomUUID().toString();
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());
        Assert.assertEquals(request.getSessionId().isPresent(), false);
        Assert.assertEquals(request.getSessionId(), Optional.empty());
        Assert.assertEquals(request.getConfig().isSessionCheck(), false);
        Assert.assertEquals(request.getConfig().getSessionTimeout(), 0);

        request.session(sessionId, true, 10);
        Assert.assertEquals(request.getSessionId().get(), sessionId);
        Assert.assertEquals(request.getConfig().isSessionCheck(), true);
        Assert.assertEquals(request.getConfig().getSessionTimeout(), 10);

        ClickHouseRequest<?> sealedRequest = request.query("select 1").seal();
        Assert.assertEquals(sealedRequest.getSessionId().get(), sessionId);
        Assert.assertEquals(sealedRequest.getConfig().isSessionCheck(), true);
        Assert.assertEquals(sealedRequest.getConfig().getSessionTimeout(), 10);

        sealedRequest = request.query("select 2").seal();
        Assert.assertEquals(sealedRequest.getSessionId().get(), sessionId);
        Assert.assertEquals(sealedRequest.getConfig().isSessionCheck(), true);
        Assert.assertEquals(sealedRequest.getConfig().getSessionTimeout(), 10);

        request.query("select 3").clearSession();
        Assert.assertEquals(sealedRequest.getSessionId().get(), sessionId);
        Assert.assertEquals(sealedRequest.getConfig().isSessionCheck(), true);
        Assert.assertEquals(sealedRequest.getConfig().getSessionTimeout(), 10);
        Assert.assertEquals(request.getSessionId().isPresent(), false);
        Assert.assertEquals(request.getSessionId(), Optional.empty());
        Assert.assertEquals(request.getConfig().isSessionCheck(), false);
        Assert.assertEquals(request.getConfig().getSessionTimeout(), 0);

        request.clearSession();
        Assert.assertEquals(request.getSessionId(), Optional.empty());
        Assert.assertEquals(request.getConfig().isSessionCheck(), false);
        Assert.assertEquals(request.getConfig().getSessionTimeout(), 0);
        request.set("session_id", sessionId);
        request.set("session_check", true);
        request.set("session_timeout", "7");
        Assert.assertEquals(request.getSessionId().get(), sessionId);
        Assert.assertEquals(request.getConfig().isSessionCheck(), true);
        Assert.assertEquals(request.getConfig().getSessionTimeout(), 7);
        Assert.assertTrue(request.getSettings().isEmpty());
        request.removeSetting("session_id");
        Assert.assertEquals(request.getSessionId().get(), sessionId);

        request.clearSession();
        Assert.assertEquals(request.getSessionId(), Optional.empty());
        Assert.assertEquals(request.getConfig().isSessionCheck(), false);
        Assert.assertEquals(request.getConfig().getSessionTimeout(), 0);
        request.option(ClickHouseClientOption.SESSION_CHECK, true);
        request.option(ClickHouseClientOption.SESSION_TIMEOUT, 3);
        request.option(ClickHouseClientOption.CUSTOM_SETTINGS,
                "session_check=false,a=1,session_id=" + sessionId + ",session_timeout=5");
        Assert.assertEquals(request.getSessionId().get(), sessionId);
        Assert.assertEquals(request.getConfig().isSessionCheck(), false);
        Assert.assertEquals(request.getConfig().getSessionTimeout(), 5);
    }

    @Test(groups = { "unit" })
    public void testSettings() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build());
        Assert.assertEquals(request.getStatements().size(), 0);
        request.set("enable_optimize_predicate_expression", 1);
        Assert.assertEquals(request.getStatements().size(), 1);
        Assert.assertEquals(request.getStatements().get(0), "SET enable_optimize_predicate_expression=1");
        request.set("log_queries_min_type", "EXCEPTION_WHILE_PROCESSING");
        Assert.assertEquals(request.getStatements().size(), 2);
        Assert.assertEquals(request.getStatements().get(1),
                "SET log_queries_min_type='EXCEPTION_WHILE_PROCESSING'");

        request.freezeSettings().set("enable_optimize_predicate_expression", 2).removeSetting("log_queries_min_type");
        Assert.assertEquals(request.settings.get("enable_optimize_predicate_expression"), 1);
        Assert.assertEquals(request.settings.get("log_queries_min_type"), "EXCEPTION_WHILE_PROCESSING");
        request.unfreezeSettings().set("enable_optimize_predicate_expression", 2).removeSetting("log_queries_min_type");
        Assert.assertEquals(request.settings.get("enable_optimize_predicate_expression"), 2);
        Assert.assertEquals(request.settings.get("log_queries_min_type"), null);
    }

    @Test(groups = { "unit" })
    public void testMutation() {
        Mutation request = ClickHouseClient.newInstance().read(ClickHouseNode.builder().build()).write();
        request.table("test_table").format(ClickHouseFormat.Arrow).data(new ByteArrayInputStream(new byte[0]));

        String expectedSql = "INSERT INTO test_table\n FORMAT Arrow";
        Assert.assertEquals(request.getQuery(), expectedSql);
        Assert.assertEquals(request.getStatements().get(0), expectedSql);

        ClickHouseRequest<?> sealedRequest = request.seal();
        Assert.assertEquals(sealedRequest.getQuery(), expectedSql);
        Assert.assertEquals(sealedRequest.getStatements().get(0), expectedSql);

        request.query(expectedSql = "select 1 format CSV");
        Assert.assertEquals(request.getQuery(), expectedSql);
        Assert.assertEquals(request.getStatements().get(0), expectedSql);

        request.query(expectedSql = "select format tsv from table format CSV ");
        Assert.assertEquals(request.getQuery(), expectedSql);
        Assert.assertEquals(request.getStatements().get(0), expectedSql);

        request.query(expectedSql = "select 1 -- format CSV ");
        expectedSql += "\n FORMAT Arrow";
        Assert.assertEquals(request.getQuery(), expectedSql);
        Assert.assertEquals(request.getStatements().get(0), expectedSql);

        request.query(expectedSql = "select format CSV from table /* ccc */");
        expectedSql += "\n FORMAT Arrow";
        Assert.assertEquals(request.getQuery(), expectedSql);
        Assert.assertEquals(request.getStatements().get(0), expectedSql);

        request.query(expectedSql = "select /* format CSV */");
        expectedSql += "\n FORMAT Arrow";
        Assert.assertEquals(request.getQuery(), expectedSql);
        Assert.assertEquals(request.getStatements().get(0), expectedSql);

        request.query(expectedSql = "select 1 format CSV a");
        expectedSql += "\n FORMAT Arrow";
        Assert.assertEquals(request.getQuery(), expectedSql);
        Assert.assertEquals(request.getStatements().get(0), expectedSql);
    }
}
