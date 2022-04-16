package com.clickhouse.client;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.client.ClickHouseRequest.Mutation;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseConfigChangeListener;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.data.ClickHouseBigIntegerValue;
import com.clickhouse.client.data.ClickHouseByteValue;
import com.clickhouse.client.data.ClickHouseDateTimeValue;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.data.ClickHouseFloatValue;
import com.clickhouse.client.data.ClickHouseIntegerValue;
import com.clickhouse.client.data.ClickHouseStringValue;

public class ClickHouseRequestTest {
    @Test(groups = { "unit" })
    public void testBuild() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(ClickHouseNode.builder().build());
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
    public void testChangeListener() {
        final List<Object[]> changedOptions = new ArrayList<>();
        // final List<Object[]> changedProperties = new ArrayList<>();
        final List<Object[]> changedSettings = new ArrayList<>();
        ClickHouseConfigChangeListener<ClickHouseRequest<?>> listener = new ClickHouseConfigChangeListener<ClickHouseRequest<?>>() {
            @Override
            public void optionChanged(ClickHouseRequest<?> source, ClickHouseOption option, Serializable oldValue,
                    Serializable newValue) {
                changedOptions.add(new Object[] { source, option, oldValue, newValue });
            }

            @Override
            public void settingChanged(ClickHouseRequest<?> source, String setting, Serializable oldValue,
                    Serializable newValue) {
                changedSettings.add(new Object[] { source, setting, oldValue, newValue });
            }
        };
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(ClickHouseNode.builder().build());
        request.setChangeListener(listener);
        Assert.assertTrue(changedOptions.isEmpty(), "Should have no option changed");
        Assert.assertTrue(changedSettings.isEmpty(), "Should have no setting changed");
        request.option(ClickHouseClientOption.ASYNC, false);
        request.format(ClickHouseFormat.Arrow);
        request.option(ClickHouseClientOption.FORMAT, ClickHouseFormat.Avro);
        request.removeOption(ClickHouseClientOption.BUFFER_SIZE);
        request.removeOption(ClickHouseClientOption.ASYNC);
        request.reset();
        request.format(ClickHouseFormat.TSV);
        Assert.assertEquals(changedOptions.toArray(new Object[0]),
                new Object[][] {
                        new Object[] { request, ClickHouseClientOption.ASYNC, null, false },
                        new Object[] { request, ClickHouseClientOption.FORMAT, null, ClickHouseFormat.Arrow },
                        new Object[] { request, ClickHouseClientOption.FORMAT, ClickHouseFormat.Arrow,
                                ClickHouseFormat.Avro },
                        new Object[] { request, ClickHouseClientOption.ASYNC, false, null },
                        new Object[] { request, ClickHouseClientOption.FORMAT, ClickHouseFormat.Avro, null } });
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
        Assert.assertNull(request.copy().changeListener, "Listener should never be copied");
        Assert.assertNull(request.seal().changeListener, "Listener should never be copied");
    }

    @Test(groups = { "unit" })
    public void testCopy() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(ClickHouseNode.builder().build());
        request.compressServerResponse(true, ClickHouseCompression.BROTLI, 2);
        request.decompressClientRequest(true, ClickHouseCompression.ZSTD, 5);
        request.external(ClickHouseExternalTable.builder().content(new ByteArrayInputStream(new byte[0])).build());
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
        Assert.assertEquals(copy.sessionId, request.sessionId);
        Assert.assertEquals(copy.sql, request.sql);
        Assert.assertEquals(copy.getPreparedQuery(), request.getPreparedQuery());

        copy = copy.write();
        Assert.assertFalse(copy.isSealed(), "Should NOT be sealed");
        Assert.assertFalse(copy == request, "Should be two different instances");
        Assert.assertTrue(copy.namedParameters.isEmpty(), "Named parameters should be empty");
        Assert.assertEquals(copy.options, request.options);
        Assert.assertNull(copy.queryId, "Query ID should be null");
        Assert.assertEquals(copy.sessionId, request.sessionId);
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
        Assert.assertEquals(copy.sessionId, request.sessionId);
        Assert.assertEquals(copy.sql, request.sql);
        Assert.assertEquals(copy.getPreparedQuery(), request.getPreparedQuery());
    }

    @Test(groups = { "unit" })
    public void testFormat() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(ClickHouseNode.builder().build());
        Assert.assertEquals(request.getFormat(),
                (ClickHouseFormat) ClickHouseDefaults.FORMAT.getEffectiveDefaultValue());
        request.format(ClickHouseFormat.ArrowStream);
        Assert.assertEquals(request.getFormat(), ClickHouseFormat.ArrowStream);
        Assert.assertEquals(request.getInputFormat(), ClickHouseFormat.ArrowStream);
        request.format(null);
        Assert.assertEquals(request.getFormat(),
                (ClickHouseFormat) ClickHouseDefaults.FORMAT.getEffectiveDefaultValue());
        Assert.assertEquals(request.getInputFormat(),
                ((ClickHouseFormat) ClickHouseDefaults.FORMAT.getEffectiveDefaultValue()).defaultInputFormat());
        request.format(ClickHouseFormat.Arrow);
        Assert.assertEquals(request.getFormat(), ClickHouseFormat.Arrow);
        Assert.assertEquals(request.getInputFormat(), ClickHouseFormat.Arrow);
    }

    @Test(groups = { "unit" })
    public void testOptions() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(ClickHouseNode.builder().build());

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
    }

    @Test(groups = { "unit" })
    public void testParams() {
        String sql = "select :one as one, :two as two, * from my_table where key=:key and arr[:idx] in numbers(:range)";
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(ClickHouseNode.builder().build())
                .query(sql);
        Assert.assertEquals(request.getQuery(), sql);
        request.params(ClickHouseByteValue.of(Byte.MIN_VALUE));
        Assert.assertEquals(request.getQuery(), sql);
        Assert.assertEquals(request.getStatements(false).size(), 1);
        Assert.assertEquals(request.getStatements(false).get(0),
                "select -128 as one, NULL as two, * from my_table where key=NULL and arr[NULL] in numbers(NULL)");

        request.params(ClickHouseStringValue.of(""),
                ClickHouseDateTimeValue.of("2012-12-12 12:23:34.56789", 2, ClickHouseValues.UTC_TIMEZONE),
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
        params.put("two", ClickHouseDateTimeValue.of("2012-12-12 12:23:34.56789", 2, ClickHouseValues.UTC_TIMEZONE)
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
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(ClickHouseNode.builder().build());
        request.compressServerResponse(true, ClickHouseCompression.BROTLI, 2);
        request.decompressClientRequest(true, ClickHouseCompression.ZSTD, 5);
        request.external(ClickHouseExternalTable.builder().content(new ByteArrayInputStream(new byte[0])).build());
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
        Assert.assertEquals(sealed.sessionId, request.sessionId);
        Assert.assertEquals(sealed.sql, request.sql);
        Assert.assertEquals(sealed.getPreparedQuery(), request.getPreparedQuery());

        Assert.assertThrows(IllegalStateException.class, () -> sealed.write());
    }

    @Test(groups = { "unit" })
    public void testSession() {
        String sessionId = UUID.randomUUID().toString();
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(ClickHouseNode.builder().build());
        Assert.assertEquals(request.getSessionId().isPresent(), false);
        Assert.assertEquals(request.sessionId, null);
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
        Assert.assertEquals(request.sessionId, null);
        Assert.assertEquals(request.getConfig().isSessionCheck(), false);
        Assert.assertEquals(request.getConfig().getSessionTimeout(), 0);
    }

    @Test(groups = { "unit" })
    public void testSettings() {
        ClickHouseRequest<?> request = ClickHouseClient.newInstance().connect(ClickHouseNode.builder().build());
        Assert.assertEquals(request.getStatements().size(), 0);
        request.set("enable_optimize_predicate_expression", 1);
        Assert.assertEquals(request.getStatements().size(), 1);
        Assert.assertEquals(request.getStatements().get(0), "SET enable_optimize_predicate_expression=1");
        request.set("log_queries_min_type", "EXCEPTION_WHILE_PROCESSING");
        Assert.assertEquals(request.getStatements().size(), 2);
        Assert.assertEquals(request.getStatements().get(1), "SET log_queries_min_type='EXCEPTION_WHILE_PROCESSING'");
    }

    @Test(groups = { "unit" })
    public void testMutation() {
        Mutation request = ClickHouseClient.newInstance().connect(ClickHouseNode.builder().build()).write();
        request.table("test_table").format(ClickHouseFormat.Arrow).data(new ByteArrayInputStream(new byte[0]));

        String expectedSql = "INSERT INTO test_table FORMAT Arrow";
        Assert.assertEquals(request.getQuery(), expectedSql);
        Assert.assertEquals(request.getStatements().get(0), expectedSql);

        request = request.seal();
        Assert.assertEquals(request.getQuery(), expectedSql);
        Assert.assertEquals(request.getStatements().get(0), expectedSql);
    }
}
