package com.clickhouse.jdbc;

import java.util.Arrays;

import com.clickhouse.client.ClickHouseConfig;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JdbcParameterizedQueryTest {
    private final ClickHouseConfig config = new ClickHouseConfig();
    @BeforeMethod(groups = "unit")
    public void setV1() {
        System.setProperty("clickhouse.jdbc.v1","true");
    }

    @Test(groups = "unit")
    public void testParseBlankQueries() {
        Assert.assertThrows(IllegalArgumentException.class, () -> JdbcParameterizedQuery.of(config, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> JdbcParameterizedQuery.of(config, ""));
        Assert.assertThrows(IllegalArgumentException.class, () -> JdbcParameterizedQuery.of(config, " \n\t\r"));
    }

    @Test(groups = "unit")
    public void testParseQueriesWithNamedParameter() {
        String sql = "select :no, :name(String)";
        JdbcParameterizedQuery q = JdbcParameterizedQuery.of(config, sql);
        Assert.assertEquals(q.getOriginalQuery(), sql);
        Assert.assertEquals(q.hasParameter(), false);
    }

    @Test(groups = "unit")
    public void testParseJdbcQueries() {
        StringBuilder builder = new StringBuilder();
        String sql = "select ?(number % 2 == 0 ? 1 : 0) from numbers(100) where number > ?";
        JdbcParameterizedQuery q = JdbcParameterizedQuery.of(config, sql);
        Assert.assertEquals(q.getOriginalQuery(), sql);
        Assert.assertEquals(q.hasParameter(), true);
        Assert.assertEquals(q.getParameters(), Arrays.asList("0", "1"));
        builder.setLength(0);
        q.apply(builder, "sum", "1");
        Assert.assertEquals(builder.toString(),
                "select sum(number % 2 == 0 ? 1 : 0) from numbers(100) where number > 1");

        Assert.assertEquals(JdbcParameterizedQuery.of(config, "select '; select 2' as ?").hasParameter(), true);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> JdbcParameterizedQuery.of(config, "select 1; select 2"));

        sql = "select 1 ? 'a' : 'b', 2 ? (select 1) : 2, ?";
        q = JdbcParameterizedQuery.of(config, sql);
        Assert.assertEquals(q.getOriginalQuery(), sql);
        Assert.assertEquals(q.hasParameter(), true);
        Assert.assertEquals(q.getParameters(), Arrays.asList("0"));
        builder.setLength(0);
        q.apply(builder, "3");
        Assert.assertEquals(builder.toString(), "select 1 ? 'a' : 'b', 2 ? (select 1) : 2, 3");

        sql = "select ?::?";
        q = JdbcParameterizedQuery.of(config, sql);
        Assert.assertEquals(q.getOriginalQuery(), sql);
        Assert.assertEquals(q.hasParameter(), true);
        Assert.assertEquals(q.getParameters(), Arrays.asList("0", "1"));
        builder.setLength(0);
        q.apply(builder, 1, new StringBuilder("Int8"));
        Assert.assertEquals(builder.toString(), "select 1::Int8");
    }
}
