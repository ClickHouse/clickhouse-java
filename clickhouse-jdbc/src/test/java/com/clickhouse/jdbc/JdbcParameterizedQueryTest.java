package com.clickhouse.jdbc;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

public class JdbcParameterizedQueryTest {
    @Test(groups = "unit")
    public void testParseBlankQueries() {
        Assert.assertThrows(IllegalArgumentException.class, () -> JdbcParameterizedQuery.of(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> JdbcParameterizedQuery.of(""));
        Assert.assertThrows(IllegalArgumentException.class, () -> JdbcParameterizedQuery.of(" \n\t\r"));
    }

    @Test(groups = "unit")
    public void testParseQueriesWithNamedParameter() {
        String sql = "select :no, :name(String)";
        JdbcParameterizedQuery q = JdbcParameterizedQuery.of(sql);
        Assert.assertEquals(q.getOriginalQuery(), sql);
        Assert.assertEquals(q.hasParameter(), false);
    }

    @Test(groups = "unit")
    public void testParseJdbcQueries() {
        String sql = "select ?(number % 2 == 0 ? 1 : 0) from numbers(100) where number > ?";
        JdbcParameterizedQuery q = JdbcParameterizedQuery.of(sql);
        Assert.assertEquals(q.getOriginalQuery(), sql);
        Assert.assertEquals(q.hasParameter(), true);
        Assert.assertEquals(q.getParameters(), Arrays.asList("0", "1"));
        Assert.assertEquals(q.apply("sum", "1"),
                "select sum(number % 2 == 0 ? 1 : 0) from numbers(100) where number > 1");

        Assert.assertEquals(JdbcParameterizedQuery.of("select '; select 2' as ?").hasParameter(), true);
        Assert.assertThrows(IllegalArgumentException.class, () -> JdbcParameterizedQuery.of("select 1; select 2"));

        sql = "select 1 ? 'a' : 'b', 2 ? (select 1) : 2, ?";
        q = JdbcParameterizedQuery.of(sql);
        Assert.assertEquals(q.getOriginalQuery(), sql);
        Assert.assertEquals(q.hasParameter(), true);
        Assert.assertEquals(q.getParameters(), Arrays.asList("0"));
        Assert.assertEquals(q.apply("3"), "select 1 ? 'a' : 'b', 2 ? (select 1) : 2, 3");

        sql = "select ?::?";
        q = JdbcParameterizedQuery.of(sql);
        Assert.assertEquals(q.getOriginalQuery(), sql);
        Assert.assertEquals(q.hasParameter(), true);
        Assert.assertEquals(q.getParameters(), Arrays.asList("0", "1"));
        Assert.assertEquals(q.apply(1, new StringBuilder("Int8")), "select 1::Int8");
    }
}
