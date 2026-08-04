package com.clickhouse.jdbc;

import java.util.Arrays;

import com.clickhouse.client.ClickHouseConfig;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
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

    @Test(groups = "unit", dataProvider = "heredocQueryProvider")
    public void testParseQueriesWithHeredoc(String sql, int parameters, String substituted) {
        JdbcParameterizedQuery q = JdbcParameterizedQuery.of(config, sql);
        Assert.assertEquals(q.getParameters().size(), parameters, "Parameter count mismatch for: " + sql);

        StringBuilder builder = new StringBuilder();
        q.apply(builder, "X", "Y");
        Assert.assertEquals(builder.toString(), substituted);
    }

    @DataProvider(name = "heredocQueryProvider")
    private static Object[][] getHeredocQueries() {
        return new Object[][] {
                // a heredoc is an opaque literal, so its contents are not parameters
                { "select $$a?b$$, ?", 1, "select $$a?b$$, X" },
                { "select $tag$ ? $tag$, ?", 1, "select $tag$ ? $tag$, X" },
                { "select $1$?$1$, ?", 1, "select $1$?$1$, X" },
                { "select $$?$$", 0, "select $$?$$" },
                { "select $$$$, ?", 1, "select $$$$, X" },
                { "select $$a$b$$, ?", 1, "select $$a$b$$, X" },
                { "select $$-- ?$$, ?", 1, "select $$-- ?$$, X" },
                { "select $$/* ? $$, ?", 1, "select $$/* ? $$, X" },
                { "select $$it's$$, ?", 1, "select $$it's$$, X" },
                { "select $$a;b$$, ?", 1, "select $$a;b$$, X" },
                { "select ?, $$a:b$$", 1, "select X, $$a:b$$" },
                { "select ?, lower($$it's$$)", 1, "select X, lower($$it's$$)" },
                { "select ?, position($$)$$, $$:$$)", 1, "select X, position($$)$$, $$:$$)" },
                { "select 1 ? $$a:b$$ : 2, ?", 1, "select 1 ? $$a:b$$ : 2, X" },
                { "select $_a1$ ? $_a1$, ?", 1, "select $_a1$ ? $_a1$, X" },
                { "$$?$$ as v, ?", 1, "$$?$$ as v, X" },
                { "insert into t values ($$a?b$$, ?)", 1, "insert into t values ($$a?b$$, X)" },
                // a dollar sign that does not open a heredoc stays an ordinary character
                { "select ? as a$x$, ? as b$x$", 2, "select X as a$x$, Y as b$x$" },
                { "select ? as a$b, ?", 2, "select X as a$b, Y" },
                { "select $$ ? , ?", 2, "select $$ X , Y" },
                { "select '$$?$$' as v, ?", 1, "select '$$?$$' as v, X" },
                { "select -- $$?$$\n?", 1, "select -- $$?$$\nX" },
                { "select /* $$?$$ */ ?", 1, "select /* $$?$$ */ X" },
                { "select 1 ? 'a' : 'b', ?", 1, "select 1 ? 'a' : 'b', X" },
        };
    }

    @Test(groups = "unit")
    public void testParseInvalidQueriesWithHeredoc() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> JdbcParameterizedQuery.of(config, "select $$a$$; select ?"));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> JdbcParameterizedQuery.of(config, "select $$a$$ as v; select 2"));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> JdbcParameterizedQuery.of(config, "select ?, f($$a$$"));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> JdbcParameterizedQuery.of(config, "select ?, 'a"));
    }
}
