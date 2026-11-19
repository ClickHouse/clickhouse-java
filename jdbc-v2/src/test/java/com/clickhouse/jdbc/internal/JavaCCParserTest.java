package com.clickhouse.jdbc.internal;


import com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlParser;
import com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlStatement;
import com.clickhouse.jdbc.internal.parser.javacc.JdbcParseHandler;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

//@Ignore
public class JavaCCParserTest extends BaseSqlParserFacadeTest {
    public JavaCCParserTest() throws Exception {
        super(SqlParserFacade.SQLParser.JAVACC.name());
    }

    @Test(dataProvider = "clusterNamesDP")
    public void testClusterName(String sql, String expectedCluster) {
        ClickHouseSqlStatement stmt = ClickHouseSqlParser.parse(sql, JdbcParseHandler.getInstance())[0];
        Assert.assertEquals(stmt.getCluster(), expectedCluster, "Cluster mismatch for: " + sql);
    }

    @DataProvider
    public static Object[][] clusterNamesDP() {
        return new Object[][] {
                {"ALTER TABLE t ON CLUSTER c DELETE WHERE 1", "c"},
                {"ALTER TABLE t ON CLUSTER 'c''1' DELETE WHERE 1", "c'1"},
                // A heredoc body has no escape sequences
                {"ALTER TABLE t ON CLUSTER $$c''1$$ DELETE WHERE 1", "c''1"},
                {"ALTER TABLE t ON CLUSTER $tag$c$tag$ DELETE WHERE 1", "c"},
        };
    }
}
