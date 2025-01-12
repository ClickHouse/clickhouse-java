package com.clickhouse.jdbc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.clickhouse.jdbc.parser.ClickHouseSqlParser;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;
import com.clickhouse.jdbc.parser.StatementType;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JdbcParseHandlerTest {
    @BeforeMethod(groups = "unit")
    public void setV1() {
        System.setProperty("clickhouse.jdbc.v1","true");
    }

    @Test(groups = "unit")
    public void testInsertFromInFileStatement() {
        JdbcParseHandler handler = JdbcParseHandler.getInstance(false, false, true);
        Assert.assertEquals(ClickHouseSqlParser.parse("INSERT INTO aaa", null, handler)[0].getSQL(), "INSERT INTO aaa");
        Assert.assertEquals(ClickHouseSqlParser.parse("INSERT INTO aaa INFILE", null, handler)[0].getSQL(),
                "INSERT INTO aaa INFILE"); // invalid
        Assert.assertEquals(ClickHouseSqlParser.parse("INSERT INTO aaa FROM INFILE", null, handler)[0].getSQL(),
                "INSERT INTO aaa FROM INFILE"); // invalid
        Assert.assertEquals(ClickHouseSqlParser.parse("INSERT INTO aaa FROM INFILE 'a.csv'", null, handler)[0].getSQL(),
                "INSERT INTO aaa FORMAT CSV");
        Assert.assertEquals(
                ClickHouseSqlParser.parse("INSERT INTO aaa FROM INFILE 'a.csv' Format CSV", null, handler)[0].getSQL(),
                "INSERT INTO aaa Format CSV");
        Assert.assertEquals(
                ClickHouseSqlParser.parse("INSERT INTO aaa FROM INFILE 'a.csv' settings a=2", null, handler)[0]
                        .getSQL(),
                "INSERT INTO aaa settings a=2");
        Assert.assertEquals(
                ClickHouseSqlParser.parse("INSERT INTO aaa FROM INFILE 'a.csv.gz' compression 'gzip' settings a=2",
                        null, handler)[0]
                        .getSQL(),
                "INSERT INTO aaa settings a=2");
        Assert.assertEquals(
                ClickHouseSqlParser.parse(
                        "INSERT INTO aaa FROM INFILE 'input_*.csv.gz' compression 'gzip' settings max_result_rows=1, max_execution_time=2 FORMAT CSV",
                        null, handler)[0].getSQL(),
                "INSERT INTO aaa settings max_result_rows=1, max_execution_time=2 FORMAT CSV");
    }

    @Test(groups = "unit")
    public void testSelectIntoOutFileStatement() {
        JdbcParseHandler handler = JdbcParseHandler.getInstance(false, false, true);
        Assert.assertEquals(ClickHouseSqlParser.parse("select 1", null, handler)[0].getSQL(), "select 1");
        Assert.assertEquals(ClickHouseSqlParser.parse("select * from outfile", null, handler)[0].getSQL(),
                "select * from outfile");
        Assert.assertEquals(ClickHouseSqlParser.parse("select into outfile", null, handler)[0].getSQL(),
                "select into outfile"); // invalid
        Assert.assertEquals(ClickHouseSqlParser.parse("select 1 into outfile 'a.csv'", null, handler)[0].getSQL(),
                "select 1 ");
        Assert.assertEquals(ClickHouseSqlParser.parse(
                "select * from numbers(10) settings max_result_rows=1,max_execution_time=3 into outfile 'a.csv' format CSV",
                null, handler)[0].getSQL(),
                "select * from numbers(10) settings max_result_rows=1,max_execution_time=3 format CSV");
    }

    @Test(groups = "unit")
    public void testParseDeleteStatement() {
        Assert.assertEquals(
                ClickHouseSqlParser.parse("delete from tbl", null, JdbcParseHandler.INSTANCE)[0]
                        .getSQL(),
                "TRUNCATE TABLE tbl");
        Assert.assertEquals(
                ClickHouseSqlParser.parse("delete from tbl where 1", null, JdbcParseHandler.INSTANCE)[0]
                        .getSQL(),
                "ALTER TABLE `tbl` DELETE where 1 SETTINGS mutations_sync=1");
        Assert.assertEquals(
                ClickHouseSqlParser.parse("delete from tbl where 1 and 1 settings a=1 format CSV", null,
                        JdbcParseHandler.INSTANCE)[0].getSQL(),
                "ALTER TABLE `tbl` DELETE where 1 and 1 SETTINGS mutations_sync=1, a=1 format CSV");
        Assert.assertEquals(
                ClickHouseSqlParser.parse(
                        "delete from tbl where 1 and 1 settings mutations_sync=0 format CSV",
                        null,
                        JdbcParseHandler.INSTANCE)[0].getSQL(),
                "ALTER TABLE `tbl` DELETE where 1 and 1 settings mutations_sync=0 format CSV");
    }

    @Test(groups = "unit")
    public void testParseInsertStatement() {
    }

    @Test(groups = "unit")
    public void testParseUpdateStatement() {
        Assert.assertEquals(
                ClickHouseSqlParser.parse("update tbl set a=1", null, JdbcParseHandler.INSTANCE)[0]
                        .getSQL(),
                "ALTER TABLE `tbl` UPDATE a=1 SETTINGS mutations_sync=1");
        Assert.assertEquals(
                ClickHouseSqlParser.parse("update tbl set a=1,b=2 where 1", null,
                        JdbcParseHandler.INSTANCE)[0]
                        .getSQL(),
                "ALTER TABLE `tbl` UPDATE a=1,b=2 where 1 SETTINGS mutations_sync=1");
        Assert.assertEquals(
                ClickHouseSqlParser.parse(
                        "update tbl set x=1, y = 2 where 1 and 1 settings a=1 format CSV", null,
                        JdbcParseHandler.INSTANCE)[0].getSQL(),
                "ALTER TABLE `tbl` UPDATE x=1, y = 2 where 1 and 1 SETTINGS mutations_sync=1, a=1 format CSV");
        Assert.assertEquals(
                ClickHouseSqlParser.parse(
                        "update tbl set y = 2 where 1 and 1 settings mutations_sync=0 format CSV",
                        null, JdbcParseHandler.INSTANCE)[0].getSQL(),
                "ALTER TABLE `tbl` UPDATE y = 2 where 1 and 1 settings mutations_sync=0 format CSV");
    }

    @Test(groups = "unit")
    public void testDeleteStatementWithoutWhereClause() {
        Assert.assertEquals(JdbcParseHandler.INSTANCE.handleStatement("delete  from  `a\\`' a` . tbl",
                StatementType.DELETE, null, "a\\`' a", "tbl", null, null, null, null, null, null,
                new HashMap<String, Integer>() {
                    {
                        put("DELETE", 0);
                        put("FROM", 8);
                    }
                }, null, null),
                new ClickHouseSqlStatement("TRUNCATE TABLE  `a\\`' a` . tbl", StatementType.DELETE,
                        null, "a\\`' a", "tbl", null, null, null, null, null, null, null, null, null));
        Assert.assertEquals(JdbcParseHandler.INSTANCE.handleStatement("delete from  `table\\`'1`",
                StatementType.DELETE,
                null, null, "table1", null, null, null, null, null, null, new HashMap<String, Integer>() {
                    {
                        put("DELETE", 0);
                        put("FROM", 7);
                    }
                }, null, null),
                new ClickHouseSqlStatement("TRUNCATE TABLE  `table\\`'1`", StatementType.DELETE, null,
                        null, "table1", null, null, null, null, null, null, null, null, null),
                null);
    }

    @Test(groups = "unit")
    public void testDeleteStatementWithWhereClause() {
        Map<String, Integer> positions = new HashMap<>();
        positions.put("DELETE", 0);
        positions.put("FROM", 7);
        positions.put("WHERE", 28);
        Assert.assertEquals(
                JdbcParseHandler.INSTANCE.handleStatement("delete from  `a\\`' a` . tbl where a = b",
                        StatementType.DELETE, null, "a\\`' a", "tbl", null, null, null, null, null, null, positions,
                        null, null),
                new ClickHouseSqlStatement(
                        "ALTER TABLE `a\\`' a`.`tbl` DELETE where a = b SETTINGS mutations_sync=1",
                        StatementType.DELETE, null, "a\\`' a", "tbl", null, null, null, null, null, null, null, null,
                        null));
        positions.put("DELETE", 0);
        positions.put("FROM", 8);
        positions.put("WHERE", 26);
        Assert.assertEquals(
                JdbcParseHandler.INSTANCE.handleStatement("delete  from  `table\\`'1` where 1",
                        StatementType.DELETE, null, null, "table\\`'1", null, null, null, null, null, null, positions,
                        null, null),
                new ClickHouseSqlStatement(
                        "ALTER TABLE `table\\`'1` DELETE where 1 SETTINGS mutations_sync=1",
                        StatementType.DELETE, null, null, "table\\`'1", null, null, null, null, null,
                        null, null, null, null));
    }

    @Test(groups = "unit")
    public void testDeleteStatementWithSettings() {
        String sql1 = "delete from tbl settings a=1";
        Map<String, Integer> positions = new HashMap<String, Integer>() {
            {
                put("DELETE", 0);
                put("FROM", sql1.indexOf("from"));
                put("SETTINGS", sql1.indexOf("settings"));
            }
        };
        Map<String, String> settings = Collections.singletonMap("a", "1");
        Assert.assertEquals(
                JdbcParseHandler.INSTANCE.handleStatement(sql1, StatementType.DELETE, null, null, "tbl",
                        null, null, null, null, null, null, positions, settings, null),
                new ClickHouseSqlStatement("TRUNCATE TABLE tbl settings a=1", StatementType.DELETE,
                        null, null, "tbl", null, null, null, null, null, null, null, settings, null));

        String sql2 = "delete from tbl where a != 1 and b != 2 settings a=1,b='a'";
        positions = new HashMap<String, Integer>() {
            {
                put("DELETE", 0);
                put("FROM", sql2.indexOf("from"));
                put("WHERE", sql2.indexOf("where"));
                put("SETTINGS", sql2.indexOf("settings"));
            }
        };
        settings = new HashMap<String, String>() {
            {
                put("a", "1");
                put("b", "'a'");
            }
        };
        Assert.assertEquals(
                JdbcParseHandler.INSTANCE.handleStatement(sql2, StatementType.DELETE, null, null, "tbl",
                        null, null, null, null, null, null, positions, settings, null),
                new ClickHouseSqlStatement(
                        "ALTER TABLE `tbl` DELETE where a != 1 and b != 2 SETTINGS mutations_sync=1, a=1,b='a'",
                        StatementType.DELETE, null, null, "tbl", null, null, null, null, null, null, null,
                        settings, null));

        String sql3 = "delete from tbl where a != 1 and b != 2 settings a=1,mutations_sync=2,b='a'";
        positions = new HashMap<String, Integer>() {
            {
                put("DELETE", 0);
                put("FROM", sql3.indexOf("from"));
                put("WHERE", sql3.indexOf("where"));
                put("SETTINGS", sql3.indexOf("settings"));
            }
        };
        settings = new HashMap<String, String>() {
            {
                put("a", "1");
                put("mutations_sync", "2");
                put("b", "'a'");
            }
        };
        Assert.assertEquals(
                JdbcParseHandler.INSTANCE.handleStatement(sql3, StatementType.DELETE, null, null, "tbl",
                        null, null, null, null, null, null, positions, settings, null),
                new ClickHouseSqlStatement(
                        "ALTER TABLE `tbl` DELETE where a != 1 and b != 2 settings a=1,mutations_sync=2,b='a'",
                        StatementType.DELETE, null, null, "tbl", null, null, null, null, null, null, null, settings,
                        null));
    }

    @Test(groups = "unit")
    public void testUpdateStatementWithoutWhereClause() {
        Assert.assertEquals(JdbcParseHandler.INSTANCE.handleStatement("update  `a\\`' a` . tbl set a=1",
                StatementType.UPDATE, null, "a\\`' a", "tbl", null, null, null, null, null, null,
                new HashMap<String, Integer>() {
                    {
                        put("UPDATE", 0);
                        put("SET", 23);
                    }
                }, null, null),
                new ClickHouseSqlStatement(
                        "ALTER TABLE `a\\`' a`.`tbl` UPDATE a=1 SETTINGS mutations_sync=1",
                        StatementType.UPDATE, null, "a\\`' a", "tbl", null, null, null, null, null, null, null, null,
                        null));
        Assert.assertEquals(JdbcParseHandler.INSTANCE.handleStatement("update  `table\\`'1` set a=1",
                StatementType.UPDATE, null, null, "table1", null, null, null, null, null, null,
                new HashMap<String, Integer>() {
                    {
                        put("UPDATE", 0);
                        put("SET", 20);
                    }
                }, null, null),
                new ClickHouseSqlStatement("ALTER TABLE `table1` UPDATE a=1 SETTINGS mutations_sync=1",
                        StatementType.UPDATE, null, null, "table1", null, null, null, null, null, null, null, null,
                        null));
    }

    @Test(groups = "unit")
    public void testUpdateStatementWithWhereClause() {
        Map<String, Integer> positions = new HashMap<>();
        positions.put("UPDATE", 0);
        positions.put("SET", 23);
        Assert.assertEquals(
                JdbcParseHandler.INSTANCE.handleStatement(
                        "Update  `a\\`' a` . tbl set a = 2 where a = b",
                        StatementType.UPDATE, null, "a\\`' a", "tbl", null, null, null, null, null, null, positions,
                        null, null),
                new ClickHouseSqlStatement(
                        "ALTER TABLE `a\\`' a`.`tbl` UPDATE a = 2 where a = b SETTINGS mutations_sync=1",
                        StatementType.UPDATE, null, "a\\`' a", "tbl", null, null, null, null, null, null, null, null,
                        null));
        positions.put("UPDATE", 0);
        positions.put("SET", 19);
        Assert.assertEquals(
                JdbcParseHandler.INSTANCE.handleStatement("update `table\\`'1` set a = b where 1",
                        StatementType.UPDATE, null, null, "table\\`'1", null, null, null, null, null, null, positions,
                        null, null),
                new ClickHouseSqlStatement(
                        "ALTER TABLE `table\\`'1` UPDATE a = b where 1 SETTINGS mutations_sync=1",
                        StatementType.UPDATE, null, null, "table\\`'1", null, null, null, null, null, null, null, null,
                        null));
    }

    @Test(groups = "unit")
    public void testUpdateStatementWithSettings() {
        String sql1 = "update tbl set x=1 settings a=1";
        Map<String, Integer> positions = new HashMap<String, Integer>() {
            {
                put("UPDATE", 0);
                put("SET", sql1.indexOf("set"));
                put("SETTINGS", sql1.indexOf("settings"));
            }
        };
        Map<String, String> settings = Collections.singletonMap("a", "1");
        Assert.assertEquals(
                JdbcParseHandler.INSTANCE.handleStatement(sql1, StatementType.UPDATE, null, null, "tbl",
                        null, null, null, null, null, null, positions, settings, null),
                new ClickHouseSqlStatement(
                        "ALTER TABLE `tbl` UPDATE x=1 SETTINGS mutations_sync=1, a=1",
                        StatementType.UPDATE, null, null, "tbl", null, null, null, null, null, null, null, settings,
                        null));

        String sql2 = "update tbl set x=1, y=2 where a != 1 and b != 2 settings a=1,b='a'";
        positions = new HashMap<String, Integer>() {
            {
                put("UPDATE", 0);
                put("SET", sql1.indexOf("set"));
                put("WHERE", sql2.indexOf("where"));
                put("SETTINGS", sql2.indexOf("settings"));
            }
        };
        settings = new HashMap<String, String>() {
            {
                put("a", "1");
                put("b", "'a'");
            }
        };
        Assert.assertEquals(
                JdbcParseHandler.INSTANCE.handleStatement(sql2, StatementType.UPDATE, null, null, "tbl",
                        null, null, null, null, null, null, positions, settings, null),
                new ClickHouseSqlStatement(
                        "ALTER TABLE `tbl` UPDATE x=1, y=2 where a != 1 and b != 2 SETTINGS mutations_sync=1, a=1,b='a'",
                        StatementType.UPDATE, null, null, "tbl", null, null, null, null, null, null, null, settings,
                        null));

        String sql3 = "update tbl set x=1,y=2 where a != 1 and b != 2 settings a=1,mutations_sync=2,b='a'";
        positions = new HashMap<String, Integer>() {
            {
                put("UPDATE", 0);
                put("SET", sql1.indexOf("set"));
                put("WHERE", sql3.indexOf("where"));
                put("SETTINGS", sql3.indexOf("settings"));
            }
        };
        settings = new HashMap<String, String>() {
            {
                put("a", "1");
                put("mutations_sync", "2");
                put("b", "'a'");
            }
        };
        Assert.assertEquals(
                JdbcParseHandler.INSTANCE.handleStatement(sql3, StatementType.UPDATE, null, null, "tbl",
                        null, null, null, null, null, null, positions, settings, null),
                new ClickHouseSqlStatement(
                        "ALTER TABLE `tbl` UPDATE x=1,y=2 where a != 1 and b != 2 settings a=1,mutations_sync=2,b='a'",
                        StatementType.UPDATE, null, null, "tbl", null, null, null, null, null, null, null, settings,
                        null));
    }
}
