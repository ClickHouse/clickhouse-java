package com.clickhouse.jdbc.internal;


import org.antlr.v4.runtime.tree.TerminalNode;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SqlParserTest {

//    @Test(groups = {"integration"})
//    public void testWithComments() throws Exception {
//        assertEquals(SqlParser.parseStatementType("    /* INSERT TESTING */\n SELECT 1 AS num").getType(), SqlParser.StatementType.SELECT);
//        assertEquals(SqlParser.parseStatementType("/* SELECT TESTING */\n INSERT INTO test_table VALUES (1)").getType(), SqlParser.StatementType.INSERT);
//        assertEquals(SqlParser.parseStatementType("/* INSERT TESTING */\n\n\n UPDATE test_table SET num = 2").getType(), SqlParser.StatementType.UPDATE);
//        assertEquals(SqlParser.parseStatementType("-- INSERT TESTING */\n SELECT 1 AS num").getType(), SqlParser.StatementType.SELECT);
//        assertEquals(SqlParser.parseStatementType("     -- SELECT TESTING \n -- SELECT AGAIN \n INSERT INTO test_table VALUES (1)").getType(), SqlParser.StatementType.INSERT);
//        assertEquals(SqlParser.parseStatementType(" SELECT 42    -- INSERT TESTING").getType(), SqlParser.StatementType.SELECT);
//        assertEquals(SqlParser.parseStatementType("#! INSERT TESTING \n SELECT 1 AS num").getType(), SqlParser.StatementType.SELECT);
//        assertEquals(SqlParser.parseStatementType("#!INSERT TESTING \n SELECT 1 AS num").getType(), SqlParser.StatementType.SELECT);
//        assertEquals(SqlParser.parseStatementType("# INSERT TESTING \n SELECT 1 AS num").getType(), SqlParser.StatementType.SELECT);
//        assertEquals(SqlParser.parseStatementType("#INSERT TESTING \n SELECT 1 AS num").getType(), SqlParser.StatementType.SELECT);
//        assertEquals(SqlParser.parseStatementType("\nINSERT TESTING \n SELECT 1 AS num").getType(), SqlParser.StatementType.INSERT_INTO_SELECT);
//        assertEquals(SqlParser.parseStatementType("         \n          INSERT TESTING \n SELECT 1 AS num").getType(), SqlParser.StatementType.INSERT_INTO_SELECT);
//        assertEquals(SqlParser.parseStatementType("INSERT INTO t SELECT 1 AS num").getType(), SqlParser.StatementType.INSERT_INTO_SELECT);
//        assertEquals(SqlParser.parseStatementType("select 1 AS num").getType(), SqlParser.StatementType.SELECT);
//        assertEquals(SqlParser.parseStatementType("insert into test_table values (1)").getType(), SqlParser.StatementType.INSERT);
//        assertEquals(SqlParser.parseStatementType("update test_table set num = 2").getType(), SqlParser.StatementType.UPDATE);
//        assertEquals(SqlParser.parseStatementType("delete from test_table where num = 2").getType(), SqlParser.StatementType.DELETE);
//        assertEquals(SqlParser.parseStatementType("sElEcT 1 AS num").getType(), SqlParser.StatementType.SELECT);
//        assertEquals(SqlParser.parseStatementType(null).getType(), SqlParser.StatementType.OTHER);
//        assertEquals(SqlParser.parseStatementType("").getType(), SqlParser.StatementType.OTHER);
//        assertEquals(SqlParser.parseStatementType("      ").getType(), SqlParser.StatementType.OTHER);
//    }
//
//    @Test(groups = {"integration"})
//    public void testParseStatementWithClause() throws Exception {
//        assertEquals(SqlParser.parseStatementType("with data as (SELECT number FROM numbers(100)) select * from data").getType(), SqlParser.StatementType.SELECT);
//    }

    @Test
    public void testParseInsertPrepared() throws Exception {
        SqlParser parser = new SqlParser();

        String sql = "INSERT INTO \n`table` (id, \nnum1, col3) \nVALUES    (?, ?, ?)   ";
        ParsedPreparedStatement parsed = parser.parsePreparedStatement(sql);
        System.out.println("table: " + parsed.getTable());
        String dataClause = sql.substring(parsed.getAssignValuesListStartPosition(), parsed.getAssignValuesListStopPosition() + 1);
        System.out.println("data clause: '" + dataClause + "'");

        int[] positions = parsed.getParamPositions();
        int[] paramPositionsInDataClause = new int[parsed.getArgCount()];
        for (int i = 0; i < parsed.getArgCount(); i++) {
            int p = positions[i] - parsed.getAssignValuesListStartPosition();
            paramPositionsInDataClause[i] = p;
            System.out.println("p in clause: " + p);
        }

        long tSBuildingSQL = System.nanoTime();
        StringBuilder insertSql = new StringBuilder(sql.substring(0, parsed.getAssignValuesListStartPosition()));
        for (int i = 0; i < 100_000; i++) {
            StringBuilder valuesClause = new StringBuilder(dataClause);
            int posOffset = 0;
            String val = "value_" + i;
            for (int j = 0; j < parsed.getArgCount(); j++) {
                int p = paramPositionsInDataClause[j] + posOffset;
                valuesClause.replace(p, p+1, val);
                posOffset += val.length() - 1;
            }
            insertSql.append(valuesClause).append(',');
        }
        insertSql.setLength(insertSql.length() -1 );
        long tFBuildingSQL = System.nanoTime();
        System.out.println("built in " + (tFBuildingSQL - tSBuildingSQL) + " ns " + ((tFBuildingSQL - tSBuildingSQL)/1000_000f) + " ms");
//        System.out.println("insertSQL: " + insertSql);

        System.out.println("-------");
        StringBuilder compiledSql = new StringBuilder(sql);
        int posOffset = 0;
        String val = "test";
        for (int i = 0; i < parsed.getArgCount(); i++) {
            int p = positions[i] + posOffset;

            System.out.println("p: " + p);
            compiledSql.replace(p, p+1, val);
            posOffset += val.length() - 1;
        }

        System.out.println(compiledSql);
    }

    @Test
    public void testParseSelectPrepared() throws Exception {
        // development test
        SqlParser parser = new SqlParser();

        String sql = "SELECT c1, c2, (true ? 1 : 0 ) as foo FROM tab1 WHERE c3 = ? AND c4 = abs(?)";
        ParsedPreparedStatement parsed = parser.parsePreparedStatement(sql);
        System.out.println("table: " + parsed.getTable());

        System.out.println("-------");
        StringBuilder compiledSql = new StringBuilder(sql);
        int posOffset = 0;
        String val = "test";
        int[] positions = parsed.getParamPositions();
        for (int i = 0; i < parsed.getArgCount(); i++) {
            int p = positions[i] + posOffset;

            System.out.println("p: " + p);
            compiledSql.replace(p, p+1, val);
            posOffset += val.length() - 1;
        }

        System.out.println(sql);
        System.out.println(compiledSql);
    }

    @Test
    public void testPreparedStatementCreateSQL() {
        SqlParser parser = new SqlParser();

        String sql = "CREATE TABLE IF NOT EXISTS `with_complex_id` (`v?``1` Int32, " +
                "\"v?\"\"2\" Int32,`v?\\`3` Int32, \"v?\\\"4\" Int32) ENGINE MergeTree ORDER BY ();";
        ParsedPreparedStatement parsed = parser.parsePreparedStatement(sql);
        // TODO: extend test expecting no errors
        assertFalse(parsed.isInsert());

        sql = "CREATE TABLE IF NOT EXISTS `test_stmt_split2` (v1 Int32, v2 String) ENGINE MergeTree ORDER BY (); ";
        parsed = parser.parsePreparedStatement(sql);
        assertFalse(parsed.isInsert());
    }


    @Test
    public void testPreparedStatementInsertSQL() {
        SqlParser parser = new SqlParser();

        String sql = "INSERT INTO `test_stmt_split2` VALUES (1, 'abc'), (2, '?'), (3, '?')";
        ParsedPreparedStatement parsed = parser.parsePreparedStatement(sql);
        // TODO: extend test expecting no errors
        assertTrue(parsed.isInsert());
        assertFalse(parsed.isHasResultSet());
        assertFalse(parsed.isInsertWithSelect());
        assertEquals(parsed.getAssignValuesGroups(), 3);

        sql = "-- line comment1 ?\n"
                + "# line comment2 ?\n"
                + "#! line comment3 ?\n"
                + "/* block comment ? \n */"
                + "INSERT INTO `with_complex_id`(`v?``1`, \"v?\"\"2\",`v?\\`3`, \"v?\\\"4\") VALUES (?, ?, ?, ?);";
        parsed = parser.parsePreparedStatement(sql);
        // TODO: extend test expecting no errors
        assertTrue(parsed.isInsert());
        assertFalse(parsed.isHasResultSet());
        assertFalse(parsed.isInsertWithSelect());
        assertEquals(parsed.getAssignValuesGroups(), 1);

        sql = "INSERT INTO tt SELECT now(), 10, 20.0, 30";
        parsed = parser.parsePreparedStatement(sql);
        // TODO: extend test expecting no errors
        assertTrue(parsed.isInsert());
        assertFalse(parsed.isHasResultSet());
        assertTrue(parsed.isInsertWithSelect());


        sql = "INSERT INTO `users` (`name`, `last_login`, `password`, `id`) VALUES\n" +
                    " (?, `parseDateTimeBestEffort`(?, ?), ?, 1)\n";
        parsed = parser.parsePreparedStatement(sql);
        // TODO: extend test expecting no errors
        assertTrue(parsed.isInsert());
        assertFalse(parsed.isHasResultSet());
        assertFalse(parsed.isInsertWithSelect());
        assertEquals(parsed.getAssignValuesGroups(), 1);
    }

    @Test
    public void testUnquoteIdentifier() {
        String[] names = new String[]{"test", "`test name1`", "\"test name 2\""};
        String[] expected = new String[]{"test", "test name1", "test name 2"};

        for (int i = 0; i < names.length; i++) {
            assertEquals(SqlParser.unquoteIdentifier(names[i]), expected[i]);
        }
    }

    @Test
    public void testEscapeQuotes() {
        String[] inStr = new String[]{"%valid_name%", "' OR 1=1 --", "\" OR 1=1 --"};
        String[] outStr = new String[]{"%valid_name%", "\\' OR 1=1 --", "\\\" OR 1=1 --"};

        for (int i = 0; i < inStr.length; i++) {
            assertEquals(SqlParser.escapeQuotes(inStr[i]), outStr[i]);
        }
    }

    @Test
    public void testStmtWithCasts() {
        String sql = "SELECT ?::integer, ?, '?::integer' FROM table WHERE v = ?::integer"; // CAST(?, INTEGER)
        SqlParser parser = new SqlParser();
        ParsedPreparedStatement stmt = parser.parsePreparedStatement(sql);
        Assert.assertEquals(stmt.getArgCount(), 3);
    }

    @Test
    public void testStmtWithFunction() {
        String sql = "SELECT `parseDateTimeBestEffort`(?, ?) as dt FROM table WHERE v > `parseDateTimeBestEffort`(?, ?)  ";
        SqlParser parser = new SqlParser();
        ParsedPreparedStatement stmt = parser.parsePreparedStatement(sql);
        Assert.assertEquals(stmt.getArgCount(), 4);
    }

    @Test(dataProvider = "testCreateStmtDP")
    public void testCreateStatement(String sql) {
        SqlParser parser = new SqlParser();
        ParsedPreparedStatement stmt = parser.parsePreparedStatement(sql);
        Assert.assertFalse(stmt.isHasErrors());
    }

    @DataProvider
    public static Object[][] testCreateStmtDP() {
        return new Object[][] {
                {"CREATE USER 'user01' IDENTIFIED WITH no_password"},
                {"CREATE USER 'user01' IDENTIFIED WITH plaintext_password BY 'qwerty'"},
                {"CREATE USER 'user01' IDENTIFIED WITH sha256_password BY 'qwerty' or IDENTIFIED BY 'password'"},
                {"CREATE USER 'user01' IDENTIFIED WITH sha256_hash BY 'hash' SALT 'salt'"},
                {"CREATE USER 'user01' IDENTIFIED WITH sha256_hash BY 'hash'"},
                {"CREATE USER 'user01' IDENTIFIED WITH double_sha1_password BY 'qwerty'"},
                {"CREATE USER 'user01' IDENTIFIED WITH double_sha1_hash BY 'hash'"},
                {"CREATE USER 'user01' IDENTIFIED WITH bcrypt_password BY 'qwerty'"},
                {"CREATE USER 'user01' IDENTIFIED WITH bcrypt_hash BY 'hash'"},
                {"CREATE USER 'user01' IDENTIFIED WITH ldap SERVER 'server_name'"},
                {"CREATE USER 'user01' IDENTIFIED WITH kerberos"},
                {"CREATE USER 'user01' IDENTIFIED WITH kerberos REALM 'realm'"},
                {"CREATE USER 'user01' IDENTIFIED WITH ssl_certificate CN 'mysite.com:user'"},
                {"CREATE USER 'user01' IDENTIFIED WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa', KEY 'another_public_key' TYPE 'ssh-ed25519'"},
                {"CREATE USER 'user01' IDENTIFIED WITH http SERVER 'http_server' SCHEME 'basic'"},
                {"CREATE USER 'user01' IDENTIFIED WITH http SERVER 'http_server'"},
                {"CREATE USER 'user01' IDENTIFIED BY 'qwerty'"},
        };
    }

    @Test(dataProvider = "testCTEStmtsDP")
    public void testCTEStatements(String sql, int args) {
        SqlParser parser = new SqlParser();
        ParsedPreparedStatement stmt = parser.parsePreparedStatement(sql);
        Assert.assertFalse(stmt.isHasErrors());
        Assert.assertEquals(stmt.getArgCount(), args);
    }

    @DataProvider
    public static Object[][] testCTEStmtsDP() {
        return new Object[][] {
                {"with ? as a, ? as b select a, b; -- two CTEs of the first form", 2},
                {"with a as (select ?), b as (select 2) select * from a, b; -- two CTEs of the second form", 1},
                {"(with a as (select ?) select * from a);", 1},
                {"with a as (select 1) select * from a; ", 0},
                {"(with ? as a select a);", 1},
                {"select * from ( with x as ( select 9 ) select * from x );", 0}

        };
    }

    @Test(dataProvider = "testMiscStmtDp")
    public void testMiscStatements(String sql, int args) {
        SqlParser parser = new SqlParser();
        ParsedPreparedStatement stmt = parser.parsePreparedStatement(sql);
        Assert.assertEquals(stmt.getArgCount(), args);
        Assert.assertFalse(stmt.isHasErrors());
    }

    @DataProvider
    public Object[][] testMiscStmtDp() {
        return new Object[][] {
            {"SELECT INTERVAL '1 day'", 0},
            {"SELECT INTERVAL 1 day", 0},
            {"SELECT * FROM table key WHERE ts = ?", 1},
            {"SELECT * FROM table source WHERE ts = ?", 1},
            {"SELECT * FROM table after WHERE ts = ?", 1},
            {"SELECT * FROM table before WHERE ts = ?", 1},
            {"SELECT * FROM table case WHERE ts = ?", 1},
            {"SELECT * FROM table cluster WHERE ts = ?", 1},
            {"SELECT * FROM table current WHERE ts = ?", 1},
            {"SELECT * FROM table index WHERE ts = ?", 1},
            {"SELECT * FROM table tables WHERE ts = ?", 1},
            {"SELECT * FROM table test WHERE ts = ?", 1},
            {"SELECT * FROM table view WHERE ts = ?", 1},
            {"SELECT * FROM table primary WHERE ts = ?", 1},
            {"insert into events (s) values ('a')", 0},
            {"insert into `events` (s) values ('a')", 0},
            {"SELECT COUNT(*) > 0 FROM system.databases WHERE name = ?", 1},
            {"SELECT count(*) > 0 FROM system.databases WHERE c1 = ?", 1},
            {"alter table user delete where reg_time = ?", 1},
        };
    }
}