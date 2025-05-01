package com.clickhouse.jdbc.internal;


import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

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
}