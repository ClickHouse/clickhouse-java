package com.clickhouse.jdbc.internal;


import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class StatementParserTest {

    @Test(groups = {"integration"})
    public void testWithComments() throws Exception {
        assertEquals(StatementParser.parseStatementType("    /* INSERT TESTING */\n SELECT 1 AS num").getType(), StatementParser.StatementType.SELECT);
        assertEquals(StatementParser.parseStatementType("/* SELECT TESTING */\n INSERT INTO test_table VALUES (1)").getType(), StatementParser.StatementType.INSERT);
        assertEquals(StatementParser.parseStatementType("/* INSERT TESTING */\n\n\n UPDATE test_table SET num = 2").getType(), StatementParser.StatementType.UPDATE);
        assertEquals(StatementParser.parseStatementType("-- INSERT TESTING */\n SELECT 1 AS num").getType(), StatementParser.StatementType.SELECT);
        assertEquals(StatementParser.parseStatementType("     -- SELECT TESTING \n -- SELECT AGAIN \n INSERT INTO test_table VALUES (1)").getType(), StatementParser.StatementType.INSERT);
        assertEquals(StatementParser.parseStatementType(" SELECT 42    -- INSERT TESTING").getType(), StatementParser.StatementType.SELECT);
        assertEquals(StatementParser.parseStatementType("#! INSERT TESTING \n SELECT 1 AS num").getType(), StatementParser.StatementType.SELECT);
        assertEquals(StatementParser.parseStatementType("#!INSERT TESTING \n SELECT 1 AS num").getType(), StatementParser.StatementType.SELECT);
        assertEquals(StatementParser.parseStatementType("# INSERT TESTING \n SELECT 1 AS num").getType(), StatementParser.StatementType.SELECT);
        assertEquals(StatementParser.parseStatementType("#INSERT TESTING \n SELECT 1 AS num").getType(), StatementParser.StatementType.SELECT);
        assertEquals(StatementParser.parseStatementType("\nINSERT TESTING \n SELECT 1 AS num").getType(), StatementParser.StatementType.INSERT_INTO_SELECT);
        assertEquals(StatementParser.parseStatementType("         \n          INSERT TESTING \n SELECT 1 AS num").getType(), StatementParser.StatementType.INSERT_INTO_SELECT);
        assertEquals(StatementParser.parseStatementType("INSERT INTO t SELECT 1 AS num").getType(), StatementParser.StatementType.INSERT_INTO_SELECT);
        assertEquals(StatementParser.parseStatementType("select 1 AS num").getType(), StatementParser.StatementType.SELECT);
        assertEquals(StatementParser.parseStatementType("insert into test_table values (1)").getType(), StatementParser.StatementType.INSERT);
        assertEquals(StatementParser.parseStatementType("update test_table set num = 2").getType(), StatementParser.StatementType.UPDATE);
        assertEquals(StatementParser.parseStatementType("delete from test_table where num = 2").getType(), StatementParser.StatementType.DELETE);
        assertEquals(StatementParser.parseStatementType("sElEcT 1 AS num").getType(), StatementParser.StatementType.SELECT);
        assertEquals(StatementParser.parseStatementType(null).getType(), StatementParser.StatementType.OTHER);
        assertEquals(StatementParser.parseStatementType("").getType(), StatementParser.StatementType.OTHER);
        assertEquals(StatementParser.parseStatementType("      ").getType(), StatementParser.StatementType.OTHER);
    }

    @Test(groups = {"integration"})
    public void testParseStatementWithClause() throws Exception {
        assertEquals(StatementParser.parseStatementType("with data as (SELECT number FROM numbers(100)) select * from data").getType(), StatementParser.StatementType.SELECT);
    }

}