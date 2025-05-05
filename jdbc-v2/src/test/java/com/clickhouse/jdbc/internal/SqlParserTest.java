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
}