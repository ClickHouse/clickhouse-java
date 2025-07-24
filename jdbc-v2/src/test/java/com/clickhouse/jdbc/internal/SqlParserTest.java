package com.clickhouse.jdbc.internal;


import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
    public void testStmtWithCasts() {
        String sql = "SELECT ?::integer, ?, '?:: integer' FROM table WHERE v = ?::integer"; // CAST(?, INTEGER)
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

    @Test
    public void testStmtWithUUID() {
        String sql = "select sum(value) from `uuid_filter_db`.`uuid_filter_table` where uuid = ?";
        SqlParser parser = new SqlParser();
        ParsedPreparedStatement stmt = parser.parsePreparedStatement(sql);
        Assert.assertEquals(stmt.getArgCount(), 1);
        Assert.assertFalse(stmt.isHasErrors());
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
            {"SELECT COUNT() FROM system.databases WHERE name = ?", 1},
            {"alter table user delete where reg_time = ?", 1},
            {"SELECT * FROM a,b WHERE id > ?", 1},
            {"DROP USER IF EXISTS default_impersonation_user", 0},
            {"DROP ROLE IF EXISTS `vkonfwxapllzkkgkqdvt`", 0},
            {"CREATE ROLE `kjxrsscptauligukwgmf` ON CLUSTER '{cluster}'", 0},
            {"GRANT SELECT ON `test_data`.`venues` TO `vkonfwxapllzkkgkqdvt`", 0},
            {"GRANT `uqkczgnpmpuktxhwvqqd` TO `default_impersonation_user`", 0},
            {"SET ROLE NONE", 0},
            {"CREATE ROLE IF NOT EXISTS row_a ON CLUSTER '{cluster}'", 0},
            {"CREATE ROW POLICY role_policy_BTABPUVDDLXZPYBCJGGZ ON `test_data`.`products` AS RESTRICTIVE FOR SELECT USING (`id` = 1) TO `annhpwyelooonsmqjldo`", 0},
            {"CREATE ROW POLICY role_policy_BTABPUVDDLXZPYBCJGGZ ON `products` AS RESTRICTIVE FOR SELECT USING (`id` = 1) TO `annhpwyelooonsmqjldo`", 0},
            {"GRANT ON CLUSTER '{cluster}' row_a, row_b, row_c TO metabase_impersonation_test_user", 0},
            {"GRANT ON CLUSTER '{cluster}' SELECT ON metabase_impersonation_test.test_1751397165968 TO metabase_impersonation_test_user", 0},
            {"CREATE ROW POLICY OR REPLACE policy_row_a ON CLUSTER '{cluster}' ON metabase_impersonation_test.test_1751397165968 FOR SELECT USING s = 'a' TO row_a", 0},
            {"CREATE ROW POLICY OR REPLACE policy_row_b ON CLUSTER '{cluster}' ON metabase_impersonation_test.test_1751397165968 FOR SELECT USING s = 'b' TO row_b", 0},
            {"CREATE ROW POLICY OR REPLACE policy_row_c ON CLUSTER '{cluster}' ON metabase_impersonation_test.test_1751397165968 FOR SELECT USING s = 'c' TO row_c", 0},
            {"GRANT SELECT ON `metabase_test_role_db`.`*` TO `metabase_test_role`,`metabase-test-role`", 0},
            {"GRANT SELECT ON `metabase_test_role_db`.* TO `metabase_test_role`,`metabase-test-role`", 0},
            {"GRANT `metabase_test_role`, `metabase-test-role` TO `metabase_test_user`", 0},
            {"GRANT ON CLUSTER '{cluster}' SELECT ON `metabase_test_role_db`.* TO `metabase_test_role`, `metabase-test-role`", 0},
            {"GRANT ON CLUSTER '{cluster}' `metabase_test_role`, `metabase-test-role` TO `metabase_test_user`", 0},
            {"SELECT * FROM `test_data`.`categories` WHERE id = 1::String or id = ?", 1},
            {"SELECT * FROM `test_data`.`categories` WHERE id = cast(1 as String) or id = ?", 1},
            {"select * from test_data.categories WHERE test_data.categories.name = ? limit 4", 1},
            {INSERT_INLINE_DATA, 0},
            {"select sum(value) from `uuid_filter_db`.`uuid_filter_table` WHERE `uuid_filter_db`.`uuid_filter_table`.`uuid` IN (CAST('36f7f85c-d7f4-49e2-af05-f45d5f6636ad' AS UUID))", 0},
            {"SELECT DISTINCT ON (column) FROM table WHERE column > ?", 1},
            {"SELECT * FROM test_table \nUNION\n DISTINCT SELECT * FROM test_table", 0},
            {"SELECT * FROM test_table \nUNION\n ALL SELECT * FROM test_table", 0},
            {"SELECT * FROM test_table1 \nUNION\n SELECT * FROM test_table2 WHERE test_table2.column1 = ?", 1},
            {COMPLEX_CTE, 4},
            {SIMPLE_CTE, 0},
            {CTE_CONSTANT_AS_VARIABLE, 1},
            {"select toYear(dt) year from test WHERE val=?", 1},
            {"select 1 year, 2 hour, 3 minute, 4 second", 0},
            {"select toYear(dt) AS year from test WHERE val=?", 1},
            {"select toYear(dt) AS yearx from test WHERE val=?", 1},
            {"SELECT v FROM t WHERE f in (?)", 1},
            {"SELECT v FROM t WHERE a > 10 AND event NOT IN (?)", 1},
            {"SELECT v FROM t WHERE f in (1, 2, 3)", 0},
            {"with ? as val1, numz as (select val1, number from system.numbers limit 10) select * from numz", 1}
        };
    }

    private static final String INSERT_INLINE_DATA =
            "INSERT INTO `interval_15_XUTLZWBLKMNZZPRZSKRF`.`checkins` (`timestamp`, `id`) " +
                    "VALUES ((`now64`(9) + INTERVAL -225 second), 1)";

    private static final String COMPLEX_CTE = "WITH ? AS starting_time, ? AS ending_time, 0 AS session_timeout, '{start}' AS starting_event, '{end}' AS ending_event, SessionData AS (\n" +
            "    WITH\n" +
            "        date,\n" +
            "        arraySort(\n" +
            "            groupArray(\n" +
            "              (\n" +
            "                  tracking.event.time,\n" +
            "                  tracking.event.event\n" +
            "              )\n" +
            "            )\n" +
            "        ) AS _sorted_events,\n" +
            "        arrayEnumerate(_sorted_events) AS _event_serial,\n" +
            "        arrayDifference(_sorted_events.1) AS _event_time_diff,\n" +
            "        \n" +
            "        arrayFilter(\n" +
            "            (x, y, z) -> y > session_timeout OR z.2 = starting_event,\n" +
            "            _event_serial,\n" +
            "            _event_time_diff,\n" +
            "            _sorted_events\n" +
            "        ) AS _gap_index_1,\n" +
            "\n" +
            "        arrayFilter(\n" +
            "            (x, y) -> y.2 = ending_event,\n" +
            "            _event_serial,\n" +
            "            _sorted_events\n" +
            "        ) AS _gap_index_2_,\n" +
            "        arrayMap(\n" +
            "            x -> x + 1,\n" +
            "            _gap_index_2_\n" +
            "        ) AS _gap_index_2,\n" +
            "\n" +
            "        arrayMap(x -> if (has(_gap_index_1,x) OR has(_gap_index_2,x), 1, 0), _event_serial) AS _session_splitter,\n" +
            "        arraySplit((x, y) -> y, _sorted_events, _session_splitter) AS _session_chain\n" +
            "    SELECT\n" +
            "        date,\n" +
            "        user_id AS user_id,\n" +
            "        arrayJoin(_session_chain) AS event_chain,\n" +
            "        \n" +
            "        arrayCompact(x -> x.2, event_chain) AS event_chain_dedup\n" +
            "    FROM tracking.event\n" +
            "    WHERE\n" +
            "        project=? AND time>=starting_time AND time<ending_time\n" +
            "        AND event NOT IN (?)\n" +
            "    GROUP BY\n" +
            "        date,\n" +
            "        user_id\n" +
            "),\n" +
            "SessionOverallInfo AS (\n" +
            "    SELECT\n" +
            "        date,\n" +
            "        COUNT(*) AS number_of_sessions\n" +
            "    FROM SessionData\n" +
            "    GROUP BY date\n" +
            ")\n" +
            "SELECT\n" +
            "    SessionOverallInfo.date, SessionOverallInfo.number_of_sessions AS number_of_total_sessions\n" +
            "FROM\n" +
            "    SessionOverallInfo\n" +
            "ORDER BY\n" +
            "    SessionOverallInfo.date";

    private static final String SIMPLE_CTE = "WITH cte_numbers AS\n" +
            "(\n" +
            "    SELECT\n" +
            "        num\n" +
            "    FROM generateRandom('num UInt64', NULL)\n" +
            "    LIMIT 1000000\n" +
            ")\n" +
            "SELECT\n" +
            "    count()\n" +
            "FROM cte_numbers\n" +
            "WHERE num IN (SELECT num FROM cte_numbers)";

    private static final String CTE_CONSTANT_AS_VARIABLE = "WITH '2019-08-01 15:23:00' AS ts_upper_bound\n" +
            "SELECT *\n" +
            "FROM hits\n" +
            "WHERE\n" +
            "    EventDate = toDate(?) AND\n" +
            "    EventTime <= ts_upper_bound;";
}