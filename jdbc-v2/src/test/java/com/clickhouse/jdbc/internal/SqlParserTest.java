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
        Assert.assertTrue(stmt.isHasResultSet());
    }

    @DataProvider
    public static Object[][] testCTEStmtsDP() {
        return new Object[][] {
                {"with ? as a, ? as b select a, b; -- two CTEs of the first form", 2},
                {"with a as (select ?), b as (select 2) select * from a, b; -- two CTEs of the second form", 1},
                {"(with a as (select ?) select * from a);", 1},
                {"with a as (select 1) select * from a; ", 0},
                {"(with ? as a select a);", 1},
                {"select * from ( with x as ( select 9 ) select * from x );", 0},
                {"WITH toDateTime(?) AS target_time SELECT * FROM table", 1},
                {"WITH toDateTime('2025-08-20 12:34:56') AS target_time SELECT * FROM table", 0},
                {"WITH toDate('2025-08-20') as DATE_END, events AS ( SELECT 1 ) SELECT * FROM events", 0},
                {"WITH toDate(?) as DATE_END, events AS ( SELECT 1 ) SELECT * FROM events", 1}
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
            {"with ? as val1, numz as (select val1, number from system.numbers limit 10) select * from numz", 1},
            {"WITH 'hello' REGEXP 'h' AS result SELECT 1", 0},
            {"WITH (select 1) as a, z AS (select 2) SELECT 1", 0},
            {"SELECT result FROM test_view(myParam = ?)", 1},
            {"WITH toDate('2025-08-20') as DATE_END, events AS ( SELECT 1 ) SELECT * FROM events", 0},
            {"select 1 table where 1 = ?", 1},
            {"insert into t (i, t) values (1, timestamp '2010-01-01 00:00:00')", 0},
            {"insert into t (i, t) values (1, date '2010-01-01')", 0
            }
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


    @Test(dataProvider = "testStatementWithoutResultSetDP")
    public void testStatementsForResultSet(String sql, int args, boolean hasResultSet) {
        SqlParser parser = new SqlParser();
        {
            ParsedPreparedStatement stmt = parser.parsePreparedStatement(sql);
            Assert.assertEquals(stmt.getArgCount(), args);
            assertEquals(stmt.isHasResultSet(), hasResultSet, "Statement result set expectation does not match");
            Assert.assertFalse(stmt.isHasErrors(), "Statement has errors");
        }

        {
            ParsedStatement stmt = parser.parsedStatement(sql);
            assertEquals(stmt.isHasResultSet(), hasResultSet, "Statement result set expectation does not match");
            Assert.assertFalse(stmt.isHasErrors(), "Statement has errors");
        }
    }

    @DataProvider
    public static Object[][] testStatementWithoutResultSetDP() {
        return  new Object[][]{
                /* has result set */
                {"SELECT * FROM test_table", 0, true},
                {"SHOW CREATE TABLE `db`.`test_table`", 0, true},
                {"SHOW CREATE TEMPORARY TABLE `db1`.`tmp_table`", 0, true},
                {"SHOW CREATE DICTIONARY dict1", 0, true},
                {"SHOW CREATE VIEW view1", 0, true},
                {"SHOW CREATE DATABASE db1", 0, true},
                {"SHOW CREATE TABLE table1 INTO OUTFILE 'table1.sql'", 0, true},
                {"SHOW TABLES ", 0, true},
                {"SHOW TABLES FROM system LIKE '%user%'", 0, true},
                {"SHOW COLUMNS FROM `orders` LIKE 'delivery_%'", 0, true},
                {"SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2", 0, true},
                {"SHOW INDEX FROM `tbl`", 0, true},
                {"SHOW PROCESSLIST", 0, true},
                {"SHOW GRANTS FOR `user01`", 0, true},
                {"SHOW GRANTS FOR `user01` FINAL", 0, true},
                {"SHOW GRANTS FOR `user01` WITH IMPLICIT FINAL", 0, true},
                {"SHOW CREATE USER `user01`", 0, true},
                {"SHOW CREATE USER CURRENT_USER", 0, true},
                {"SHOW CREATE ROLE `role_01`", 0, true},
                {"SHOW CREATE POLICY policy_1 ON `tableA`, `db1`.`tableB`", 0, true},
                {"SHOW CREATE ROW POLICY policy_1 ON `tableA`, `db1`.`tableB`", 0, true},
                {"SHOW CREATE QUOTA CURRENT", 0, true},
                {"SHOW CREATE QUOTA `q1`", 0, true},
                {"SHOW CREATE PROFILE `p1`", 0, true},
                {"SHOW CREATE SETTINGS PROFILE `p3`", 0, true},
                {"SHOW USERS", 0, true},
                {"SHOW CURRENT ROLES", 0, true},
                {"SHOW ENABLED ROLES", 0, true},
                {"SHOW SETTINGS PROFILES", 0, true},
                {"SHOW PROFILES", 0, true},
                {"SHOW POLICIES ON `db`.`table`", 0, true},
                {"SHOW ROW POLICIES ON table1", 0, true},
                {"SHOW QUOTAS", 0, true},
                {"SHOW CURRENT QUOTA", 0, true},
                {"SHOW QUOTA", 0, true},
                {"SHOW ACCESS", 0, true},
                {"SHOW CLUSTER `default`", 0, true},
                {"SHOW CLUSTERS LIKE 'test%' LIMIT 1", 0, true},
                {"SHOW SETTINGS LIKE 'send_timeout'", 0, true},
                {"SHOW SETTINGS ILIKE '%CONNECT_timeout%'", 0, true},
                {"SHOW CHANGED SETTINGS ILIKE '%MEMORY%'", 0, true},
                {"SHOW SETTING `min_insert_block_size_rows`", 0, true},
                {"SHOW FILESYSTEM CACHES", 0, true},
                {"SHOW ENGINES", 0, true},
                {"SHOW FUNCTIONS", 0, true},
                {"SHOW FUNCTIONS LIKE '%max%'", 0, true},
                {"SHOW MERGES", 0, true},
                {"SHOW MERGES LIKE 'your_t%' LIMIT 1", 0, true},
                {"EXPLAIN SELECT sum(number) FROM numbers(10) GROUP BY number", 0, true},
                {"EXPLAIN SELECT 1", 0, true},
                {"EXPLAIN SELECT sum(number) FROM numbers(10) UNION ALL SELECT sum(number) FROM numbers(10) ORDER BY sum(number) ASC FORMAT TSV", 0, true},
                {"DESCRIBE TABLE table", 0, true},
                {"DESC TABLE table1", 0, true},
                {"EXISTS TABLE `db`.`table01`", 0, true},
                {"CHECK GRANT SELECT(col2) ON table_2", 0, true},
                {"CHECK TABLE test_table", 0, true},
                {"CHECK TABLE t0 PARTITION ID '201003' FORMAT PrettyCompactMonoBlock SETTINGS check_query_single_value_result = 0", 0, true},


                /* no result set */
                {"INSERT INTO test_table VALUES (1, ?)", 1, false},
                {"CREATE DATABASE `test_db`", 0, false},
                {"CREATE DATABASE `test_db` COMMENT 'for tests'", 0, false},
                {"CREATE DATABASE IF NOT EXISTS `test_db`", 0, false},
                {"CREATE DATABASE IF NOT EXISTS `test_db` ON CLUSTER `cluster`", 0, false},
                {"CREATE DATABASE IF NOT EXISTS `test_db` ON CLUSTER `cluster` ENGINE = Replicated('clickhouse1:9000', 'test_db')", 0, false},
                {"CREATE TABLE `test_table` (id UInt64)", 0, false},
                {"CREATE TABLE IF NOT EXISTS `test_table` (id UInt64)", 0, false},
                {"CREATE TABLE `test_table` (id UInt64) ENGINE = MergeTree ORDER BY (id)", 0, false},
                {"CREATE TABLE `test_table` (id UInt64) ENGINE = Memory", 0, false},
                {"CREATE TABLE `test_table` (id UInt64 NOT NULL ) ENGINE = MergeTree ORDER BY id", 0, false},
                {"CREATE TABLE `test_table` (id UInt64 NULL ) ENGINE = MergeTree() ORDER BY id", 0, false},
                {"CREATE TABLE `test_table` (id UInt64) ENGINE = MergeTree() ORDER BY id ON CLUSTER `cluster`", 0, false},
                {"CREATE TABLE `test_table` (id UInt64) ENGINE = MergeTree() ORDER BY id ON CLUSTER `cluster` ENGINE = Replicated('clickhouse1:9000', 'test_db')", 0, false},
                {"CREATE TABLE `test_table` (id UInt64) ENGINE = MergeTree() ORDER BY id ON CLUSTER `cluster` ENGINE = Replicated('clickhouse1:9000', 'test_db') COMMENT 'for tests'", 0, false},
                {"CREATE VIEW `test_db`.`source_table` source AS ( SELECT * FROM source_a UNION SELECT * FROM source_b)", 0, false},
                {"CREATE OR REPLACE VIEW `test_db`.`source_table` source AS ( SELECT * FROM source_a UNION SELECT * FROM source_b)", 0, false},
                {"CREATE OR REPLACE VIEW `test_db`.`source_table` source ON CLUSTER `cluster` AS ( SELECT * FROM source_a UNION SELECT * FROM source_b)", 0, false},
                {"CREATE VIEW `test_db`.`source_table` source AS ( SELECT * FROM source_a UNION SELECT * FROM source_b) ENGINE = MaterializedView", 0, false},
                {"CREATE VIEW `test_db`.`source_table` source AS ( SELECT * FROM source_a UNION SELECT * FROM source_b) ENGINE = MaterializedView()", 0, false},
                {"CREATE VIEW `test_db`.`source_table` source AS ( SELECT * FROM source_a UNION SELECT * FROM source_b) ENGINE = MaterializedView() COMMENT 'for tests'", 0, false},
                { "CREATE DICTIONARY `test_db`.dict1 (k1 UInt64 EXPRESSION(k1 + 1), k2 String DEFAULT 'default', a1 Array(UInt64) DEFAULT []) PRIMARY KEY k1 SOURCE(CLICKHOUSE(db='test_db', table='dict1')) LAYOUT(FLAT()) LIFETIME(MIN 1000 MAX 2000)", 0, false},
                {"CREATE DICTIONARY `test_db`.dict1 (k1 UInt64 (k1 + 1), k2 String DEFAULT 'default', a1 Array(UInt64) DEFAULT []) PRIMARY KEY k1 SOURCE(CLICKHOUSE(db='test_db', table='dict1')) LAYOUT(FLAT()) LIFETIME(MIN 1000 MAX 2000) SETTINGS(cache_size = 1000) COMMENT 'for tests'", 0, false},
                {"CREATE OR REPLACE DICTIONARY IF NOT EXISTS `test_db`.dict1 (k1 UInt64 (k1 + 1), k2 String DEFAULT 'default', a1 Array(UInt64) DEFAULT []) PRIMARY KEY k1 SOURCE(CLICKHOUSE(db='test_db', table='dict1')) LAYOUT(FLAT()) LIFETIME(MIN 1000 MAX 2000) SETTINGS(cache_size = 1000, v='123') COMMENT 'for tests'", 0, false},
                {"CREATE FUNCTION test_func AS () -> 10", 0, false},
                {"CREATE FUNCTION test_func AS (x) -> 10 * x", 0, false},
                {"CREATE FUNCTION test_func AS (x, y) -> y * x", 0, false},
                {"CREATE FUNCTION test_func ON CLUSTER `cluster` AS (x, y) -> y * x", 0, false},
                {"CREATE USER IF NOT EXISTS `user`", 0, false},
                {"CREATE USER IF NOT EXISTS `user` ON CLUSTER `cluster`", 0, false},
                {"CREATE ROLE IF NOT EXISTS `role1` ON CLUSTER", 0, false},
                {"CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter", 0, false},
                {"CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio", 0, false},
                {"CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio", 0, false},
                {"CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO role1, role2", 0, false},
                {"CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO ALL EXCEPT role3", 0, false},
                {"CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER", 0, false},
                {"CREATE QUOTA qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default", 0, false},
                {"CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin", 0, false},
                {"CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE", 0, false},
                {"alter table t2 alter column v type Int32", 0, false},
                {"ALTER TABLE t MODIFY COLUMN j default 1", 0, false},
                {"ALTER TABLE t MODIFY COMMENT 'comment'", 0, false},
                {"DELETE FROM db.table1 ON CLUSTER `default` WHERE max(a, 10) > ?", 1, false},
                {"DELETE FROM table WHERE a = ?", 1, false},
                {"DELETE FROM table WHERE a = ? AND b = ?", 2, false},
                {"DELETE FROM hits WHERE Title LIKE '%hello%';", 0, false},

                {"SYSTEM START FETCHES", 0, false},
                {"SYSTEM RELOAD DICTIONARIES", 0, false},
                {"SYSTEM RELOAD DICTIONARIES ON CLUSTER `default`", 0, false},
                {"GRANT SELECT ON db.* TO john", 0, false},
                {"GRANT ON CLUSTER `default` SELECT(a, b) ON db1.tableA TO `user` WITH GRANT OPTION WITH REPLACE OPTION", 0, false},
                {"GRANT SELECT ON db.* TO user01 WITH REPLACE OPTION", 0, false},
                {"GRANT ON CLUSTER `default` role1, role2 TO `user01` WITH ADMIN OPTION WITH REPLACE OPTION", 0, false},
                {"GRANT role1, role2 TO `user01` WITH ADMIN OPTION WITH REPLACE OPTION", 0, false},
                {"GRANT CURRENT GRANTS TO user01", 0, false},
                {"REVOKE SELECT(a,b) ON db1.tableA FROM `user01`", 0, false},
                {"REVOKE SELECT ON db1.* FROM ALL", 0, false},
                {"REVOKE SELECT ON db1.* FROM ALL EXCEPT `admin01`", 0, false},
                {"REVOKE SELECT ON db1.* FROM ALL EXCEPT CURRENT USER", 0, false},
                {"REVOKE ON CLUSTER `default` SELECT ON db1.* FROM ALL EXCEPT CURRENT USER", 0, false},
                {"REVOKE ON CLUSTER `blaster` ADMIN OPTION FOR role1, role3 FROM `user01`", 0, false},
                {"REVOKE ON CLUSTER `blaster` role1, role3 FROM ALL EXCEPT CURRENT USER", 0, false},
                {"REVOKE ON CLUSTER `blaster` role1, role3 FROM ALL EXCEPT `very_nice_user`", 0, false},
                {"UPDATE db.table01 ON CLUSTER `default` SET col1 = ?, col2 = ? WHERE col3 > ?", 3, false},
                {"UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today()", 0, false},
                {"ATTACH TABLE test FROM '01188_attach/test' (s String, n UInt8) ENGINE = File(TSV)", 0, false},
                {"ATTACH TABLE test AS REPLICATED", 0, false},
                {"DETACH TABLE test", 0, false},
                {"ATTACH DICTIONARY IF NOT EXISTS db.dict1 ON CLUSTER `default`", 0, false},
                {"ATTACH DATABASE IF NOT EXISTS db1 ENGINE=MergeTree ON CLUSTER `default`", 0, false},
                {"DROP DATABASE `db1`",0, false},
                {"DROP TABLE `db1`.`table01`", 0, false},
                {"DROP DICTIONARY `dict1`", 0, false},
                {"DROP ROLE IF EXISTS `role01`", 0 , false},
                {"DROP POLICY IF EXISTS `pol1` ON db1.table1 ON CLUSTER `default` FROM `test`", 0, false},
                {"DROP POLICY IF EXISTS `pol1` ON db1.table1 ON CLUSTER `default`", 0, false},
                {"DROP POLICY IF EXISTS `pol1` ON table1", 0, false},
                {"DROP QUOTA IF EXISTS q1", 0, false},
                {"DROP SETTINGS PROFILE IF EXISTS `profile1` ON CLUSTER `default`", 0, false},
                {"DROP VIEW view1 ON CLUSTER `default` SYNC", 0, false},
                {"DROP FUNCTION linear_equation", 0, false},
                {"DROP NAMED COLLECTION foobar", 0, false},
                {"KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'", 0, false},
                {"KILL QUERY WHERE user='username' SYNC", 0, false},
                {"KILL QUERY ON CLUSTER `default` WHERE user='username' SYNC", 0, false},
                {"KILL QUERY ON CLUSTER `default` WHERE user='username' ASYNC", 0, false},
                {"KILL QUERY ON CLUSTER `default` WHERE user='username' TEST", 0, false},
                {"KILL MUTATION WHERE database = 'default' AND table = 'table'", 0, false},
                {"KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'", 0, false},
                {"OPTIMIZE TABLE table DEDUPLICATE BY colX,colY,colZ", 0, false},
                {"OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX", 0, false},
                {"OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY)", 0, false},
                {"OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex')", 0, false},
                {"OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX", 0, false},
                {"OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY)", 0, false},
                {"OPTIMIZE TABLE table DEDUPLICATE", 0, false},
                {"OPTIMIZE TABLE table DEDUPLICATE BY *", 0, false},
                {"RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak", 0, false},
                {"RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak ON CLUSTER `default`", 0, false},
                {"RENAME DICTIONARY dictA TO dictB ON CLUSTER `default`", 0, false},
                {"EXCHANGE TABLES table1 AND table2", 0, false},
                {"EXCHANGE TABLES table1 AND table2 ON CLUSTER `default`", 0, false},
                {"EXCHANGE DICTIONARIES dict1 AND dict2", 0, false},
                {"EXCHANGE DICTIONARIES dict1 AND dict2 ON CLUSTER `default`", 0, false},
                {"SET profile = 'profile-name-from-the-settings-file'", 0, false},
                {"SET use_some_feature_flag", 0, false},
                {"SET use_some_feature_flag = 'true'", 0, false},
                {"SET ROLE role1", 0, false},
                {"SET DEFAULT ROLE role1 TO user", 0, false},
                {"SET DEFAULT ROLE NONE TO user", 0, false},
                {"SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user", 0, false},
                {"TRUNCATE TABLE IF EXISTS `db1`.`table1` ON CLUSTER `default` SYNC", 0, false},
                {"TRUNCATE TABLE `db1`.`table1` ON CLUSTER `default` SYNC", 0, false},
                {"TRUNCATE TABLE `db1`.`table1` ON CLUSTER `default`", 0, false},
                {"TRUNCATE TABLE `db1`.`table1`", 0, false},
                {"TRUNCATE DATABASE IF EXISTS db ON CLUSTER `cluster`", 0, false},
                {"TRUNCATE DATABASE IF EXISTS db", 0, false},
                {"TRUNCATE DATABASE `db`", 0, false},
                {"TRUNCATE ALL TABLES FROM IF EXISTS `db` NOT LIKE 'tmp%' ON CLUSTER `cluster`", 0, false},
                {"TRUNCATE ALL TABLES FROM IF EXISTS `db` NOT LIKE 'tmp%'", 0, false},
                {"TRUNCATE ALL TABLES FROM `db` NOT LIKE 'tmp%' ON CLUSTER `cluster`", 0, false},
                {"TRUNCATE TABLES FROM `db` LIKE 'tmp%' ON CLUSTER `cluster`", 0, false},
                {"TRUNCATE TABLES FROM `db` LIKE 'tmp%'", 0, false},
                {"USE test_db", 0, false},
                {"MOVE USER test TO local_directory", 0, false},
                {"MOVE ROLE test TO memory", 0, false},
                {"UNDROP TABLE tab", 0, false},
                {"UNDROP TABLE db.tab ON CLUSTER `default`", 0, false},
                {"UNDROP TABLE db.tab UUID '857d-4a57-9ee0-327da5d60a90' ON CLUSTER `default`", 0, false},

        };
    }
}