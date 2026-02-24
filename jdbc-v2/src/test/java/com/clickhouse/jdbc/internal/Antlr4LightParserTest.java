package com.clickhouse.jdbc.internal;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class Antlr4LightParserTest extends BaseSqlParserFacadeTest {
    public Antlr4LightParserTest() throws Exception {
        super(SqlParserFacade.SQLParser.ANTLR4_LIGHT.name());
    }

    @Test
    public void testOtherStatementSavesFirstVerb() throws Exception {
        SqlParserFacade parser = SqlParserFacade.getParser(SqlParserFacade.SQLParser.ANTLR4_LIGHT.name());
        ParsedPreparedStatement stmt = parser.parsePreparedStatement("WITH cte AS (SELECT 1) SELECT * FROM cte");
        Assert.assertEquals(stmt.getStatementVerb(), "WITH");
        Assert.assertTrue(stmt.isHasResultSet());

        stmt = parser.parsePreparedStatement("SHOW TABLES");
        Assert.assertEquals(stmt.getStatementVerb(), "SHOW");
        Assert.assertTrue(stmt.isHasResultSet());
    }

    @Test
    public void testUseAndInsertAreParsed() throws Exception {
        SqlParserFacade parser = SqlParserFacade.getParser(SqlParserFacade.SQLParser.ANTLR4_LIGHT.name());

        ParsedStatement useStmt = parser.parsedStatement("USE analytics");
        Assert.assertEquals(useStmt.getStatementVerb(), "USE");
        Assert.assertEquals(useStmt.getUseDatabase(), "analytics");

        ParsedPreparedStatement insertStmt = parser.parsePreparedStatement("INSERT INTO db1.t1 (id, name) VALUES (?, ?)");
        Assert.assertEquals(insertStmt.getStatementVerb(), "INSERT");
        Assert.assertTrue(insertStmt.isInsert());
        Assert.assertEquals(insertStmt.getTable(), "db1.t1");
        Assert.assertEquals(insertStmt.getInsertColumns().length, 2);
    }

    @Test
    public void testMultiDotNotation() {
        SqlParserFacade parser = lightParser();

        // Test INSERT with multi-dot notation
        String sql3 = "INSERT INTO a.b.c (col1, col2) VALUES (?, ?)";
        ParsedPreparedStatement stmt3 = parser.parsePreparedStatement(sql3);
        Assert.assertEquals(stmt3.getArgCount(), 2);
        Assert.assertFalse(stmt3.isHasErrors());
        Assert.assertTrue(stmt3.isInsert());
        Assert.assertEquals(stmt3.getTable(), "a.b.c");
    }

    @Test
    public void testQuotedIdentifiersWithDots() {
        SqlParserFacade parser = lightParser();
        /*
         * Comprehensive test for quoted identifiers containing dots.
         * These cases are all valid in ClickHouse with MySQL-style backtick quoting.
         */

        // Case 1: Unquoted database + unquoted table
        testParsedTableName(parser, "INSERT INTO db.table (id) VALUES (?)", "db.table");

        // Case 2: Quoted database + quoted table
        testParsedTableName(parser, "INSERT INTO `db`.`table` (id) VALUES (?)", "db.table");

        // Case 3: Dots inside quoted table name
        testParsedTableName(parser, "INSERT INTO db.`table.name` (id) VALUES (?)", "db.table.name");

        // Case 4: Dots inside quoted database name
        testParsedTableName(parser, "INSERT INTO `db.part1`.`table` (id) VALUES (?)", "db.part1.table");

        // Case 5: Mixed quoted/unquoted identifiers
        testParsedTableName(parser, "INSERT INTO db.`table.name` (id) VALUES (?)", "db.table.name");

        // Case 6: Mixed quoted/unquoted (reverse)
        testParsedTableName(parser, "INSERT INTO `db.part1`.table (id) VALUES (?)", "db.part1.table");

        // Case 7: Escaped backticks inside quoted identifier
        testParsedTableName(parser, "INSERT INTO db.`tab``le` (id) VALUES (?)", "db.tab`le");

        // Case 8: Weird characters inside quoted identifiers (spaces, symbols)
        testParsedTableName(parser, "INSERT INTO `my db`.`table name!@#` (id) VALUES (?)", "my db.table name!@#");

        // Case 9: Quoted database and table with dots
        testParsedTableName(parser, "INSERT INTO `db.part1`.`table.name` (id) VALUES (?)", "db.part1.table.name");

        // Case 10: Quoted table name containing multiple dots
        testParsedTableName(parser, "INSERT INTO db.`a.b.c.d` (id) VALUES (?)", "db.a.b.c.d");

        // Case 11: Quoted database name containing multiple dots
        testParsedTableName(parser, "INSERT INTO `db.part1.part2`.`table` (id) VALUES (?)", "db.part1.part2.table");

        // Case 12: Multi-part unquoted chain (3-part identifier)
        testParsedTableName(parser, "INSERT INTO db.part1.table2 (id) VALUES (?)", "db.part1.table2");

        // Case 13: Multi-part quoted chain
        testParsedTableName(parser, "INSERT INTO `db.part1`.`part2`.`table` (id) VALUES (?)", "db.part1.part2.table");

        // Case 14: Mixed multi-part unquoted + quoted
        testParsedTableName(parser, "INSERT INTO db.part1.`table.name` (id) VALUES (?)", "db.part1.table.name");

        // Case 15: Mixed multi-part quoted + unquoted
        testParsedTableName(parser, "INSERT INTO `db.part1`.part2.table3 (id) VALUES (?)", "db.part1.part2.table3");
    }

    private void testParsedTableName(SqlParserFacade parser, String sql, String expectedTableName) {
        ParsedPreparedStatement stmt = parser.parsePreparedStatement(sql);
        Assert.assertFalse(stmt.isHasErrors(), "Query should parse without errors: " + sql);
        Assert.assertEquals(stmt.getTable(), expectedTableName, "Table name mismatch for: " + sql);
    }

    private SqlParserFacade lightParser() {
        try {
            return SqlParserFacade.getParser(SqlParserFacade.SQLParser.ANTLR4_LIGHT.name());
        } catch (Exception e) {
            throw new RuntimeException("Unable to create ANTLR4 light parser", e);
        }
    }


    @DataProvider
    public static Object[][] testStatementWithoutResultSetDP() {
        return new Object[][]{
                /* has result set */
                {"SELECT * FROM test_table", 0, true},
                {"SELECT 1 table WHERE 1 = ?", 1, true},
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


                /* parser fallback because SYSTEM keyword can be used with result set queries */
                {"SYSTEM START FETCHES", 0, true},
                {"SYSTEM RELOAD DICTIONARIES", 0, true},
                {"SYSTEM RELOAD DICTIONARIES ON CLUSTER `default`", 0, true},

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
                {"CREATE TABLE myusers ( id UInt64, ip String, url String, tenant String) ENGINE = MergeTree() PRIMARY KEY (id)", 0, false},
                {"CREATE VIEW `test_db`.`source_table` source AS ( SELECT * FROM source_a UNION SELECT * FROM source_b)", 0, false},
                {"CREATE OR REPLACE VIEW `test_db`.`source_table` source AS ( SELECT * FROM source_a UNION SELECT * FROM source_b)", 0, false},
                {"CREATE OR REPLACE VIEW `test_db`.`source_table` source ON CLUSTER `cluster` AS ( SELECT * FROM source_a UNION SELECT * FROM source_b)", 0, false},
                {"CREATE VIEW `test_db`.`source_table` source AS ( SELECT * FROM source_a UNION SELECT * FROM source_b) ENGINE = MaterializedView", 0, false},
                {"CREATE VIEW `test_db`.`source_table` source AS ( SELECT * FROM source_a UNION SELECT * FROM source_b) ENGINE = MaterializedView()", 0, false},
                {"CREATE VIEW `test_db`.`source_table` source AS ( SELECT * FROM source_a UNION SELECT * FROM source_b) ENGINE = MaterializedView() COMMENT 'for tests'", 0, false},
                {"CREATE DICTIONARY `test_db`.dict1 (k1 UInt64 EXPRESSION(k1 + 1), k2 String DEFAULT 'default', a1 Array(UInt64) DEFAULT []) PRIMARY KEY k1 SOURCE(CLICKHOUSE(db='test_db', table='dict1')) LAYOUT(FLAT()) LIFETIME(MIN 1000 MAX 2000)", 0, false},
                {"CREATE DICTIONARY `test_db`.dict1 (k1 UInt64 (k1 + 1), k2 String DEFAULT 'default', a1 Array(UInt64) DEFAULT []) PRIMARY KEY k1 SOURCE(CLICKHOUSE(db='test_db', table='dict1')) LAYOUT(FLAT()) LIFETIME(MIN 1000 MAX 2000) SETTINGS(cache_size = 1000) COMMENT 'for tests'", 0, false},
                {"CREATE OR REPLACE DICTIONARY IF NOT EXISTS `test_db`.dict1 (k1 UInt64 (k1 + 1), k2 String DEFAULT 'default', a1 Array(UInt64) DEFAULT []) PRIMARY KEY k1 SOURCE(CLICKHOUSE(db='test_db', table='dict1')) LAYOUT(FLAT()) LIFETIME(MIN 1000 MAX 2000) SETTINGS(cache_size = 1000, v='123') COMMENT 'for tests'", 0, false},
                {"CREATE FUNCTION test_func AS () -> 10", 0, false},
                {"CREATE FUNCTION test_func AS (x) -> 10 * x", 0, false},
                {"CREATE FUNCTION test_func AS (x, y) -> y * x", 0, false},
                {"CREATE FUNCTION test_func ON CLUSTER `cluster` AS (x, y) -> y * x", 0, false},
                {"CREATE USER IF NOT EXISTS `user`", 0, false},
                {"CREATE USER IF NOT EXISTS `user` ON CLUSTER `cluster`", 0, false},
                {"CREATE ROLE IF NOT EXISTS `role1` ON CLUSTER 'cluster'", 0, false},
                {"CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter", 0, false},
                {"CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio", 0, false},
                {"CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio", 0, false},
                {"CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO role1, role2", 0, false},
                {"CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO ALL EXCEPT role3", 0, false},
                {"CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER", 0, false},
                {"CREATE QUOTA qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default", 0, false},
                {"CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin", 0, false},
                {"CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE", 0, false},
                {"CREATE TABLE IF NOT EXISTS statistics.vast_event_v2 (ip UInt32, url String) ENGINE = ReplacingMergeTree()", 0, false},
                {"CREATE TABLE IF NOT EXISTS `statistics`.vast_event_v2 (ip UInt32, url String) ENGINE = ReplacingMergeTree()", 0, false},
                {"alter table t2 alter column v type Int32", 0, false},
                {"alter table t alter column j default 1", 0, false},
                {"ALTER TABLE t MODIFY COLUMN j default 1", 0, false},
                {"ALTER TABLE t MODIFY COMMENT 'comment'", 0, false},
                {"ALTER TABLE t ADD COLUMN id Int32 AFTER v", 0, false},
                {"ALTER TABLE t ADD COLUMN id Int32 FIRST", 0, false},
                {"DELETE FROM db.table1 ON CLUSTER `default` WHERE max(a, 10) > ?", 1, false},
                {"DELETE FROM table WHERE a = ?", 1, false},
                {"DELETE FROM table WHERE a = ? AND b = ?", 2, false},
                {"DELETE FROM hits WHERE Title LIKE '%hello%';", 0, false},
                {"DELETE FROM t WHERE true", 0, false},

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
                {"DROP DATABASE `db1`", 0, false},
                {"DROP TABLE `db1`.`table01`", 0, false},
                {"DROP DICTIONARY `dict1`", 0, false},
                {"DROP ROLE IF EXISTS `role01`", 0, false},
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
                {"SET setting_1 = 'some value'", 0, false},
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
                {"TRUNCATE TEMPORARY TABLE t", 0, false},
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
