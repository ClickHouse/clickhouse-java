package com.clickhouse.jdbc.parser;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.clickhouse.client.ClickHouseConfig;

public class ClickHouseSqlParserTest {
    private ClickHouseSqlStatement[] parse(String sql) {
        return ClickHouseSqlParser.parse(sql, new ClickHouseConfig());
    }

    private String loadSql(String file) {
        InputStream inputStream = ClickHouseSqlParserTest.class.getResourceAsStream("/sqls/" + file);

        StringBuilder sql = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                sql.append(line).append("\n");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        return sql.toString();
    }

    private ClickHouseSqlStatement checkSingleStatement(ClickHouseSqlStatement[] stmts, String sql) {
        return checkSingleStatement(stmts, sql, StatementType.UNKNOWN, ClickHouseSqlStatement.DEFAULT_DATABASE,
                ClickHouseSqlStatement.DEFAULT_TABLE);
    }

    private ClickHouseSqlStatement checkSingleStatement(ClickHouseSqlStatement[] stmts, String sql,
            StatementType stmtType) {
        return checkSingleStatement(stmts, sql, stmtType, ClickHouseSqlStatement.DEFAULT_DATABASE,
                ClickHouseSqlStatement.DEFAULT_TABLE);
    }

    private ClickHouseSqlStatement checkSingleStatement(ClickHouseSqlStatement[] stmts, String sql,
            StatementType stmtType,
            String database, String table) {
        assertEquals(stmts.length, 1);

        ClickHouseSqlStatement s = stmts[0];
        assertEquals(s.getSQL(), sql);
        assertEquals(s.getStatementType(), stmtType);
        assertEquals(s.getDatabaseOrDefault(null), database);
        assertEquals(s.getTable(), table);

        return stmts[0];
    }

    @Test(groups = "unit")
    public void testParseNonSql() throws ParseException {
        String sql;

        assertEquals(parse(sql = null),
                new ClickHouseSqlStatement[] {
                        new ClickHouseSqlStatement(sql, StatementType.UNKNOWN) });
        assertEquals(parse(sql = ""),
                new ClickHouseSqlStatement[] {
                        new ClickHouseSqlStatement(sql, StatementType.UNKNOWN) });

        checkSingleStatement(parse(sql = "invalid sql"), sql);
        checkSingleStatement(parse(sql = "-- some comments"), sql);
        checkSingleStatement(parse(sql = "/*********\r\n\r\t some ***** comments*/"), sql);

        checkSingleStatement(parse(sql = "select"), sql, StatementType.UNKNOWN);
        checkSingleStatement(parse(sql = "select ()"), sql, StatementType.UNKNOWN);
        checkSingleStatement(parse(sql = "select (()"), sql, StatementType.UNKNOWN);
        checkSingleStatement(parse(sql = "select [[]"), sql, StatementType.UNKNOWN);
        // checkSingleStatement(parse(sql = "select 1 select"), sql,
        // StatementType.UNKNOWN);
    }

    @Test(groups = "unit")
    public void testAlterStatement() {
        String sql;

        checkSingleStatement(parse(sql = "ALTER TABLE alter_test ADD COLUMN Added0 UInt32"), sql,
                StatementType.ALTER,
                "system", "alter_test");
        checkSingleStatement(
                parse(sql = "ALTER TABLE test_db.test_table UPDATE a = 1, \"b\" = '2', `c`=3.3 WHERE d=123 and e=456"),
                sql, StatementType.ALTER_UPDATE, "test_db", "test_table");
        checkSingleStatement(parse(sql = "ALTER TABLE tTt on cluster 'cc' delete WHERE d=123 and e=456"), sql,
                StatementType.ALTER_DELETE, "system", "tTt");
        checkSingleStatement(parse(sql = "ALTER USER user DEFAULT ROLE role1, role2"), sql,
                StatementType.ALTER);
    }

    @Test(groups = "unit")
    public void testAttachStatement() {
        String sql;

        checkSingleStatement(parse(sql = "ATTACH TABLE IF NOT EXISTS t.t ON CLUSTER cluster"), sql,
                StatementType.ATTACH);
    }

    @Test(groups = "unit")
    public void testCheckStatement() {
        String sql;

        checkSingleStatement(parse(sql = "check table a"), sql, StatementType.CHECK);
        checkSingleStatement(parse(sql = "check table a.a"), sql, StatementType.CHECK);
    }

    @Test(groups = "unit")
    public void testCreateStatement() {
        String sql;

        checkSingleStatement(parse(sql = "create table a(a String) engine=Memory"), sql, StatementType.CREATE);
    }

    @Test(groups = "unit")
    public void testDeleteStatement() {
        String sql;

        checkSingleStatement(parse(sql = "delete from a"), sql, StatementType.DELETE, "system", "a");
        checkSingleStatement(parse(sql = "delete from c.a where upper(a)=upper(lower(b))"), sql,
                StatementType.DELETE,
                "c", "a");
    }

    @Test(groups = "unit")
    public void testDescribeStatement() {
        String sql;

        checkSingleStatement(parse(sql = "desc a"), sql, StatementType.DESCRIBE, "system", "columns");
        checkSingleStatement(parse(sql = "desc table a"), sql, StatementType.DESCRIBE, "system", "columns");
        checkSingleStatement(parse(sql = "describe table a.a"), sql, StatementType.DESCRIBE, "a", "columns");
        checkSingleStatement(parse(sql = "desc table table"), sql, StatementType.DESCRIBE, "system", "columns");
        // fix issue #614
        checkSingleStatement(parse(sql = "desc t1 t2"), sql, StatementType.DESCRIBE, "system", "columns");
        checkSingleStatement(parse(sql = "desc table t1 t2"), sql, StatementType.DESCRIBE, "system", "columns");
        checkSingleStatement(parse(sql = "desc table t1 as `t2`"), sql, StatementType.DESCRIBE, "system",
                "columns");
    }

    @Test(groups = "unit")
    public void testDetachStatement() {
        String sql;

        checkSingleStatement(parse(sql = "detach TABLE t"), sql, StatementType.DETACH);
        checkSingleStatement(parse(sql = "detach TABLE if exists t.t on cluster 'cc'"), sql,
                StatementType.DETACH);
    }

    @Test(groups = "unit")
    public void testDropStatement() {
        String sql;

        checkSingleStatement(parse(sql = "drop TEMPORARY table t"), sql, StatementType.DROP);
        checkSingleStatement(parse(sql = "drop TABLE if exists t.t on cluster 'cc'"), sql, StatementType.DROP);
    }

    @Test(groups = "unit")
    public void testExistsStatement() {
        String sql;

        checkSingleStatement(parse(sql = "EXISTS TEMPORARY TABLE a"), sql, StatementType.EXISTS);
        checkSingleStatement(parse(sql = "EXISTS TABLE a.a"), sql, StatementType.EXISTS);
        checkSingleStatement(parse(sql = "EXISTS DICTIONARY c"), sql, StatementType.EXISTS);
    }

    @Test(groups = "unit")
    public void testExplainStatement() {
        String sql;

        checkSingleStatement(parse(
                sql = "EXPLAIN SELECT sum(number) FROM numbers(10) UNION ALL SELECT sum(number) FROM numbers(10) ORDER BY sum(number) ASC FORMAT TSV"),
                sql, StatementType.EXPLAIN);
        checkSingleStatement(parse(sql = "EXPLAIN AST SELECT 1"), sql, StatementType.EXPLAIN);
        checkSingleStatement(parse(
                sql = "EXPLAIN SYNTAX SELECT * FROM system.numbers AS a, system.numbers AS b, system.numbers AS c"),
                sql, StatementType.EXPLAIN);
    }

    @Test(groups = "unit")
    public void testGrantStatement() {
        String sql;

        checkSingleStatement(parse(sql = "GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION"), sql,
                StatementType.GRANT);
        checkSingleStatement(parse(sql = "GRANT INSERT(x,y) ON db.table TO john"), sql, StatementType.GRANT);
    }

    @Test(groups = "unit")
    public void testInsertStatement() throws ParseException {
        String sql;

        ClickHouseSqlStatement s = parse(sql = "insert into table test(a,b) Values (1,2)")[0];
        assertEquals(sql.substring(s.getStartPosition("values"), s.getEndPosition("VALUES")), "Values");
        assertEquals(sql.substring(0, s.getEndPosition("values")) + " (1,2)", sql);

        Pattern values = Pattern.compile("(?i)VALUES[\\s]*\\(");
        int valuePosition = -1;
        Matcher matcher = values.matcher(sql);
        if (matcher.find()) {
            valuePosition = matcher.start();
        }
        assertEquals(s.getStartPosition("values"), valuePosition);

        s = checkSingleStatement(parse(sql = "insert into function null('a UInt8') values(1)"), sql,
                StatementType.INSERT);
        Assert.assertEquals(s.getContentBetweenKeywords(ClickHouseSqlStatement.KEYWORD_VALUES_START,
                ClickHouseSqlStatement.KEYWORD_VALUES_END, 1), "1");
        s = checkSingleStatement(parse(sql = "insert into function null('a UInt8') values(1)(2)"), sql,
                StatementType.INSERT);
        Assert.assertEquals(s.getContentBetweenKeywords(ClickHouseSqlStatement.KEYWORD_VALUES_START,
                ClickHouseSqlStatement.KEYWORD_VALUES_END, 1), "");
        checkSingleStatement(parse(sql = "insert into function null('a UInt8') select * from number(10)"), sql,
                StatementType.INSERT);
        checkSingleStatement(parse(sql = "insert into test2(a,b) values('values(',',')"), sql,
                StatementType.INSERT, "system", "test2");
        checkSingleStatement(parse(sql = "INSERT INTO table t(a, b, c) values('1', ',', 'ccc')"), sql,
                StatementType.INSERT, "system", "t");
        checkSingleStatement(parse(sql = "INSERT INTO table t(a, b, c) values('1', 2, 'ccc') (3,2,1)"), sql,
                StatementType.INSERT, "system", "t");
        checkSingleStatement(parse(sql = "INSERT INTO table s.t select * from ttt"), sql, StatementType.INSERT,
                "s", "t");
        checkSingleStatement(parse(sql = "INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2)"),
                sql, StatementType.INSERT, "system", "insert_select_testtable");
        checkSingleStatement(
                parse(sql = "insert into `test` (num)SETTINGS input_format_null_as_default = 1 values (?)"),
                sql, StatementType.INSERT, "system", "test");
        checkSingleStatement(
                parse(sql = "insert into `test` (id, name) SETTINGS input_format_null_as_default = 1 values (1,2)(3,4),(5,6)"),
                sql, StatementType.INSERT, "system", "test");
        s = checkSingleStatement(
                parse(sql = "insert into `test`"), sql, StatementType.INSERT, "system", "test");
        Assert.assertEquals(s.getContentBetweenKeywords(ClickHouseSqlStatement.KEYWORD_TABLE_COLUMNS_START,
                ClickHouseSqlStatement.KEYWORD_TABLE_COLUMNS_END, 1), "");
        s = checkSingleStatement(
                parse(sql = "insert into `test` (id, name) format RowBinary"),
                sql, StatementType.INSERT, "system", "test");
        Assert.assertEquals(s.getContentBetweenKeywords(ClickHouseSqlStatement.KEYWORD_TABLE_COLUMNS_START,
                ClickHouseSqlStatement.KEYWORD_TABLE_COLUMNS_END, 1), "id, name");
    }

    @Test(groups = "unit")
    public void testKillStatement() {
        String sql;

        checkSingleStatement(parse(sql = "KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'"), sql,
                StatementType.KILL);
        checkSingleStatement(parse(
                sql = "KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt' SYNC"),
                sql, StatementType.KILL);
    }

    @Test(groups = "unit")
    public void testOptimizeStatement() {
        String sql;

        checkSingleStatement(
                parse(sql = "OPTIMIZE TABLE a ON CLUSTER cluster PARTITION ID 'partition_id' FINAL"),
                sql,
                StatementType.OPTIMIZE);
    }

    @Test(groups = "unit")
    public void testRenameStatement() {
        String sql;

        checkSingleStatement(parse(sql = "RENAME TABLE table1 TO table2, table3 TO table4 ON CLUSTER cluster"),
                sql,
                StatementType.RENAME);
        checkSingleStatement(parse(
                sql = "RENAME TABLE db1.table1 TO db2.table2, db2.table3 to db2.table4, db3.table5 to db2.table6 ON CLUSTER 'c'"),
                sql, StatementType.RENAME);
    }

    @Test(groups = "unit")
    public void testRevokeStatement() {
        String sql;

        checkSingleStatement(parse(sql = "REVOKE SELECT ON accounts.* FROM john"), sql, StatementType.REVOKE);
        checkSingleStatement(parse(sql = "REVOKE SELECT(wage) ON accounts.staff FROM mira"), sql,
                StatementType.REVOKE);
    }

    @Test(groups = "unit")
    public void testSelectStatement() {
        String sql;

        assertEquals(parse(sql = "select\n1"), new ClickHouseSqlStatement[] { new ClickHouseSqlStatement(sql,
                StatementType.SELECT, null, null, "unknown", null, null, null, null, null, null, null, null, null) });
        assertEquals(parse(sql = "select\r\n1"), new ClickHouseSqlStatement[] { new ClickHouseSqlStatement(sql,
                StatementType.SELECT, null, null, "unknown", null, null, null, null, null, null, null, null, null) });

        assertEquals(parse(sql = "select 314 limit 5\nFORMAT JSONCompact;"),
                new ClickHouseSqlStatement[] {
                        new ClickHouseSqlStatement("select 314 limit 5\nFORMAT JSONCompact",
                                StatementType.SELECT, null, null, "unknown", null, null, null,
                                "JSONCompact", null, null, Collections.singletonMap("FORMAT", 19), null, null) });

        checkSingleStatement(parse(sql = "select (())"), sql, StatementType.SELECT);
        checkSingleStatement(parse(sql = "select []"), sql, StatementType.SELECT);
        checkSingleStatement(parse(sql = "select [[]]"), sql, StatementType.SELECT);
        checkSingleStatement(parse(sql = "select *"), sql, StatementType.SELECT);
        checkSingleStatement(parse(sql = "select timezone()"), sql, StatementType.SELECT);
        checkSingleStatement(parse(sql = "select @@version, $version"), sql, StatementType.SELECT);
        checkSingleStatement(parse(sql = "select * from jdbc('db', 'schema', 'select 1')"), sql,
                StatementType.SELECT,
                "system", "jdbc");
        checkSingleStatement(parse(sql = "select 1 as a1, a.a as a2, aa(a1, a2) a3, length(a3) as a4 from x"),
                sql,
                StatementType.SELECT, "system", "x");
        checkSingleStatement(parse(sql = "select x.* from (select [1,2] a, (1,2,3) b, a[1], b.2) x"), sql,
                StatementType.SELECT, "system", "x");
        checkSingleStatement(parse(sql = "select (3, [[1,2],[3,4]]) as a, (a.2)[2][1]"), sql,
                StatementType.SELECT);
        checkSingleStatement(
                parse(sql = "select 1,1.1,'\"''`a' a, \"'`\"\"a\" as b, (1 + `a`.a) c, null, inf i, nan as n"),
                sql,
                StatementType.SELECT);
        checkSingleStatement(parse(sql = "select 1 as select"), sql, StatementType.SELECT);
        checkSingleStatement(parse(sql = "select 1, 2 a, 3 as b, 1+1-2*3/4, *, c.* from c a"), sql,
                StatementType.SELECT, "system", "c");
        checkSingleStatement(parse(sql = "select 1 as select"), sql, StatementType.SELECT);
        checkSingleStatement(parse(
                sql = "   -- cc\nselect 1 as `a.b`, a, 1+1, b from \"a\".`b` inner join a on a.abb/* \n\r\n1*/\n=2 and a.abb = c.a and a=1 and (k is null and j not in(1,2))"),
                sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "SELECT idx, s FROM test.mymetadata WHERE idx = ?"), sql,
                StatementType.SELECT,
                "test", "mymetadata");
        checkSingleStatement(parse(sql = "WITH 2 AS two SELECT two * two"), sql, StatementType.SELECT);
        checkSingleStatement(parse(
                sql = "SELECT i, array(toUnixTimestamp(dt_server[1])), array(toUnixTimestamp(dt_berlin[1])), array(toUnixTimestamp(dt_lax[1])) FROM test.fun_with_timezones_array"),
                sql, StatementType.SELECT, "test", "fun_with_timezones_array");
        checkSingleStatement(parse(sql = "SELECT SUM(x) FROM t WHERE y = ? GROUP BY ?"), sql,
                StatementType.SELECT,
                "system", "t");

        assertEquals(parse(sql = loadSql("issue-441_with-totals.sql")),
                new ClickHouseSqlStatement[] { new ClickHouseSqlStatement(sql, StatementType.SELECT,
                        null, null, "unknown", null, null, null, null, null, null, new HashMap<String, Integer>() {
                            {
                                put("TOTALS", 208);
                            }
                        }, null, null) });
        assertEquals(parse(sql = loadSql("issue-555_custom-format.sql")),
                new ClickHouseSqlStatement[] {
                        new ClickHouseSqlStatement(sql, StatementType.SELECT, null, null, "wrd", null, null, null,
                                "CSVWithNames", null, null, Collections.singletonMap("FORMAT", 1557), null, null) });
        assertEquals(parse(sql = loadSql("with-clause.sql")),
                new ClickHouseSqlStatement[] {
                        new ClickHouseSqlStatement(sql, StatementType.SELECT, null, null, "unknown", null, null, null,
                                null, null, null, null, null, null) });
    }

    @Test(groups = "unit")
    public void testSetStatement() {
        String sql;

        checkSingleStatement(parse(sql = "SET profile = 'my-profile', mutations_sync=1"), sql,
                StatementType.SET);
        checkSingleStatement(parse(sql = "SET DEFAULT ROLE role1, role2, role3 TO user"), sql,
                StatementType.SET);
    }

    @Test(groups = "unit")
    public void testShowStatement() {
        String sql;

        checkSingleStatement(parse(sql = "SHOW DATABASES LIKE '%de%'"), sql, StatementType.SHOW, "system",
                "databases");
        checkSingleStatement(parse(sql = "show tables from db"), sql, StatementType.SHOW, "system", "tables");
        checkSingleStatement(parse(sql = "show dictionaries from db"), sql, StatementType.SHOW, "system",
                "dictionaries");
    }

    @Test(groups = "unit")
    public void testSystemStatement() {
        String sql;

        checkSingleStatement(
                parse(sql = "SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk'"),
                sql,
                StatementType.SYSTEM);
        checkSingleStatement(parse(sql = "SYSTEM RESTART REPLICA db.replicated_merge_tree_family_table_name"),
                sql,
                StatementType.SYSTEM);
    }

    @Test(groups = "unit")
    public void testTruncateStatement() {
        String sql;

        checkSingleStatement(parse(sql = "truncate table a.b"), sql, StatementType.TRUNCATE, "a", "b");
    }

    @Test(groups = "unit")
    public void testUpdateStatement() {
        String sql;

        checkSingleStatement(parse(sql = "update a set a='1'"), sql, StatementType.UPDATE,
                ClickHouseSqlStatement.DEFAULT_DATABASE, "a");
        checkSingleStatement(parse(sql = "update a.a set `a`=2 where upper(a)=upper(lower(b))"), sql,
                StatementType.UPDATE, "a", "a");
    }

    @Test(groups = "unit")
    public void testUseStatement() throws ParseException {
        String sql;
        checkSingleStatement(parse(sql = "use system"), sql, StatementType.USE);
    }

    @Test(groups = "unit")
    public void testWatchStatement() throws ParseException {
        String sql;
        checkSingleStatement(parse(sql = "watch system.processes"), sql, StatementType.WATCH);
    }

    @Test(groups = "unit")
    public void testComments() throws ParseException {
        String sql;
        checkSingleStatement(parse(sql = "select\n--something\n//else\n1/*2*/ from a.b"), sql,
                StatementType.SELECT,
                "a", "b");

        checkSingleStatement(parse(sql = "select 1/*/**/*/ from a.b"), sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "select 1/*/1/**/*2*/ from a.b"), sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "SELECT /*/**/*/ 1 from a.b"), sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "SELECT /*a/*b*/c*/ 1 from a.b"), sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "SELECT /*ab/*cd*/ef*/ 1 from a.b"), sql, StatementType.SELECT, "a",
                "b");
    }

    @Test(groups = "unit")
    public void testMultipleStatements() throws ParseException {
        assertEquals(parse("use ab;;;select 1; ;\t;\r;\n"),
                new ClickHouseSqlStatement[] {
                        new ClickHouseSqlStatement("use ab", StatementType.USE, null, "ab",
                                null, null, null, null, null, null, null, null, null, null),
                        new ClickHouseSqlStatement("select 1", StatementType.SELECT) });
        assertEquals(parse("select * from \"a;1\".`b;c`;;;select 1 as `a ; a`; ;\t;\r;\n"),
                new ClickHouseSqlStatement[] {
                        new ClickHouseSqlStatement("select * from \"a;1\".`b;c`",
                                StatementType.SELECT, null, "a;1", "b;c", null, null, null, null, null, null, null,
                                null, null),
                        new ClickHouseSqlStatement("select 1 as `a ; a`",
                                StatementType.SELECT) });
    }

    @Test(groups = "unit")
    public void testAlias() throws ParseException {
        String sql;
        checkSingleStatement(parse(sql = "select 1 as c, 2 b"), sql, StatementType.SELECT);
        checkSingleStatement(parse(sql = "select 1 from a.b c"), sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "select 1 select from a.b c"), sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "select 1 from (select 2) b"), sql, StatementType.SELECT, "system",
                "b");
        checkSingleStatement(parse(sql = "select 1 from (select 2) as from"), sql, StatementType.SELECT,
                "system",
                "from");
        checkSingleStatement(parse(sql = "select 1 from a.b c1, b.a c2"), sql, StatementType.SELECT, "a", "b");
    }

    @Test(groups = "unit")
    public void testExpression() throws ParseException {
        String sql;
        checkSingleStatement(parse(sql = "SELECT a._ from a.b"), sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "SELECT 2 BETWEEN 1 + 1 AND 3 - 1 from a.b"), sql,
                StatementType.SELECT, "a",
                "b");
        checkSingleStatement(parse(sql = "SELECT CASE WHEN 1 THEN 2 WHEN 3 THEN  4 ELSE 5 END from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "select (1,2) a1, a1.1, a1 .1, a1 . 1 from a.b"), sql,
                StatementType.SELECT,
                "a", "b");
        checkSingleStatement(parse(sql = "select -.0, +.0, -a from a.b"), sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "select 1 and `a`.\"b\" c1, c1 or (c2 and c3), c4 ? c5 : c6 from a.b"),
                sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "select [[[1,2],[3,4],[5,6]]] a, a[1][1][2] from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(
                parse(sql = "select [[[[]]]], a[1][2][3], ([[1]] || [[2]])[2][1] ,func(1,2) [1] [2] [ 3 ] from a.b"),
                sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "select c.c1, c.c2 c, c.c3 as cc, c.c4.1.2 from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "select - (select (1,).1) from a.b"), sql, StatementType.SELECT, "a",
                "b");
        checkSingleStatement(parse(sql = "select 1.1e1,(1) . 1 , ((1,2)).1 .2 . 3 from a.b"), sql,
                StatementType.SELECT,
                "a", "b");
        checkSingleStatement(parse(sql = "select a.b.c1, c1, b.c1 from a.b"), sql, StatementType.SELECT, "a",
                "b");
        checkSingleStatement(parse(sql = "select date'2020-02-04', timestamp '2020-02-04' from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "select count (), sum(c1), fake(a1, count(), (1+1)) from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "select {}, {'a':'b', 'c':'1'} from a.b"), sql, StatementType.SELECT,
                "a",
                "b");
        checkSingleStatement(parse(sql = "select [], [1,2], [ [1,2], [3,4] ] from a.b"), sql,
                StatementType.SELECT, "a",
                "b");
        checkSingleStatement(parse(sql = "select 1+1-1*1/1 from a.b"), sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(parse(sql = "select (1+(1-1)*1/1)-1 from a.b"), sql, StatementType.SELECT, "a",
                "b");
        checkSingleStatement(parse(sql = "select (1+(1+(-1))*1/1)-(select (1,).1) from a.b"), sql,
                StatementType.SELECT,
                "a", "b");
    }

    @Test(groups = "unit")
    public void testFormat() throws ParseException {
        String sql = "select 1 as format, format csv";
        ClickHouseSqlStatement[] stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasFormat(), false);
        assertEquals(stmts[0].getFormat(), null);

        sql = "select 1 format csv";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasFormat(), true);
        assertEquals(stmts[0].getFormat(), "csv");

        sql = "select 1 a, a.a b, a.a.a c, e.* except(e1), e.e.* except(e2), 'aaa' format, format csv from numbers(2) FORMAT CSVWithNames";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasFormat(), true);
        assertEquals(stmts[0].getFormat(), "CSVWithNames");
    }

    @Test(groups = "unit")
    public void testInFile() throws ParseException {
        String sql = "insert into mytable from infile 'inputs*.csv'";
        ClickHouseSqlStatement[] stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasCompressAlgorithm(), false);
        assertEquals(stmts[0].getCompressAlgorithm(), null);
        assertEquals(stmts[0].hasCompressLevel(), false);
        assertEquals(stmts[0].getCompressLevel(), null);
        assertEquals(stmts[0].hasFormat(), false);
        assertEquals(stmts[0].getFormat(), null);
        assertEquals(stmts[0].hasFile(), true);
        assertEquals(stmts[0].getFile(), "'inputs*.csv'");

        sql = "insert into mytable from infile 'inputs{1,2,3}.bin' format RowBinary";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].getCompressAlgorithm(), null);
        assertEquals(stmts[0].getCompressLevel(), null);
        assertEquals(stmts[0].hasFormat(), true);
        assertEquals(stmts[0].getFormat(), "RowBinary");
        assertEquals(stmts[0].hasFile(), true);
        assertEquals(stmts[0].getFile(), "'inputs{1,2,3}.bin'");

        sql = "insert into mytable from infile 'inputs{1,2,3}.n.gz' compression 'gzip' format Native";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].getCompressAlgorithm(), "'gzip'");
        assertEquals(stmts[0].getCompressLevel(), null);
        assertEquals(stmts[0].hasFormat(), true);
        assertEquals(stmts[0].getFormat(), "Native");
        assertEquals(stmts[0].hasFile(), true);
        assertEquals(stmts[0].getFile(), "'inputs{1,2,3}.n.gz'");

        // actually ClickHouse does not support compression level for infile
        sql = "insert into mytable from infile 'inputs{1,2,3}.n.gz' compression 'gzip' level 3 settings a=1, b='2' format Native";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasCompressAlgorithm(), true);
        assertEquals(stmts[0].getCompressAlgorithm(), "'gzip'");
        assertEquals(stmts[0].hasCompressLevel(), true);
        assertEquals(stmts[0].getCompressLevel(), "3");
        assertEquals(stmts[0].hasFormat(), true);
        assertEquals(stmts[0].getFormat(), "Native");
        assertEquals(stmts[0].hasFile(), true);
        assertEquals(stmts[0].getFile(), "'inputs{1,2,3}.n.gz'");

        sql = "select * from infile";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].getCompressAlgorithm(), null);
        assertEquals(stmts[0].getCompressLevel(), null);
        assertEquals(stmts[0].getFormat(), null);
        assertEquals(stmts[0].getFile(), null);

        sql = "insert into mytable from infile :a compression ? level ? format Native";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].getCompressAlgorithm(), "?");
        assertEquals(stmts[0].getCompressLevel(), "?");
        assertEquals(stmts[0].getFormat(), "Native");
        assertEquals(stmts[0].getFile(), ":a");
    }

    @Test(groups = "unit")
    public void testOutFile() throws ParseException {
        String sql = "select 1 into outfile '1.txt'";
        ClickHouseSqlStatement[] stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasCompressAlgorithm(), false);
        assertEquals(stmts[0].getCompressAlgorithm(), null);
        assertEquals(stmts[0].hasCompressLevel(), false);
        assertEquals(stmts[0].getCompressLevel(), null);
        assertEquals(stmts[0].hasFormat(), false);
        assertEquals(stmts[0].getFormat(), null);
        assertEquals(stmts[0].hasFile(), true);
        assertEquals(stmts[0].getFile(), "'1.txt'");

        sql = "select * from numbers(10) settings max_result_rows=1 into outfile 'a.csv.gz' compression 'gzip' level 5 format CSV";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasCompressAlgorithm(), true);
        assertEquals(stmts[0].getCompressAlgorithm(), "'gzip'");
        assertEquals(stmts[0].hasCompressLevel(), true);
        assertEquals(stmts[0].getCompressLevel(), "5");
        assertEquals(stmts[0].hasFormat(), true);
        assertEquals(stmts[0].getFormat(), "CSV");
        assertEquals(stmts[0].hasFile(), true);
        assertEquals(stmts[0].getFile(), "'a.csv.gz'");

        sql = "insert into outfile values(1,2,3)";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].getCompressAlgorithm(), null);
        assertEquals(stmts[0].getCompressLevel(), null);
        assertEquals(stmts[0].getFormat(), null);
        assertEquals(stmts[0].getFile(), null);

        sql = "select * from numbers(10) settings max_result_rows=1 into outfile ? compression :a level :b format CSV";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasCompressAlgorithm(), true);
        assertEquals(stmts[0].getCompressAlgorithm(), ":a");
        assertEquals(stmts[0].hasCompressLevel(), true);
        assertEquals(stmts[0].getCompressLevel(), ":b");
        assertEquals(stmts[0].hasFormat(), true);
        assertEquals(stmts[0].getFormat(), "CSV");
        assertEquals(stmts[0].hasFile(), true);
        assertEquals(stmts[0].getFile(), "?");
    }

    @Test(groups = "unit")
    public void testWithTotals() throws ParseException {
        String sql = "select 1 as with totals";
        ClickHouseSqlStatement[] stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasWithTotals(), false);

        sql = "select 1 with totals";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasWithTotals(), true);
    }

    @Test(groups = "unit")
    public void testParameterHandling() throws ParseException {
        String sql = "insert into table d.t(a1, a2, a3) values(?,?,?)";
        ClickHouseSqlStatement[] stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);

        stmts = ClickHouseSqlParser.parse(sql, new ClickHouseConfig(), new ParseHandler() {
            @Override
            public String handleParameter(String cluster, String database, String table, int columnIndex) {
                return String.valueOf(columnIndex);
            }
        });
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "insert into table d.t(a1, a2, a3) values(1,2,3)");
    }

    @Test(groups = "unit")
    public void testMacroHandling() throws ParseException {
        String sql = "select #listOfColumns #ignored from (#subQuery('1','2','3'))";
        ClickHouseSqlStatement[] stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "select   from ()");

        stmts = ClickHouseSqlParser.parse(sql, new ClickHouseConfig(), new ParseHandler() {
            @Override
            public String handleMacro(String name, List<String> parameters) {
                if ("listOfColumns".equals(name)) {
                    return "a, b";
                } else if ("subQuery".equals(name)) {
                    return "select " + String.join("+", parameters);
                } else {
                    return null;
                }
            }
        });
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "select a, b  from (select 1+2+3)");
    }

    @Test(groups = "unit")
    public void testExtractDBAndTableName() {
        String sql;

        checkSingleStatement(parse(sql = "SELECT 1 from table"), sql, StatementType.SELECT, "system", "table");
        checkSingleStatement(parse(sql = "SELECT 1 from table a"), sql, StatementType.SELECT, "system",
                "table");
        checkSingleStatement(parse(sql = "SELECT 1 from\ntable a"), sql, StatementType.SELECT, "system",
                "table");
        checkSingleStatement(parse(sql = "SELECT 1\nfrom\ntable a"), sql, StatementType.SELECT, "system",
                "table");
        checkSingleStatement(parse(sql = "SELECT 1\nFrom\ntable a"), sql, StatementType.SELECT, "system",
                "table");
        checkSingleStatement(parse(sql = "SELECT 1 from db.table a"), sql, StatementType.SELECT, "db", "table");
        checkSingleStatement(parse(sql = " SELECT 1 from \"db.table\" a"), sql, StatementType.SELECT, "system",
                "db.table");
        checkSingleStatement(parse(sql = "SELECT 1 from `db.table` a"), sql, StatementType.SELECT, "system",
                "db.table");
        checkSingleStatement(parse(sql = "from `db.table` a"), sql, StatementType.UNKNOWN, "system", "unknown");
        checkSingleStatement(parse(sql = " from `db.table` a"), sql, StatementType.UNKNOWN, "system",
                "unknown");
        checkSingleStatement(parse(sql = "ELECT from `db.table` a"), sql, StatementType.UNKNOWN, "system",
                "unknown");
        checkSingleStatement(parse(sql = "SHOW tables"), sql, StatementType.SHOW, "system", "tables");
        checkSingleStatement(parse(sql = "desc table1"), sql, StatementType.DESCRIBE, "system", "columns");
        checkSingleStatement(parse(sql = "DESC table1"), sql, StatementType.DESCRIBE, "system", "columns");
        checkSingleStatement(parse(sql = "SELECT 'from db.table a' from tab"), sql, StatementType.SELECT,
                "system",
                "tab");
        checkSingleStatement(parse(sql = "SELECT"), sql, StatementType.UNKNOWN, "system", "unknown");
        checkSingleStatement(parse(sql = "S"), sql, StatementType.UNKNOWN, "system", "unknown");
        checkSingleStatement(parse(sql = ""), sql, StatementType.UNKNOWN, "system", "unknown");
        checkSingleStatement(parse(sql = " SELECT 1 from table from"), sql, StatementType.SELECT, "system",
                "table");
        checkSingleStatement(parse(sql = " SELECT 1 from table from"), sql, StatementType.SELECT, "system",
                "table");
        checkSingleStatement(parse(sql = "SELECT fromUnixTimestamp64Milli(time) as x from table"), sql,
                StatementType.SELECT, "system", "table");
        checkSingleStatement(parse(sql = " SELECT fromUnixTimestamp64Milli(time)from table"), sql,
                StatementType.SELECT,
                "system", "table");
        checkSingleStatement(parse(sql = "/*qq*/ SELECT fromUnixTimestamp64Milli(time)from table"), sql,
                StatementType.SELECT, "system", "table");
        checkSingleStatement(parse(sql = " SELECTfromUnixTimestamp64Milli(time)from table"), sql,
                StatementType.UNKNOWN,
                "system", "unknown");
        checkSingleStatement(parse(sql = " SELECT fromUnixTimestamp64Milli(time)from \".inner.a\""), sql,
                StatementType.SELECT, "system", ".inner.a");
        checkSingleStatement(parse(sql = " SELECT fromUnixTimestamp64Milli(time)from db.`.inner.a`"), sql,
                StatementType.SELECT, "db", ".inner.a");
    }

    @Test(groups = "unit")
    public void testJdbcEscapeSyntax() {
        String sql = "select {d '123'}";
        ClickHouseSqlStatement[] stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "select date'123'");
        assertEquals(stmts[0].hasTempTable(), false);

        sql = "select {t '123'}";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "select timestamp'1970-01-01 123'");
        assertEquals(stmts[0].hasTempTable(), false);

        sql = "select {ts '123'}";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "select timestamp'123'");
        assertEquals(stmts[0].hasTempTable(), false);

        sql = "select {ts '123.1'}";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "select toDateTime64('123.1',1)");
        assertEquals(stmts[0].hasTempTable(), false);

        sql = "select {tt '1''2\\'3'}";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "select `1'2'3`");
        assertEquals(stmts[0].hasTempTable(), true);
        assertEquals(stmts[0].getTempTables(), Collections.singleton("1'2'3"));

        sql = "select {d 1} {t} {tt} {ts 123.1'}";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "select    ");
    }

    @Test(groups = "unit")
    public void testNewParameterSyntax() {
        String sql = "select {column_a:String}";
        ClickHouseSqlStatement[] stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].isQuery(), true);
        assertEquals(stmts[0].getSQL(), sql);

        sql = "select :column_a(String)";
        stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].isQuery(), true);
        assertEquals(stmts[0].getSQL(), sql);
    }

    @Test(groups = "unit")
    public void testTcl() {
        ClickHouseSqlStatement[] stmts = parse("begin transaction; commit;rollback;");
        assertEquals(stmts.length, 3);
        assertEquals(stmts[0].isTCL(), true);
        assertEquals(stmts[0].containsKeyword("bEGin"), true);
        assertEquals(stmts[0].getSQL(), "begin transaction");
        assertEquals(stmts[1].isTCL(), true);
        assertEquals(stmts[1].containsKeyword("Commit"), true);
        assertEquals(stmts[1].getSQL(), " commit");
        assertEquals(stmts[2].isTCL(), true);
        assertEquals(stmts[2].containsKeyword("RollBack"), true);
        assertEquals(stmts[2].getSQL(), "rollback");
    }

    @Test(enabled = false)
    public void testSETRoleStatements() {
        final String simpleStmt = "SET ROLE  ROL1, ROL2";
        ClickHouseSqlStatement[] stmts = parse(simpleStmt);
        Assert.assertEquals(stmts.length, 1);
        Assert.assertEquals(stmts[0].getStatementType(), StatementType.SET);
        Assert.assertEquals(stmts[0].getOperationType(), OperationType.UNKNOWN);
        Assert.assertEquals(stmts[0].getSQL(), simpleStmt);
        Assert.assertNotNull(stmts[0].getSettings().get("_ROLES"));

        final String compositeStmt = "SET ROLE ROL1; SET ROLE ROL2;";
        stmts = parse(compositeStmt);
        Assert.assertEquals(stmts.length, 2);
        for (ClickHouseSqlStatement stmt : stmts) {
            Assert.assertEquals(stmt.getStatementType(), StatementType.SET);
            Assert.assertNotNull(stmts[0].getSettings().get("_ROLES"));
        }
    }

    // known issue
    public void testTernaryOperator() {
        String sql = "select x > 2 ? 'a' : 'b' from (select number as x from system.numbers limit ?)";
        ClickHouseSqlStatement[] stmts = parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getStatementType(), StatementType.SELECT);
        assertEquals(stmts[0].getParameters().size(), 1);
    }

    static void parseAllSqlFiles(File f) throws IOException {
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            for (File file : files) {
                parseAllSqlFiles(file);
            }
        } else if (f.getName().endsWith(".sql")) {
            StringBuilder sql = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    sql.append(line).append("\n");
                }
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }

            ClickHouseSqlParser p = new ClickHouseSqlParser(sql.toString(), null, null);
            try {
                p.sql();
            } catch (ParseException e) {
                System.out.println(f.getAbsolutePath() + " -> " + e.getMessage());
            } catch (TokenMgrException e) {
                System.out.println(f.getAbsolutePath() + " -> " + e.getMessage());
            }
        }
    }

    // TODO: add a sub-module points to ClickHouse/tests/queries?
    public static void main(String[] args) throws IOException {
        String chTestQueryDir = "D:/Sources/Github/ch/queries";
        if (args != null && args.length > 0) {
            chTestQueryDir = args[0];
        }
        chTestQueryDir = System.getProperty("chTestQueryDir", chTestQueryDir);
        parseAllSqlFiles(new File(chTestQueryDir));
    }
}
