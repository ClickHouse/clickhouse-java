package ru.yandex.clickhouse.jdbc.parser;

import org.testng.annotations.Test;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

import static org.testng.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHouseSqlParserTest {
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

    private void checkSingleStatement(ClickHouseSqlStatement[] stmts, String sql) {
        checkSingleStatement(stmts, sql, StatementType.UNKNOWN, ClickHouseSqlStatement.DEFAULT_DATABASE,
                ClickHouseSqlStatement.DEFAULT_TABLE);
    }

    private void checkSingleStatement(ClickHouseSqlStatement[] stmts, String sql, StatementType stmtType) {
        checkSingleStatement(stmts, sql, stmtType, ClickHouseSqlStatement.DEFAULT_DATABASE,
                ClickHouseSqlStatement.DEFAULT_TABLE);
    }

    private void checkSingleStatement(ClickHouseSqlStatement[] stmts, String sql, StatementType stmtType,
            String database, String table) {
        assertEquals(stmts.length, 1);

        ClickHouseSqlStatement s = stmts[0];
        assertEquals(s.getSQL(), sql);
        assertEquals(s.getStatementType(), stmtType);
        assertEquals(s.getDatabaseOrDefault(null), database);
        assertEquals(s.getTable(), table);
    }

    @Test
    public void testParseNonSql() throws ParseException {
        String sql;

        assertEquals(ClickHouseSqlParser.parse(sql = null), new ClickHouseSqlStatement[] {
                new ClickHouseSqlStatement(sql, StatementType.UNKNOWN, null, null, null, null, null, null) });
        assertEquals(ClickHouseSqlParser.parse(sql = ""), new ClickHouseSqlStatement[] {
                new ClickHouseSqlStatement(sql, StatementType.UNKNOWN, null, null, null, null, null, null) });

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "invalid sql"), sql);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "-- some comments"), sql);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "/*********\r\n\r\t some ***** comments*/"), sql);

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select"), sql, StatementType.UNKNOWN);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select ()"), sql, StatementType.UNKNOWN);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select (()"), sql, StatementType.UNKNOWN);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select [[]"), sql, StatementType.UNKNOWN);
        // checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1 select"), sql,
        // StatementType.UNKNOWN);
    }

    @Test
    public void testAlterStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser
                .parse(sql = "ALTER TABLE test_db.test_table UPDATE a = 1, \"b\" = '2', `c`=3.3 WHERE d=123 and e=456"),
                sql, StatementType.ALTER_UPDATE, "test_db", "test_table");
        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "ALTER TABLE tTt on cluster 'cc' delete WHERE d=123 and e=456"), sql,
                StatementType.ALTER_DELETE, "system", "tTt");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "ALTER USER user DEFAULT ROLE role1, role2"), sql,
                StatementType.ALTER);
    }

    @Test
    public void testAttachStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "ATTACH TABLE IF NOT EXISTS t.t ON CLUSTER cluster"), sql,
                StatementType.ATTACH);
    }

    @Test
    public void testCheckStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "check table a"), sql, StatementType.CHECK);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "check table a.a"), sql, StatementType.CHECK);
    }

    @Test
    public void testCreateStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "create table a(a String) engine=Memory"), sql,
                StatementType.CREATE);
    }

    @Test
    public void testDeleteStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "delete from a"), sql, StatementType.DELETE, "system",
                "a");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "delete from c.a where upper(a)=upper(lower(b))"), sql,
                StatementType.DELETE, "c", "a");
    }

    @Test
    public void testDescribeStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "desc a"), sql, StatementType.DESCRIBE, "system",
                "columns");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "desc table a"), sql, StatementType.DESCRIBE, "system",
                "columns");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "describe table a.a"), sql, StatementType.DESCRIBE, "a",
                "columns");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "desc table table"), sql, StatementType.DESCRIBE, "system",
                "columns");
    }

    @Test
    public void testDetachStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "detach TABLE t"), sql, StatementType.DETACH);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "detach TABLE if exists t.t on cluster 'cc'"), sql,
                StatementType.DETACH);
    }

    @Test
    public void testDropStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "drop TEMPORARY table t"), sql, StatementType.DROP);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "drop TABLE if exists t.t on cluster 'cc'"), sql,
                StatementType.DROP);
    }

    @Test
    public void testExistsStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "EXISTS TEMPORARY TABLE a"), sql, StatementType.EXISTS);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "EXISTS TABLE a.a"), sql, StatementType.EXISTS);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "EXISTS DICTIONARY c"), sql, StatementType.EXISTS);
    }

    @Test
    public void testExplainStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(
                sql = "EXPLAIN SELECT sum(number) FROM numbers(10) UNION ALL SELECT sum(number) FROM numbers(10) ORDER BY sum(number) ASC FORMAT TSV"),
                sql, StatementType.EXPLAIN);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "EXPLAIN AST SELECT 1"), sql, StatementType.EXPLAIN);
        checkSingleStatement(ClickHouseSqlParser.parse(
                sql = "EXPLAIN SYNTAX SELECT * FROM system.numbers AS a, system.numbers AS b, system.numbers AS c"),
                sql, StatementType.EXPLAIN);
    }

    @Test
    public void testGrantStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION"),
                sql, StatementType.GRANT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "GRANT INSERT(x,y) ON db.table TO john"), sql,
                StatementType.GRANT);
    }

    @Test
    public void testInsertStatement() throws ParseException {
        String sql;

        ClickHouseSqlStatement s = ClickHouseSqlParser.parse(sql = "insert into table test(a,b) Values (1,2)")[0];
        assertEquals(sql.substring(s.getStartPosition("values"), s.getEndPosition("VALUES")), "Values");
        assertEquals(sql.substring(0, s.getEndPosition("values")) + " (1,2)", sql);

        Pattern values = Pattern.compile("(?i)VALUES[\\s]*\\(");
        int valuePosition = -1;
        Matcher matcher = values.matcher(sql);
        if (matcher.find()) {
            valuePosition = matcher.start();
        }
        assertEquals(s.getStartPosition("values"), valuePosition);

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "insert into function null('a UInt8') values(1)"), sql,
                StatementType.INSERT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "insert into function null('a UInt8') values(1)(2)"), sql,
                StatementType.INSERT);
        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "insert into function null('a UInt8') select * from number(10)"), sql,
                StatementType.INSERT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "insert into test2(a,b) values('values(',',')"), sql,
                StatementType.INSERT, "system", "test2");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "INSERT INTO table t(a, b, c) values('1', ',', 'ccc')"),
                sql, StatementType.INSERT, "system", "t");
        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "INSERT INTO table t(a, b, c) values('1', 2, 'ccc') (3,2,1)"), sql,
                StatementType.INSERT, "system", "t");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "INSERT INTO table s.t select * from ttt"), sql,
                StatementType.INSERT, "s", "t");
        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2)"), sql,
                StatementType.INSERT, "system", "insert_select_testtable");

    }

    @Test
    public void testKillStatement() {
        String sql;

        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'"), sql,
                StatementType.KILL);
        checkSingleStatement(ClickHouseSqlParser.parse(
                sql = "KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt' SYNC"),
                sql, StatementType.KILL);
    }

    @Test
    public void testOptimizeStatement() {
        String sql;

        checkSingleStatement(
                ClickHouseSqlParser
                        .parse(sql = "OPTIMIZE TABLE a ON CLUSTER cluster PARTITION ID 'partition_id' FINAL"),
                sql, StatementType.OPTIMIZE);
    }

    @Test
    public void testRenameStatement() {
        String sql;

        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "RENAME TABLE table1 TO table2, table3 TO table4 ON CLUSTER cluster"),
                sql, StatementType.RENAME);
        checkSingleStatement(ClickHouseSqlParser.parse(
                sql = "RENAME TABLE db1.table1 TO db2.table2, db2.table3 to db2.table4, db3.table5 to db2.table6 ON CLUSTER 'c'"),
                sql, StatementType.RENAME);
    }

    @Test
    public void testRevokeStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "REVOKE SELECT ON accounts.* FROM john"), sql,
                StatementType.REVOKE);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "REVOKE SELECT(wage) ON accounts.staff FROM mira"), sql,
                StatementType.REVOKE);
    }

    @Test
    public void testSelectStatement() {
        String sql;

        assertEquals(ClickHouseSqlParser.parse(sql = "select\n1"), new ClickHouseSqlStatement[] {
                new ClickHouseSqlStatement(sql, StatementType.SELECT, null, null, "unknown", null, null, null) });
        assertEquals(ClickHouseSqlParser.parse(sql = "select\r\n1"), new ClickHouseSqlStatement[] {
                new ClickHouseSqlStatement(sql, StatementType.SELECT, null, null, "unknown", null, null, null) });

        assertEquals(ClickHouseSqlParser.parse(sql = "select 314 limit 5\nFORMAT JSONCompact;"),
                new ClickHouseSqlStatement[] { new ClickHouseSqlStatement("select 314 limit 5\nFORMAT JSONCompact",
                        StatementType.SELECT, null, null, "unknown", "JSONCompact", null, null) });

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select (())"), sql, StatementType.SELECT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select []"), sql, StatementType.SELECT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select [[]]"), sql, StatementType.SELECT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select *"), sql, StatementType.SELECT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select timezone()"), sql, StatementType.SELECT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select @@version, $version"), sql, StatementType.SELECT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select * from jdbc('db', 'schema', 'select 1')"), sql,
                StatementType.SELECT, "system", "jdbc");
        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "select 1 as a1, a.a as a2, aa(a1, a2) a3, length(a3) as a4 from x"),
                sql, StatementType.SELECT, "system", "x");
        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "select x.* from (select [1,2] a, (1,2,3) b, a[1], b.2) x"), sql,
                StatementType.SELECT, "system", "x");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select (3, [[1,2],[3,4]]) as a, (a.2)[2][1]"), sql,
                StatementType.SELECT);
        checkSingleStatement(
                ClickHouseSqlParser
                        .parse(sql = "select 1,1.1,'\"''`a' a, \"'`\"\"a\" as b, (1 + `a`.a) c, null, inf i, nan as n"),
                sql, StatementType.SELECT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1 as select"), sql, StatementType.SELECT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1, 2 a, 3 as b, 1+1-2*3/4, *, c.* from c a"), sql,
                StatementType.SELECT, "system", "c");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1 as select"), sql, StatementType.SELECT);
        checkSingleStatement(ClickHouseSqlParser.parse(
                sql = "   -- cc\nselect 1 as `a.b`, a, 1+1, b from \"a\".`b` inner join a on a.abb/* \n\r\n1*/\n=2 and a.abb = c.a and a=1 and (k is null and j not in(1,2))"),
                sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT idx, s FROM test.mymetadata WHERE idx = ?"), sql,
                StatementType.SELECT, "test", "mymetadata");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "WITH 2 AS two SELECT two * two"), sql,
                StatementType.SELECT);
        checkSingleStatement(ClickHouseSqlParser.parse(
                sql = "SELECT i, array(toUnixTimestamp(dt_server[1])), array(toUnixTimestamp(dt_berlin[1])), array(toUnixTimestamp(dt_lax[1])) FROM test.fun_with_timezones_array"),
                sql, StatementType.SELECT, "test", "fun_with_timezones_array");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT SUM(x) FROM t WHERE y = ? GROUP BY ?"), sql,
                StatementType.SELECT, "system", "t");

        assertEquals(ClickHouseSqlParser.parse(sql = loadSql("issue-441_with-totals.sql")),
                new ClickHouseSqlStatement[] { new ClickHouseSqlStatement(sql, StatementType.SELECT, null, null,
                        "unknown", null, null, new HashMap<String, Integer>() {
                            {
                                put("TOTALS", 208);
                            }
                        }) });
        assertEquals(ClickHouseSqlParser.parse(sql = loadSql("issue-555_custom-format.sql")),
                new ClickHouseSqlStatement[] { new ClickHouseSqlStatement(sql, StatementType.SELECT, null, null, "wrd",
                        "CSVWithNames", null, null) });
    }

    @Test
    public void testSetStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SET profile = 'my-profile', mutations_sync=1"), sql,
                StatementType.SET);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SET DEFAULT ROLE role1, role2, role3 TO user"), sql,
                StatementType.SET);
    }

    @Test
    public void testShowStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SHOW DATABASES LIKE '%de%'"), sql, StatementType.SHOW,
                "system", "databases");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "show tables from db"), sql, StatementType.SHOW, "system",
                "tables");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "show dictionaries from db"), sql, StatementType.SHOW,
                "system", "dictionaries");
    }

    @Test
    public void testSystemStatement() {
        String sql;

        checkSingleStatement(
                ClickHouseSqlParser
                        .parse(sql = "SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk'"),
                sql, StatementType.SYSTEM);
        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "SYSTEM RESTART REPLICA db.replicated_merge_tree_family_table_name"),
                sql, StatementType.SYSTEM);
    }

    @Test
    public void testTruncateStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "truncate table a.b"), sql, StatementType.TRUNCATE, "a",
                "b");
    }

    @Test
    public void testUpdateStatement() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "update a set a='1'"), sql, StatementType.UPDATE);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "update a.a set `a`=2 where upper(a)=upper(lower(b))"),
                sql, StatementType.UPDATE);
    }

    @Test
    public void testUseStatement() throws ParseException {
        String sql;
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "use system"), sql, StatementType.USE);
    }

    @Test
    public void testWatchStatement() throws ParseException {
        String sql;
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "watch system.processes"), sql, StatementType.WATCH);
    }

    @Test
    public void testMultipleStatements() throws ParseException {
        assertEquals(ClickHouseSqlParser.parse("use ab;;;select 1; ;\t;\r;\n"),
                new ClickHouseSqlStatement[] {
                        new ClickHouseSqlStatement("use ab", StatementType.USE, null, "ab", null, null, null, null),
                        new ClickHouseSqlStatement("select 1", StatementType.SELECT) });
        assertEquals(ClickHouseSqlParser.parse("select * from \"a;1\".`b;c`;;;select 1 as `a ; a`; ;\t;\r;\n"),
                new ClickHouseSqlStatement[] {
                        new ClickHouseSqlStatement("select * from \"a;1\".`b;c`", StatementType.SELECT, null, "a;1",
                                "b;c", null, null, null),
                        new ClickHouseSqlStatement("select 1 as `a ; a`", StatementType.SELECT) });
    }

    @Test
    public void testAlias() throws ParseException {
        String sql;
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1 as c, 2 b"), sql, StatementType.SELECT);
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1 from a.b c"), sql, StatementType.SELECT, "a",
                "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1 select from a.b c"), sql, StatementType.SELECT,
                "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1 from (select 2) b"), sql, StatementType.SELECT,
                "system", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1 from (select 2) as from"), sql,
                StatementType.SELECT, "system", "from");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1 from a.b c1, b.a c2"), sql, StatementType.SELECT,
                "a", "b");
    }

    @Test
    public void testExpression() throws ParseException {
        String sql;

        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "select 1 and `a`.\"b\" c1, c1 or (c2 and c3), c4 ? c5 : c6 from a.b"),
                sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select [[[1,2],[3,4],[5,6]]] a, a[1][1][2] from a.b"),
                sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(
                ClickHouseSqlParser.parse(
                        sql = "select [[[[]]]], a[1][2][3], ([[1]] || [[2]])[2][1] ,func(1,2) [1] [2] [ 3 ] from a.b"),
                sql, StatementType.SELECT, "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select c.c1, c.c2 c, c.c3 as cc, c.c4.1.2 from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select - (select (1,).1) from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1.1e1,(1) . 1 , ((1,2)).1 .2 . 3 from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select a.b.c1, c1, b.c1 from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "select date'2020-02-04', timestamp '2020-02-04' from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = "select count (), sum(c1), fake(a1, count(), (1+1)) from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select {}, {'a':'b', 'c':'1'} from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select [], [1,2], [ [1,2], [3,4] ] from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select 1+1-1*1/1 from a.b"), sql, StatementType.SELECT,
                "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select (1+(1-1)*1/1)-1 from a.b"), sql,
                StatementType.SELECT, "a", "b");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "select (1+(1+(-1))*1/1)-(select (1,).1) from a.b"), sql,
                StatementType.SELECT, "a", "b");
    }

    @Test
    public void testFormat() throws ParseException {
        String sql = "select 1 as format, format csv";
        ClickHouseSqlStatement[] stmts = ClickHouseSqlParser.parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasFormat(), false);
        assertEquals(stmts[0].getFormat(), null);

        sql = "select 1 format csv";
        stmts = ClickHouseSqlParser.parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasFormat(), true);
        assertEquals(stmts[0].getFormat(), "csv");

        sql = "select 1 a, a.a b, a.a.a c, e.* except(e1), e.e.* except(e2), 'aaa' format, format csv from numbers(2) FORMAT CSVWithNames";
        stmts = ClickHouseSqlParser.parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasFormat(), true);
        assertEquals(stmts[0].getFormat(), "CSVWithNames");
    }

    @Test
    public void testOutfile() throws ParseException {
        String sql = "select 1 into outfile '1.txt'";
        ClickHouseSqlStatement[] stmts = ClickHouseSqlParser.parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasOutfile(), true);
        assertEquals(stmts[0].getOutfile(), "'1.txt'");

        sql = "insert into outfile values(1,2,3)";
        stmts = ClickHouseSqlParser.parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasOutfile(), false);
        assertEquals(stmts[0].getOutfile(), null);
    }

    @Test
    public void testWithTotals() throws ParseException {
        String sql = "select 1 as with totals";
        ClickHouseSqlStatement[] stmts = ClickHouseSqlParser.parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasWithTotals(), false);

        sql = "select 1 with totals";
        stmts = ClickHouseSqlParser.parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);
        assertEquals(stmts[0].hasWithTotals(), true);
    }

    @Test
    public void testParameterHandling() throws ParseException {
        String sql = "insert into table d.t(a1, a2, a3) values(?,?,?)";
        ClickHouseSqlStatement[] stmts = ClickHouseSqlParser.parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), sql);

        stmts = ClickHouseSqlParser.parse(sql, new ClickHouseProperties(), new ParseHandler() {
            @Override
            public String handleMacro(String name, List<String> parameters) {
                return null;
            }

            @Override
            public String handleParameter(String cluster, String database, String table, int columnIndex) {
                return String.valueOf(columnIndex);
            }
        });
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "insert into table d.t(a1, a2, a3) values(1,2,3)");
    }

    @Test
    public void testMacroHandling() throws ParseException {
        String sql = "select #listOfColumns #ignored from (#subQuery('1','2','3'))";
        ClickHouseSqlStatement[] stmts = ClickHouseSqlParser.parse(sql);
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "select   from ()");

        stmts = ClickHouseSqlParser.parse(sql, new ClickHouseProperties(), new ParseHandler() {
            @Override
            public String handleMacro(String name, List<String> parameters) {
                if ("listOfColumns".equals(name)) {
                    return "a, b";
                } else if ("subQuery".equals(name)) {
                    return "select " + String.join("||", parameters);
                } else {
                    return null;
                }
            }

            @Override
            public String handleParameter(String cluster, String database, String table, int columnIndex) {
                return null;
            }
        });
        assertEquals(stmts.length, 1);
        assertEquals(stmts[0].getSQL(), "select a, b  from (select 1||2||3)");
    }

    @Test
    public void testExtractDBAndTableName() {
        String sql;

        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT 1 from table"), sql, StatementType.SELECT,
                "system", "table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT 1 from table a"), sql, StatementType.SELECT,
                "system", "table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT 1 from\ntable a"), sql, StatementType.SELECT,
                "system", "table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT 1\nfrom\ntable a"), sql, StatementType.SELECT,
                "system", "table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT 1\nFrom\ntable a"), sql, StatementType.SELECT,
                "system", "table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT 1 from db.table a"), sql, StatementType.SELECT,
                "db", "table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = " SELECT 1 from \"db.table\" a"), sql,
                StatementType.SELECT, "system", "db.table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT 1 from `db.table` a"), sql, StatementType.SELECT,
                "system", "db.table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "from `db.table` a"), sql, StatementType.UNKNOWN, "system",
                "unknown");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = " from `db.table` a"), sql, StatementType.UNKNOWN,
                "system", "unknown");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "ELECT from `db.table` a"), sql, StatementType.UNKNOWN,
                "system", "unknown");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SHOW tables"), sql, StatementType.SHOW, "system",
                "tables");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "desc table1"), sql, StatementType.DESCRIBE, "system",
                "columns");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "DESC table1"), sql, StatementType.DESCRIBE, "system",
                "columns");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT 'from db.table a' from tab"), sql,
                StatementType.SELECT, "system", "tab");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT"), sql, StatementType.UNKNOWN, "system",
                "unknown");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "S"), sql, StatementType.UNKNOWN, "system", "unknown");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = ""), sql, StatementType.UNKNOWN, "system", "unknown");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = " SELECT 1 from table from"), sql, StatementType.SELECT,
                "system", "table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = " SELECT 1 from table from"), sql, StatementType.SELECT,
                "system", "table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "SELECT fromUnixTimestamp64Milli(time) as x from table"),
                sql, StatementType.SELECT, "system", "table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = " SELECT fromUnixTimestamp64Milli(time)from table"), sql,
                StatementType.SELECT, "system", "table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = "/*qq*/ SELECT fromUnixTimestamp64Milli(time)from table"),
                sql, StatementType.SELECT, "system", "table");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = " SELECTfromUnixTimestamp64Milli(time)from table"), sql,
                StatementType.UNKNOWN, "system", "unknown");
        checkSingleStatement(ClickHouseSqlParser.parse(sql = " SELECT fromUnixTimestamp64Milli(time)from \".inner.a\""),
                sql, StatementType.SELECT, "system", ".inner.a");
        checkSingleStatement(
                ClickHouseSqlParser.parse(sql = " SELECT fromUnixTimestamp64Milli(time)from db.`.inner.a`"), sql,
                StatementType.SELECT, "db", ".inner.a");
    }
}
