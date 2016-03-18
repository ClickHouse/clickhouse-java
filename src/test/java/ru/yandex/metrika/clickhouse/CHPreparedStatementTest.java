package ru.yandex.metrika.clickhouse;

import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * Created by zhur on 14/03/16.
 */
public class CHPreparedStatementTest {

    @Test
    public void testParseSql() throws Exception {
        assertEquals(new ArrayList<String>(){{
            add("SELECT * FROM tbl");
        }}, CHPreparedStatement.parseSql("SELECT * FROM tbl"));

        assertEquals(new ArrayList<String>(){{
            add("SELECT * FROM tbl WHERE t = ");
            add("");
        }}, CHPreparedStatement.parseSql("SELECT * FROM tbl WHERE t = ?"));

        assertEquals(new ArrayList<String>(){{
            add("SELECT 'a\\'\\\\sdfasdf?adsf\\\\' as `sadf\\`?` FROM tbl WHERE t = ");
            add(" AND r = ");
            add(" ORDER BY 1");
        }}, CHPreparedStatement.parseSql("SELECT 'a\\'\\\\sdfasdf?adsf\\\\' as `sadf\\`?` FROM tbl WHERE t = ? AND r = ? ORDER BY 1"));

    }
}