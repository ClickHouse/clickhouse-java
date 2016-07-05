package ru.yandex.clickhouse;


import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.testng.Assert.assertEquals;

/**
 * Created by zhur on 14/03/16.
 */
public class ClickHousePreparedStatementTest {

    @Test
    public void testParseSql() throws Exception {
        assertEquals(new ArrayList<String>(){{
            add("SELECT * FROM tbl");
        }}, ClickHousePreparedStatementImpl.parseSql("SELECT * FROM tbl"));

        assertEquals(new ArrayList<String>(){{
            add("SELECT * FROM tbl WHERE t = ");
            add("");
        }}, ClickHousePreparedStatementImpl.parseSql("SELECT * FROM tbl WHERE t = ?"));

        assertEquals(new ArrayList<String>(){{
            add("SELECT 'a\\'\\\\sdfasdf?adsf\\\\' as `sadf\\`?` FROM tbl WHERE t = ");
            add(" AND r = ");
            add(" ORDER BY 1");
        }}, ClickHousePreparedStatementImpl.parseSql("SELECT 'a\\'\\\\sdfasdf?adsf\\\\' as `sadf\\`?` FROM tbl WHERE t = ? AND r = ? ORDER BY 1"));

    }
}