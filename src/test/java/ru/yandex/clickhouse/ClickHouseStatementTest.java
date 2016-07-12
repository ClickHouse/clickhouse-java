package ru.yandex.clickhouse;


import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ClickHouseStatementTest {
    @Test
    public void testClickhousify() throws Exception {
        String sql = "SELECT ololo FROM ololoed;";
        assertEquals("SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;", ClickHouseStatementImpl.clickhousifySql(sql));

        String sql2 = "SELECT ololo FROM ololoed";
        assertEquals("SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;", ClickHouseStatementImpl.clickhousifySql(sql2));

        String sql3 = "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes";
        assertEquals("SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes", ClickHouseStatementImpl.clickhousifySql(sql3));

        String sql4 = "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;";
        assertEquals("SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;", ClickHouseStatementImpl.clickhousifySql(sql4));

        String sql5 = "SHOW ololo FROM ololoed;";
        assertEquals("SHOW ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;", ClickHouseStatementImpl.clickhousifySql(sql5));
    }
}
