package ru.yandex.clickhouse;


import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.http.impl.client.HttpClientBuilder;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class ClickHouseStatementTest {
    @Test
    public void testClickhousify() throws Exception {
        String sql = "SELECT ololo FROM ololoed;";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql), "SELECT ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql2 = "SELECT ololo FROM ololoed";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql2), "SELECT ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql3 = "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql3), "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes");

        String sql4 = "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql4), "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;");

        String sql5 = "SHOW ololo FROM ololoed;";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql5), "SHOW ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql6 = " show ololo FROM ololoed;";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql6), "show ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql7 = "SELECT ololo FROM ololoed \nFORMAT TabSeparatedWithNamesAndTypes";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql7), "SELECT ololo FROM ololoed \nFORMAT TabSeparatedWithNamesAndTypes");

        String sql8 = "SELECT ololo FROM ololoed \n\n FORMAT TabSeparatedWithNamesAndTypes";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql8), "SELECT ololo FROM ololoed \n\n FORMAT TabSeparatedWithNamesAndTypes");

        String sql9 = "SELECT ololo FROM ololoed\n-- some comments one line";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql9), "SELECT ololo FROM ololoed\n-- some comments one line\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql10 = "SELECT ololo FROM ololoed\n-- some comments\ntwo line";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql10), "SELECT ololo FROM ololoed\n-- some comments\ntwo line\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql11 = "SELECT ololo FROM ololoed/*\nsome comments\ntwo line*/";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql11), "SELECT ololo FROM ololoed/*\nsome comments\ntwo line*/\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql12 = "SELECT ololo FROM ololoed\n// c style some comments one line";
        assertEquals(ClickHouseStatementImpl.clickhousifySql(sql12), "SELECT ololo FROM ololoed\n// c style some comments one line\nFORMAT TabSeparatedWithNamesAndTypes;");

    }

    @Test
    public void testCredentials() throws SQLException, URISyntaxException {
        ClickHouseProperties properties = new ClickHouseProperties(new Properties());
        ClickHouseProperties withCredentials = properties.withCredentials("test_user", "test_password");
        assertTrue(withCredentials != properties);
        assertNull(properties.getUser());
        assertNull(properties.getPassword());
        assertEquals(withCredentials.getUser(), "test_user");
        assertEquals(withCredentials.getPassword(), "test_password");

        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(
                HttpClientBuilder.create().build(),null, withCredentials, ResultSet.TYPE_FORWARD_ONLY
                );

        URI uri = statement.buildRequestUri(null, null, null, null, false);
        String query = uri.getQuery();
        assertTrue(query.contains("password=test_password"));
        assertTrue(query.contains("user=test_user"));
    }

    @Test
    public void testMaxExecutionTime() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setMaxExecutionTime(20);
        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(HttpClientBuilder.create().build(), null,
                properties, ResultSet.TYPE_FORWARD_ONLY);
        URI uri = statement.buildRequestUri(null, null, null, null, false);
        String query = uri.getQuery();
        assertTrue(query.contains("max_execution_time=20"), "max_execution_time param is missing in URL");
        
        statement.setQueryTimeout(10);
        uri = statement.buildRequestUri(null, null, null, null, false);
        query = uri.getQuery();
        assertTrue(query.contains("max_execution_time=10"), "max_execution_time param is missing in URL");
    }
    
    @Test
    public void testMaxMemoryUsage() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setMaxMemoryUsage(41L);
        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(HttpClientBuilder.create().build(), null,
                properties, ResultSet.TYPE_FORWARD_ONLY);

        URI uri = statement.buildRequestUri(null, null, null, null, false);
        String query = uri.getQuery();
        assertTrue(query.contains("max_memory_usage=41"), "max_memory_usage param is missing in URL");
    }

    @Test
    public void testAdditionalRequestParams() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(
                HttpClientBuilder.create().build(),
                null,
                properties,
                ResultSet.TYPE_FORWARD_ONLY
        );

        URI uri = statement.buildRequestUri(
                null,
                null,
                null,
                ImmutableMap.of("cache_namespace", "aaaa"),
                false
        );
        String query = uri.getQuery();
        assertTrue(query.contains("cache_namespace=aaaa"), "cache_namespace param is missing in URL");
    }

    @Test
    public void testIsSelect() {
        assertTrue(ClickHouseStatementImpl.isSelect("SELECT 42"));
        assertTrue(ClickHouseStatementImpl.isSelect("select 42"));
        assertFalse(ClickHouseStatementImpl.isSelect("selectfoo"));
        assertTrue(ClickHouseStatementImpl.isSelect("  SELECT foo"));
        assertTrue(ClickHouseStatementImpl.isSelect("WITH foo"));
        assertTrue(ClickHouseStatementImpl.isSelect("DESC foo"));
        assertTrue(ClickHouseStatementImpl.isSelect("EXISTS foo"));
        assertTrue(ClickHouseStatementImpl.isSelect("SHOW foo"));
        assertTrue(ClickHouseStatementImpl.isSelect("-- foo\n SELECT 42"));
        assertTrue(ClickHouseStatementImpl.isSelect("--foo\n SELECT 42"));
        assertFalse(ClickHouseStatementImpl.isSelect("- foo\n SELECT 42"));
        assertTrue(ClickHouseStatementImpl.isSelect("/* foo */ SELECT 42"));
        assertTrue(ClickHouseStatementImpl.isSelect("/*\n * foo\n*/\n SELECT 42"));
        assertFalse(ClickHouseStatementImpl.isSelect("/ foo */ SELECT 42"));
        assertFalse(ClickHouseStatementImpl.isSelect("-- SELECT baz\n UPDATE foo"));
        assertFalse(ClickHouseStatementImpl.isSelect("/* SELECT baz */\n UPDATE foo"));
        assertFalse(ClickHouseStatementImpl.isSelect("/*\n UPDATE foo"));
        assertFalse(ClickHouseStatementImpl.isSelect("/*"));
        assertFalse(ClickHouseStatementImpl.isSelect("/**/"));
        assertFalse(ClickHouseStatementImpl.isSelect(" --"));
    }

}
