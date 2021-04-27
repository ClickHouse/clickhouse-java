package ru.yandex.clickhouse;


import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.impl.client.HttpClientBuilder;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class ClickHouseStatementTest {
    @Test
    public void testClickhousify() throws Exception {
        ClickHouseStatementImpl s = new ClickHouseStatementImpl(null, null, null, ResultSet.TYPE_FORWARD_ONLY);
        String sql = "SELECT ololo FROM ololoed;";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            "SELECT ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes");

        sql = "SELECT ololo FROM ololoed";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            "SELECT ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes");

        sql = "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes");

        sql = "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes");

        sql = "SHOW ololo FROM ololoed;";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            "SHOW ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes");

        sql = " show ololo FROM ololoed;";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            " show ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes");

        sql = "SELECT ololo FROM ololoed \nFORMAT TabSeparatedWithNamesAndTypes";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            "SELECT ololo FROM ololoed \nFORMAT TabSeparatedWithNamesAndTypes");

        sql = "SELECT ololo FROM ololoed \n\n FORMAT TabSeparatedWithNamesAndTypes";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            "SELECT ololo FROM ololoed \n\n FORMAT TabSeparatedWithNamesAndTypes");

        sql = "SELECT ololo FROM ololoed\n-- some comments one line";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            "SELECT ololo FROM ololoed\n-- some comments one line\nFORMAT TabSeparatedWithNamesAndTypes");

        sql = "SELECT ololo FROM ololoed\n-- some comments\ntwo line";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            "SELECT ololo FROM ololoed\n-- some comments\ntwo line\nFORMAT TabSeparatedWithNamesAndTypes");

        sql = "SELECT ololo FROM ololoed/*\nsome comments\ntwo line*/";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            "SELECT ololo FROM ololoed/*\nsome comments\ntwo line*/\nFORMAT TabSeparatedWithNamesAndTypes");

        sql = "SELECT ololo FROM ololoed\n// c style some comments one line";
        assertEquals(s.parseSqlStatements(sql, ClickHouseFormat.TabSeparatedWithNamesAndTypes, null).getSQL(),
            "SELECT ololo FROM ololoed\n// c style some comments one line\nFORMAT TabSeparatedWithNamesAndTypes");

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
            HttpClientBuilder.create().build(),
            null, withCredentials, ResultSet.TYPE_FORWARD_ONLY);

        URI uri = statement.buildRequestUri(null, null, null, null, false);
        String query = uri.getQuery();
        // we use Basic AUTH nowadays
        assertFalse(query.contains("password=test_password"));
        assertFalse(query.contains("user=test_user"));
    }

    @Test
    public void testMaxExecutionTime() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setMaxExecutionTime(20);
        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(HttpClientBuilder.create().build(),
            null, properties, ResultSet.TYPE_FORWARD_ONLY);
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
        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(HttpClientBuilder.create().build(),
           null, properties, ResultSet.TYPE_FORWARD_ONLY);

        URI uri = statement.buildRequestUri(null, null, null, null, false);
        String query = uri.getQuery();
        assertTrue(query.contains("max_memory_usage=41"), "max_memory_usage param is missing in URL");
    }

    @Test
    public void testAdditionalRequestParams() {
        ClickHouseProperties properties = new ClickHouseProperties();
        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(
                HttpClientBuilder.create().build(),
                null,
                properties,
                ResultSet.TYPE_FORWARD_ONLY
        );

        statement.option("cache_namespace", "aaaa");
        URI uri = statement.buildRequestUri(
                null,
                null,
                null,
                null,
                false
        );
        String query = uri.getQuery();
        assertTrue(query.contains("cache_namespace=aaaa"), "cache_namespace param is missing in URL");

        uri = statement.buildRequestUri(
                null,
                null,
                null,
                Collections.singletonMap("cache_namespace", "bbbb"),
                false
        );
        query = uri.getQuery();
        assertTrue(query.contains("cache_namespace=bbbb"), "cache_namespace param is missing in URL");

        // check that statement level params are given to Writer
        assertEquals(statement.write().getRequestParams().get("cache_namespace"), "aaaa");
    }

    @Test
    public void testAdditionalDBParams() {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setMaxThreads(1);

        ClickHouseStatementImpl statement = new ClickHouseStatementImpl(
                HttpClientBuilder.create().build(),
                null,
                properties,
                ResultSet.TYPE_FORWARD_ONLY
        );

        URI uri = statement.buildRequestUri(null, null, null, null, false);
        assertTrue(uri.getQuery().contains("max_threads=1"));

        // override on statement level
        statement.addDbParam(ClickHouseQueryParam.MAX_THREADS, "2");

        uri = statement.buildRequestUri(null, null, null, null, false);
        assertTrue(uri.getQuery().contains("max_threads=2"));

        // override on method level
        uri = statement.buildRequestUri(null, null, Collections.singletonMap(ClickHouseQueryParam.MAX_THREADS, "3"), null, false);
        assertTrue(uri.getQuery().contains("max_threads=3"));

        // check that statement level params are given to Writer
        assertEquals(statement.write().getAdditionalDBParams().get(ClickHouseQueryParam.MAX_THREADS), "2");
    }

    @Test
    public void testIsSelect() throws SQLException {
        ClickHouseStatementImpl s = new ClickHouseStatementImpl(null, null, null, ResultSet.TYPE_FORWARD_ONLY);
        assertTrue(s.parseSqlStatements("SELECT 42")[0].isQuery());
        assertTrue(s.parseSqlStatements("select 42")[0].isQuery());
        assertFalse(s.parseSqlStatements("selectfoo")[0].isQuery());
        assertTrue(s.parseSqlStatements("  SELECT foo")[0].isQuery());
        assertFalse(s.parseSqlStatements("WITH foo")[0].isQuery());
        assertTrue(s.parseSqlStatements("DESC foo")[0].isQuery());
        assertTrue(s.parseSqlStatements("EXISTS foo")[0].isQuery());
        assertTrue(s.parseSqlStatements("SHOW foo")[0].isQuery());
        assertTrue(s.parseSqlStatements("-- foo\n SELECT 42")[0].isQuery());
        assertTrue(s.parseSqlStatements("--foo\n SELECT 42")[0].isQuery());
        assertFalse(s.parseSqlStatements("- foo\n SELECT 42")[0].isQuery());
        assertTrue(s.parseSqlStatements("/* foo */ SELECT 42")[0].isQuery());
        assertTrue(s.parseSqlStatements("/*\n * foo\n*/\n SELECT 42")[0].isQuery());
        assertFalse(s.parseSqlStatements("/ foo */ SELECT 42")[0].isQuery());
        assertFalse(s.parseSqlStatements("-- SELECT baz\n UPDATE foo")[0].isQuery());
        assertFalse(s.parseSqlStatements("/* SELECT baz */\n UPDATE foo")[0].isQuery());
        assertFalse(s.parseSqlStatements("/*\n UPDATE foo")[0].isQuery());
        assertFalse(s.parseSqlStatements("/*")[0].isQuery());
        assertFalse(s.parseSqlStatements("/**/")[0].isQuery());
        assertFalse(s.parseSqlStatements(" --")[0].isQuery());
        assertTrue(s.parseSqlStatements("explain select 42")[0].isQuery());
        assertTrue(s.parseSqlStatements("EXPLAIN select 42")[0].isQuery());
        assertFalse(s.parseSqlStatements("--EXPLAIN select 42\n alter")[0].isQuery());
        assertTrue(s.parseSqlStatements("--\nEXPLAIN select 42")[0].isQuery());
        assertTrue(s.parseSqlStatements("/*test*/ EXPLAIN select 42")[0].isQuery());
    }

}
