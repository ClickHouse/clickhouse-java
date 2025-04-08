package com.clickhouse.jdbc.internal;

import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class JdbcUtilsTest {
    @Test(groups = { "integration" })
    public void testTokenizeSQL() {
        String sql1 = "SELECT * FROM table WHERE id = 1";
        List<String> tokens1 = JdbcUtils.tokenizeSQL(sql1);
        assertEquals(tokens1.size(), 8);
        assertEquals(tokens1.get(0), "SELECT");
        assertEquals(tokens1.get(1), "*");
        assertEquals(tokens1.get(2), "FROM");
        assertEquals(tokens1.get(3), "table");
        assertEquals(tokens1.get(4), "WHERE");
        assertEquals(tokens1.get(5), "id");
        assertEquals(tokens1.get(6), "=");
        assertEquals(tokens1.get(7), "1");

        String sql2 = "SELECT * FROM table WHERE id = 1 AND name = 'John'";
        List<String> tokens2 = JdbcUtils.tokenizeSQL(sql2);
        assertEquals(tokens2.size(), 12);
        assertEquals(tokens2.get(0), "SELECT");
        assertEquals(tokens2.get(1), "*");
        assertEquals(tokens2.get(2), "FROM");
        assertEquals(tokens2.get(3), "table");
        assertEquals(tokens2.get(4), "WHERE");
        assertEquals(tokens2.get(5), "id");
        assertEquals(tokens2.get(6), "=");
        assertEquals(tokens2.get(7), "1");
        assertEquals(tokens2.get(8), "AND");
        assertEquals(tokens2.get(9), "name");
        assertEquals(tokens2.get(10), "=");
        assertEquals(tokens2.get(11), "'John'");

        String sql3 = "SELECT * FROM table WHERE \"id = 1 AND name = 'John' OR age = 30\"";//Technically, this is not a valid SQL statement
        List<String> tokens3 = JdbcUtils.tokenizeSQL(sql3);
        assertEquals(tokens3.size(), 6);
        assertEquals(tokens3.get(0), "SELECT");
        assertEquals(tokens3.get(1), "*");
        assertEquals(tokens3.get(2), "FROM");
        assertEquals(tokens3.get(3), "table");
        assertEquals(tokens3.get(4), "WHERE");
        assertEquals(tokens3.get(5).replace("\"", ""), "id = 1 AND name = 'John' OR age = 30");
    }

    @Test
    public void testEscapeQuotes() {
        String[] inStr = new String[]{"%valid_name%", "' OR 1=1 --", "\" OR 1=1 --"};
        String[] outStr = new String[]{"%valid_name%", "\\' OR 1=1 --", "\\\" OR 1=1 --"};

        for (int i = 0; i < inStr.length; i++) {
            assertEquals(JdbcUtils.escapeQuotes(inStr[i]), outStr[i]);
        }
    }
}
