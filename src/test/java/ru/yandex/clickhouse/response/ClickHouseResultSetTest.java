package ru.yandex.clickhouse.response;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.ByteArrayInputStream;
import java.sql.ResultSet;
import java.util.Map;
import java.util.TimeZone;

import static org.testng.Assert.*;
import static org.testng.AssertJUnit.assertEquals;

public class ClickHouseResultSetTest {
    @DataProvider(name = "longArrays")
    public Object[][] longArrays() {
        return new Object[][]{
                {"[0]", new long[]{0}},
                {"[324000111222,123,-456]", new long[]{324000111222L, 123, -456}},
                {"[]", new long[]{}},
        };
    }

    @Test(dataProvider = "longArrays")
    public void toLongArrayTest(String str, long[] expected) throws Exception {
        Assert.assertEquals(
                ClickHouseResultSet.toLongArray(ByteFragment.fromString(str)),
                expected
        );
    }

    ClickHouseProperties props = new ClickHouseProperties();
    TimeZone tz = TimeZone.getDefault();

    @Test
    public void withoutTotals() throws Exception {
        String response =
          "SiteName\tcount()\n" +
            "String\tUInt64\n" +
            "hello.com\t21209048\n" +
            "there.com\t49302091\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ResultSet rs = new ClickHouseResultSet(is, 1024, "db", "table", false, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));
        assertEquals(21209048L, rs.getLong(2));

        rs.next();
        assertEquals("there.com", rs.getString(1));
        assertEquals(49302091L, rs.getLong(2));

        assertFalse(rs.next());
    }

    @Test
    public void withoutTotalsSingleColumn() throws Exception {
        String response =
          "SiteName\n" +
            "String\n" +
            "hello.com\n" +
            "there.com\n" +
            "\n" +
            "\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ClickHouseResultSet rs = new ClickHouseResultSet(is, 1024, "db", "table", false, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));

        rs.next();
        assertEquals("there.com", rs.getString(1));

        rs.next();
        assertEquals("", rs.getString(1));

        rs.next();
        assertEquals("", rs.getString(1));

        assertFalse(rs.next());
        try {
            rs.getTotals();
            fail("The getTotals shall fail when usesWithTotals is set to false.");
        } catch (IllegalStateException e) {
            // this is expected
        }
        assertFalse(rs.next());
    }

    @Test
    public void withTotals() throws Exception {
        String response = "SiteName\tcount()\n" +
          "String\tUInt64\n" +
          "hello.com\t21209048\n" +
          "there.com\t49302091\n" +
          "\n" +
          "\t70511139\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ClickHouseResultSet rs = new ClickHouseResultSet(is, 1024, "db", "table", true, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));
        assertEquals(21209048L, rs.getLong(2));

        rs.next();
        assertEquals("there.com", rs.getString(1));
        assertEquals(49302091L, rs.getLong(2));

        assertFalse(rs.next());

        rs.getTotals();
        assertEquals("", rs.getString(1));
        assertEquals(70511139L, rs.getLong(2));
    }

    public void withTotalsAndEmptyStrings() throws Exception {
        String response = "SiteName\tCountry\n" +
          "String\tString\n" +
          "hello.com\tPoland\n" +
          "there.com\tUSA\n" +
          "\t\n" +
          "other.com\t\n" +
          "\n" +
          "\t\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ClickHouseResultSet rs = new ClickHouseResultSet(is, 1024, "db", "table", true, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));
        assertEquals("Poland", rs.getString(2));


        rs.next();
        assertEquals("there.com", rs.getString(1));
        assertEquals("USA", rs.getString(2));

        rs.next();
        assertEquals("", rs.getString(1));
        assertEquals("", rs.getString(2));

        rs.next();
        assertEquals("other.com", rs.getString(1));
        assertEquals("", rs.getString(2));

        assertFalse(rs.next());

        rs.getTotals();
        assertEquals("", rs.getString(1));
        assertEquals(70511139L, rs.getLong(2));
    }

    @Test
    public void withTotalsSingleColumn() throws Exception {
        String response =
          "SiteName\n" +
            "String\n" +
            "hello.com\n" +
            "there.com\n" +
            "\n" +
            "other.com\n" +
            "\n" + // with totals separator row
            "\n"; // with totals values row

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ResultSet rs = new ClickHouseResultSet(is, 1024, "db", "table", true, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));

        rs.next();
        assertEquals("there.com", rs.getString(1));

        rs.next();
        assertEquals("", rs.getString(1));

        rs.next();
        assertEquals("other.com", rs.getString(1));

        assertFalse(rs.next());
    }

    @Test
    public void withTotalsSingleIntColumn() throws Exception {
        String response =
          "Code\n" +
            "UInt64\n" +
            "1\n" +
            "2\n" +
            "3\n" +
            "\n" + // with totals separator row
            "0"; // with totals values row

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ClickHouseResultSet rs = new ClickHouseResultSet(is, 1024, "db", "table", true, null, null, props);

        rs.next();
        assertEquals(1L, rs.getLong(1));

        rs.next();
        assertEquals(2L, rs.getLong(1));

        rs.next();
        assertEquals(3L, rs.getLong(1));

        assertFalse(rs.next());

        rs.getTotals();
        assertEquals(0L, rs.getLong(1));
    }

    @Test
    public void withTotalsSingleNullableColumn() throws Exception {
        String response =
          "SiteName\n" +
            "Nullable(String)\n" +
            "hello.com\n" +
            "there.com\n" +
            "\n" +
            "\\N\n" +
            "other.com\n" +
            "\n" + // with totals separator row
            "\\N\n";// with totals values row

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ClickHouseResultSet rs = new ClickHouseResultSet(is, 1024, "db", "table", true, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));

        rs.next();
        assertEquals("there.com", rs.getString(1));

        rs.next();
        assertEquals("", rs.getString(1));

        rs.next();
        assertNull(rs.getString(1));

        rs.next();
        assertEquals("other.com", rs.getString(1));

        assertFalse(rs.next());

        rs.getTotals();
        assertNull(rs.getString(1));
    }

    @Test
    public void testExtractTablesNamesForSimpleQuery() {
        String query =
                ("SELECT " +
                        "c1 as col1, " +
                        "c2, " +
                        "n3 " +
                        "FROM test.metadata_test_2").toLowerCase();

        Map<String, String> columns = ClickHouseResultSet.getTableNames(query, new String[]{"c1"});
        Assert.assertEquals("test.metadata_test_2", columns.get("col1"));
    }

    @Test
    public void testExtractTablesNamesForQueryWithJoinStatement() {
        String query = ("SELECT " +
                "c2, " +
                "n3 " +
                "FROM (" +
                "    SELECT " +
                "       i as commonColumn, " +
                "       s as c2, " +
                "       col1 " +
                "    FROM test.metadata_test_1 " +
                ") ANY LEFT JOIN (" +
                "SELECT " +
                "        i as commonColumn, " +
                "        s, " +
                "        col1 as n2, " +
                "        col2 AS n3 " +
                "FROM metadata_test_2)" +
                "USING commonColumn ").toLowerCase();

        Map<String, String> columns = ClickHouseResultSet.getTableNames(query, new String[]{"c2", "n3"});
        Assert.assertEquals("test.metadata_test_1", columns.get("c2"));
        Assert.assertEquals("metadata_test_2", columns.get("n3"));
    }

    @Test
    public void testExtractTablesNamesForJoinQueryWithStar() {
        String query = (
                "SELECT " +
                        " * " +
                        "FROM (" +
                        "    SELECT " +
                        "       i as commonColumn, " +
                        "       s as c2, " +
                        "       col1 " +
                        "    FROM test.metadata_test_1 " +
                        ") ANY LEFT JOIN (" +
                        "SELECT " +
                        "        i as commonColumn, " +
                        "        s, " +
                        "        col1 as n2, " +
                        "        col2 AS n3 " +
                        "FROM test.metadata_test_2)" +
                        "USING commonColumn ").toLowerCase();

        Map<String, String> columns = ClickHouseResultSet.getTableNames(query, new String[]{"c2", "n3"});
        Assert.assertEquals("test.metadata_test_1", columns.get("c2"));
        Assert.assertEquals("test.metadata_test_1", columns.get("n3"));
    }

    @Test
    public void testExtractTablesNamesForSimpleQueryWithStar() {
        String query = "SELECT * FROM test.metadata_test_2".toLowerCase();

        Map<String, String> columns = ClickHouseResultSet.getTableNames(query, new String[]{"c2", "n3"});
        Assert.assertEquals(2, columns.size());
        Assert.assertEquals("test.metadata_test_2", columns.get("c2"));
        Assert.assertEquals("test.metadata_test_2", columns.get("n3"));
    }
}
