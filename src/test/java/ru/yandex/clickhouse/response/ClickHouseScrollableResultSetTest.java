package ru.yandex.clickhouse.response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.TimeZone;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

public class ClickHouseScrollableResultSetTest extends ClickHouseResultSetTest {

    ClickHouseProperties props = new ClickHouseProperties();
    TimeZone tz = TimeZone.getDefault();


    @Override
    @DataProvider(name = "longArrays")
    public Object[][] longArrays() {
        return new Object[][]{
                {"[0]", new long[]{0}},
                {"[333000111222,1024,-8521551,9223372036854775807,-9223372036854775808]",
                	new long[]{333000111222L, 1024, -8521551,9223372036854775807L,-9223372036854775808L}},
                {"[]", new long[]{}},
        };
    }

    @Override
    @Test
    public void testIsLast() throws Exception {
        String response =
                "SiteName\tcount()\n" +
                        "String\tUInt64\n" +
                        "hello.com\t21209048\n" +
                        "there.com\t49302091\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

        rs.next();
        assertFalse(rs.isLast());
        rs.next();
        assertTrue(rs.isLast());
        assertFalse(rs.next());
    }

    @Test
    public void testPrevious() throws Exception {
        String response =
                "SiteName\tcount()\n" +
                  "String\tUInt64\n" +
                  "hello.com\t21209048\n" +
                  "there.com\t49302091\n";

              ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

              ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

              rs.next();
              assertEquals("hello.com", rs.getString(1));
              assertEquals(21209048L, rs.getLong(2));

              rs.next();
              assertEquals("there.com", rs.getString(1));
              assertEquals(49302091L, rs.getLong(2));

              rs.previous();
              assertEquals("hello.com", rs.getString(1));
              assertEquals(21209048L, rs.getLong(2));
    }

    @Test
    public void testAbsolute() throws Exception {
        String response =
                "SiteName\tcount()\n" +
                  "String\tUInt64\n" +
                  "hello.com\t21209048\n" +
                  "there.com\t49302091\n";

              ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

              ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

              rs.absolute(2);
              assertEquals("there.com", rs.getString(1));
              assertEquals(49302091L, rs.getLong(2));

              rs.absolute(1);
              assertEquals("hello.com", rs.getString(1));
              assertEquals(21209048L, rs.getLong(2));

              rs.absolute(-1);
              assertEquals("there.com", rs.getString(1));
              assertEquals(49302091L, rs.getLong(2));

              assertTrue(rs.isLast());

    }

    public void testAbsoluteWithTotal() throws Exception {
        String response = "SiteName\tcount()\n" +
                "String\tUInt64\n" +
                "hello.com\t21209048\n" +
                "there.com\t49302091\n" +
                "\n" +
                "\t70511139\n";

      ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

      ClickHouseResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

      rs.absolute(2);
      assertEquals("there.com", rs.getString(1));
      assertEquals(49302091L, rs.getLong(2));

      rs.absolute(1);
      assertEquals("hello.com", rs.getString(1));
      assertEquals(21209048L, rs.getLong(2));

      rs.absolute(-1);
      assertEquals("there.com", rs.getString(1));
      assertEquals(49302091L, rs.getLong(2));

      assertTrue(rs.isLast());

      assertFalse(rs.absolute(3));
      assertTrue(rs.isAfterLast());

      assertFalse(rs.absolute(-3));
      assertTrue(rs.isBeforeFirst());

      assertFalse(rs.absolute(0));
      assertTrue(rs.isBeforeFirst());

      rs.getTotals();
      assertEquals("", rs.getString(1));
      assertEquals(70511139L, rs.getLong(2));

    }

    @Test
    public void testRelative() throws Exception {
        String response =
                "SiteName\tcount()\n" +
                  "String\tUInt64\n" +
                  "hello.com\t21209048\n" +
                  "there.com\t49302091\n";

              ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

              ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

              rs.next();
              assertEquals("hello.com", rs.getString(1));
              assertEquals(21209048L, rs.getLong(2));

              rs.relative(0);
              assertEquals("hello.com", rs.getString(1));
              assertEquals(21209048L, rs.getLong(2));

              rs.relative(1);
              assertEquals("there.com", rs.getString(1));
              assertEquals(49302091L, rs.getLong(2));

              rs.relative(-1);
              assertEquals("hello.com", rs.getString(1));
              assertEquals(21209048L, rs.getLong(2));

              assertFalse(rs.relative(5));
              assertFalse(rs.relative(-5));

    }

    @Test
    public void testBeforeFirst() throws Exception {
        String response =
                "SiteName\tcount()\n" +
                  "String\tUInt64\n" +
                  "hello.com\t21209048\n" +
                  "there.com\t49302091\n";

              ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

              ClickHouseResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

              rs.next();
              assertEquals("hello.com", rs.getString(1));
              assertEquals(21209048L, rs.getLong(2));

              rs.beforeFirst();
              rs.next();
              assertEquals("hello.com", rs.getString(1));
              assertEquals(21209048L, rs.getLong(2));

              rs.afterLast();
              assertFalse(rs.hasNext());

    }

    private static ClickHouseResultSet buildResultSet(InputStream is, int bufferSize, String db, String table, boolean usesWithTotals, ClickHouseStatement statement, TimeZone timezone, ClickHouseProperties properties) throws IOException {
    	return new ClickHouseScrollableResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
    }
}
