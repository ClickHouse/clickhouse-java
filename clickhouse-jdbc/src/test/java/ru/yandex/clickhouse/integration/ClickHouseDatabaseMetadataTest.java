package ru.yandex.clickhouse.integration;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.TimeZone;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseDatabaseMetadata;

public class ClickHouseDatabaseMetadataTest {

    private ClickHouseDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        dataSource = ClickHouseContainerForTest.newDataSource();
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @AfterTest
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testMetadata() throws Exception {
        connection.createStatement().executeQuery(
            "DROP TABLE IF EXISTS test.testMetadata");
        connection.createStatement().executeQuery(
            "CREATE TABLE test.testMetadata("
          + "foo Nullable(UInt32), bar UInt64) ENGINE = TinyLog");
        ResultSet columns = connection.getMetaData().getColumns(
            null, "test", "testMetaData", null);
        while (columns.next()) {
            String colName = columns.getString("COLUMN_NAME");
            if ("foo".equals(colName)) {
                Assert.assertEquals(columns.getString("TYPE_NAME"), "UInt32");
            } else if ("bar".equals(columns.getString("COLUMN_NAME"))) {
                Assert.assertEquals(columns.getString("TYPE_NAME"), "UInt64");
            } else {
                throw new IllegalStateException(
                    "Unexpected column name " + colName);
            }
        }
    }

    @Test
    public void testMetadataColumns() throws Exception {
        connection.createStatement().executeQuery(
            "DROP TABLE IF EXISTS test.testMetadata");
        connection.createStatement().executeQuery(
            "CREATE TABLE test.testMetadata("
          + "foo Float32, bar UInt8 DEFAULT 42 COMMENT 'baz') ENGINE = TinyLog");
        ResultSet columns = connection.getMetaData().getColumns(
            null, "test", "testMetadata", null);
        columns.next();
        Assert.assertEquals(columns.getString("TABLE_CAT"), "default");
        Assert.assertEquals(columns.getString("TABLE_SCHEM"), "test");
        Assert.assertEquals(columns.getString("TABLE_NAME"), "testMetadata");
        Assert.assertEquals(columns.getString("COLUMN_NAME"), "foo");
        Assert.assertEquals(columns.getInt("DATA_TYPE"), Types.REAL);
        Assert.assertEquals(columns.getString("TYPE_NAME"), "Float32");
        Assert.assertEquals(columns.getInt("COLUMN_SIZE"), 8);
        Assert.assertEquals(columns.getInt("BUFFER_LENGTH"), 0);
        Assert.assertEquals(columns.getInt("DECIMAL_DIGITS"), 8);
        Assert.assertEquals(columns.getInt("NUM_PREC_RADIX"), 10);
        Assert.assertEquals(columns.getInt("NULLABLE"), DatabaseMetaData.columnNoNulls);
        Assert.assertNull(columns.getObject("REMARKS"));
        Assert.assertNull(columns.getObject("COLUMN_DEF"));
        Assert.assertNull(columns.getObject("SQL_DATA_TYPE"));
        Assert.assertNull(columns.getObject("SQL_DATETIME_SUB"));
        Assert.assertEquals(columns.getInt("CHAR_OCTET_LENGTH"), 0);
        Assert.assertEquals(columns.getInt("ORDINAL_POSITION"), 1);
        Assert.assertEquals(columns.getString("IS_NULLABLE"), "NO");
        Assert.assertNull(columns.getObject("SCOPE_CATALOG"));
        Assert.assertNull(columns.getObject("SCOPE_SCHEMA"));
        Assert.assertNull(columns.getObject("SCOPE_TABLE"));
        Assert.assertNull(columns.getObject("SOURCE_DATA_TYPE"));
        Assert.assertEquals(columns.getString("IS_AUTOINCREMENT"), "NO");
        Assert.assertEquals(columns.getString("IS_GENERATEDCOLUMN"), "NO");
        columns.next();
        Assert.assertEquals(columns.getInt("COLUMN_DEF"), 42);
        Assert.assertEquals(columns.getObject("REMARKS"), "baz");
    }

    @Test
    public void testDriverVersion() throws Exception {
        DatabaseMetaData metaData = new ClickHouseDatabaseMetadata(
            "url", Mockito.mock(ClickHouseConnection.class));
        Assert.assertEquals(metaData.getDriverVersion(), "0.1");
        Assert.assertEquals(metaData.getDriverMajorVersion(), 0);
        Assert.assertEquals(metaData.getDriverMinorVersion(), 1);
    }

    @Test
    public void testDatabaseVersion() throws Exception {
        String dbVersion = connection.getMetaData().getDatabaseProductVersion();
        Assert.assertFalse(dbVersion == null || dbVersion.isEmpty());
        int dbMajor = Integer.parseInt(dbVersion.substring(0, dbVersion.indexOf(".")));
        Assert.assertTrue(dbMajor > 0);
        Assert.assertEquals(connection.getMetaData().getDatabaseMajorVersion(), dbMajor);
        int majorIdx = dbVersion.indexOf(".") + 1;
        int dbMinor = Integer.parseInt(dbVersion.substring(majorIdx, dbVersion.indexOf(".", majorIdx)));
        Assert.assertEquals(connection.getMetaData().getDatabaseMinorVersion(), dbMinor);
    }

    @Test(dataProvider = "tableEngines")
    public void testGetTablesEngines(String engine) throws Exception {
        connection.createStatement().executeQuery(
            "DROP TABLE IF EXISTS test.testMetadata");
        connection.createStatement().executeQuery(
            "CREATE TABLE test.testMetadata("
          + "foo Date) ENGINE = "
          + engine);
        ResultSet tableMeta = connection.getMetaData().getTables(null, "test", "testMetadata", null);
        tableMeta.next();
        Assert.assertEquals("TABLE", tableMeta.getString("TABLE_TYPE"));
    }

    @Test
    public void testGetTablesViews() throws Exception {
        connection.createStatement().executeQuery(
            "DROP TABLE IF EXISTS test.testMetadataView");
        connection.createStatement().executeQuery(
            "CREATE VIEW test.testMetadataView AS SELECT 1 FROM system.tables");
        ResultSet tableMeta = connection.getMetaData().getTables(
            null, "test", "testMetadataView", null);
        tableMeta.next();
        Assert.assertEquals("VIEW", tableMeta.getString("TABLE_TYPE"));
    }

    @Test
    public void testToDateTimeTZ() throws Exception {
        connection.createStatement().executeQuery(
            "DROP TABLE IF EXISTS test.testDateTimeTZ");
        connection.createStatement().executeQuery(
            "CREATE TABLE test.testDateTimeTZ (foo DateTime) Engine = Memory");
        connection.createStatement().execute(
            "INSERT INTO test.testDateTimeTZ (foo) VALUES('2019-04-12 13:37:00')");
        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT toDateTime(foo) FROM test.testDateTimeTZ");
        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertEquals(meta.getColumnClassName(1), Timestamp.class.getCanonicalName());
        TimeZone timezone = ((ClickHouseConnection) connection).getTimeZone();
        Assert.assertEquals(meta.getColumnTypeName(1), "DateTime('" + timezone.getID() + "')");
        Assert.assertEquals(meta.getColumnType(1), Types.TIMESTAMP);
    }

    @DataProvider(name = "tableEngines")
    private Object[][] getTableEngines() {
        return new Object[][] {
            new String[] {"TinyLog"},
            new String[] {"Log"},
            new String[] {"Memory"},
            new String[] {"MergeTree(foo, (foo), 8192)"}
        };
        // unfortunately this is hard to test
        // new String[] {"Dictionary(myDict)"},
    }

}
