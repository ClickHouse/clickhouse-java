package com.clickhouse.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.ClickHouseHttpConnectionFactory;
import com.clickhouse.data.ClickHouseColumn;

import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseDatabaseMetaDataTest extends JdbcIntegrationTest {
    @BeforeMethod(groups = "integration")
    public void setV1() {
        System.setProperty("clickhouse.jdbc.v1","true");
    }

    private static final Logger log = LoggerFactory.getLogger(ClickHouseDatabaseMetaDataTest.class);
    @DataProvider(name = "selectedColumns")
    private Object[][] getSelectedColumns() {
        return new Object[][] {
                // COLUMN_SIZE, DECIMAL_DIGITS, CHAR_OCTET_LENGTH
                // new Object[] { "Bool", 1, null, null }, // Bool was an alias before 21.12
                new Object[] { "Int8", 3, 0, null },
                new Object[] { "UInt8", 3, 0, null },
                new Object[] { "FixedString(3)", 3, null, 3 },
                new Object[] { "String", 0, null, null },
                new Object[] { "Date", 10, 0, null },
                new Object[] { "DateTime64(5)", 29, 5, null },
                new Object[] { "Decimal64(10)", 18, 10, null },
                new Object[] { "Decimal(10,2)", 10, 2, null },
                new Object[] { "Decimal(12,0)", 12, 0, null },
                new Object[] { "Float32", 12, 0, null },
                new Object[] { "Float64", 22, 0, null } };
    }

    @Test(groups = "integration")
    public void testDatabaseTerm() throws SQLException {
        Properties props = new Properties();
        props.setProperty("databaseTerm", "schema");
        try (ClickHouseConnection conn = newConnection(props)) {
            DatabaseMetaData md = conn.getMetaData();
            Assert.assertEquals(md.getCatalogTerm(), "catalog");
            Assert.assertFalse(md.getCatalogs().next());
            Assert.assertFalse(md.supportsCatalogsInDataManipulation());
            Assert.assertFalse(md.supportsCatalogsInIndexDefinitions());
            Assert.assertFalse(md.supportsCatalogsInPrivilegeDefinitions());
            Assert.assertFalse(md.supportsCatalogsInProcedureCalls());
            Assert.assertFalse(md.supportsCatalogsInTableDefinitions());

            Assert.assertEquals(md.getSchemaTerm(), "database");
            Assert.assertTrue(md.getSchemas().next());
            Assert.assertTrue(md.supportsSchemasInDataManipulation());
            Assert.assertTrue(md.supportsSchemasInIndexDefinitions());
            Assert.assertTrue(md.supportsSchemasInPrivilegeDefinitions());
            Assert.assertTrue(md.supportsSchemasInProcedureCalls());
            Assert.assertTrue(md.supportsSchemasInTableDefinitions());
        }

        props.setProperty("databaseTerm", "catalog");
        try (ClickHouseConnection conn = newConnection(props)) {
            DatabaseMetaData md = conn.getMetaData();
            Assert.assertEquals(md.getCatalogTerm(), "database");
            Assert.assertTrue(md.getCatalogs().next());
            Assert.assertTrue(md.supportsCatalogsInDataManipulation());
            Assert.assertTrue(md.supportsCatalogsInIndexDefinitions());
            Assert.assertTrue(md.supportsCatalogsInPrivilegeDefinitions());
            Assert.assertTrue(md.supportsCatalogsInProcedureCalls());
            Assert.assertTrue(md.supportsCatalogsInTableDefinitions());

            Assert.assertEquals(md.getSchemaTerm(), "schema");
            Assert.assertFalse(md.getSchemas().next());
            Assert.assertFalse(md.supportsSchemasInDataManipulation());
            Assert.assertFalse(md.supportsSchemasInIndexDefinitions());
            Assert.assertFalse(md.supportsSchemasInPrivilegeDefinitions());
            Assert.assertFalse(md.supportsSchemasInProcedureCalls());
            Assert.assertFalse(md.supportsSchemasInTableDefinitions());
        }
    }

    @Test(groups = "integration")
    public void testGetTypeInfo() throws SQLException {
        Properties props = new Properties();
        props.setProperty("decompress", "0");
        try (ClickHouseConnection conn = newConnection(props); ResultSet rs = conn.getMetaData().getTypeInfo()) {
            while (rs.next()) {
                Assert.assertNotNull(rs.getString(1));
            }
        }
    }

    @Test(groups = "integration")
    public void testGetClientInfo() throws SQLException {
        String clientName = "";
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                ResultSet rs = conn.getMetaData().getClientInfoProperties()) {
            while (rs.next()) {
                if (ClickHouseConnection.PROP_APPLICATION_NAME.equals(rs.getString(1))) {
                    clientName = rs.getString(3);
                }
            }
            Assert.assertEquals(clientName, ClickHouseClientOption.CLIENT_NAME.getDefaultValue());
        }

        props.setProperty(ClickHouseClientOption.CLIENT_NAME.getKey(), "client1");
        try (ClickHouseConnection conn = newConnection(props)) {
            clientName = "";
            try (ResultSet rs = conn.getMetaData().getClientInfoProperties()) {
                while (rs.next()) {
                    if (ClickHouseConnection.PROP_APPLICATION_NAME.equals(rs.getString(1))) {
                        clientName = rs.getString(3);
                    }
                }
                Assert.assertEquals(clientName, "client1");
            }

            conn.setClientInfo(ClickHouseConnection.PROP_APPLICATION_NAME, "client2");
            clientName = "";
            try (ResultSet rs = conn.getMetaData().getClientInfoProperties()) {
                while (rs.next()) {
                    if (ClickHouseConnection.PROP_APPLICATION_NAME.equals(rs.getString(1))) {
                        clientName = rs.getString(3);
                    }
                }
                Assert.assertEquals(clientName, "client2");
            }
        }
    }

    @Test(dataProvider = "selectedColumns", groups = "integration")
    public void testGetColumns(String columnType, Integer columnSize, Integer decimalDigits, Integer octectLength)
            throws SQLException {
        ClickHouseColumn c = ClickHouseColumn.of("x", columnType);
        String tableName = "test_get_column_" + c.getDataType().name().toLowerCase();
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists " + tableName + "; "
                    + "create table " + tableName + "(x " + columnType + ") engine=Memory");
            try (ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(), conn.getSchema(), tableName, "%")) {
                Assert.assertTrue(rs.next(), "Should have one record");
                Assert.assertEquals(rs.getString("cOLUMN_NAME"), "x");
                Assert.assertEquals(rs.getObject("COLUMN_SIZE"), columnSize);
                Assert.assertEquals(rs.getObject("DECIMAL_DIGITS"), decimalDigits);
                Assert.assertEquals(rs.getObject("CHAR_OCTET_LENGTH"), octectLength);
                Assert.assertFalse(rs.next(), "Should have only one record");
            }
        }
    }

    @Test(groups = "integration")
    public void testMaxRows() throws SQLException {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.MAX_RESULT_ROWS.getKey(), "1");
        int count = 0;
        try (ClickHouseConnection conn = newConnection(props)) {
            try (ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(), conn.getSchema(), "%", "%")) {
                while (rs.next()) {
                    count++;
                }
            }
        }
        Assert.assertTrue(count > 1, "Should have more than one row returned");
    }

    @Test(groups = "integration")
    public void testTableComment() throws SQLException {
        String tableName = "test_table_comment";
        String tableComment = "table comments";
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            // https://github.com/ClickHouse/ClickHouse/pull/30852
            if (!conn.getServerVersion().check("[21.6,)")) {
                return;
            }

            s.execute(String.format(Locale.ROOT,
                    "drop table if exists %1$s; create table %1$s(s String) engine=Memory comment '%2$s'",
                    tableName, tableComment));
            try (ResultSet rs = conn.getMetaData().getTables(conn.getCatalog(), conn.getSchema(), tableName, null)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString("remarks"), tableComment);
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = "integration")
    public void testGetTables() throws SQLException {
        if (isCloud()) return; //TODO: testGetTables - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        String db1 = "a" + UUID.randomUUID().toString().replace('-', 'X');
        String db2 = "b" + UUID.randomUUID().toString().replace('-', 'X');
        String tableName = "test_get_tables";
        Properties props = new Properties();
        props.setProperty("databaseTerm", "catalog");
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            // no record
            try (ResultSet rs = s.executeQuery("select * from numbers(1) where number=-1")) {
                Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
                Assert.assertFalse(rs.isFirst(), "Should NOT be the first");
                Assert.assertTrue(rs.isAfterLast(), "Should be after the last");
                Assert.assertFalse(rs.isLast(), "Should NOT be the last");

                Assert.assertFalse(rs.next(), "Should NOT have any row");
                Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
                Assert.assertFalse(rs.isFirst(), "Should NOT be the first");
                Assert.assertTrue(rs.isAfterLast(), "Should be after the last");
                Assert.assertFalse(rs.isLast(), "Should NOT be the last");

                Assert.assertFalse(rs.next(), "Should NOT have any row");
            }

            try (ResultSet rs = conn.getMetaData().getTables(null, null, UUID.randomUUID().toString(), null)) {
                Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
                Assert.assertFalse(rs.isFirst(), "Should NOT be the first");
                Assert.assertTrue(rs.isAfterLast(), "Should be after the last");
                Assert.assertFalse(rs.isLast(), "Should NOT be the last");

                Assert.assertFalse(rs.next(), "Should NOT have any row");
                Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
                Assert.assertFalse(rs.isFirst(), "Should NOT be the first");
                Assert.assertTrue(rs.isAfterLast(), "Should be after the last");
                Assert.assertFalse(rs.isLast(), "Should NOT be the last");

                Assert.assertFalse(rs.next(), "Should NOT have any row");
            }

            s.execute(String.format(Locale.ROOT, "create database %2$s; create database %3$s; "
                    + "create table %2$s.%1$s(id Int32, value String)engine=Memory; "
                    + "create view %3$s.%1$s as select * from %2$s.%1$s", tableName, db1, db2));
            try (ResultSet rs = conn.getMetaData().getTables(null, null, tableName,
                    new String[] { "MEMORY TABLE", "VIEW" })) {
                Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.isFirst(), "Should be the first row");
                Assert.assertEquals(rs.getString("TABLE_CAT"), db1);
                Assert.assertEquals(rs.getString("TABLE_NAME"), tableName);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString("TABLE_CAT"), db2);
                Assert.assertEquals(rs.getString("TABLE_NAME"), tableName);
                Assert.assertTrue(rs.isLast(), "Should be the last row");
                Assert.assertFalse(rs.next());
                Assert.assertTrue(rs.isAfterLast(), "Should be after the last row");
            } finally {
                s.execute(String.format(Locale.ROOT, "drop database %1$s; drop database %2$s", db1, db2));
            }

            try (ResultSet rs1 = s.executeQuery("select * from system.tables");
                    ResultSet rs2 = conn.getMetaData().getTables(null, null, null, null)) {
                int count1 = 0 , count1withOutSystem = 0;
                while (rs1.next()) {
                    log.debug("%s.%s", rs1.getString(1) , rs1.getString(2));
                    String databaseName = rs1.getString(1);
                    count1++;
                    if (!databaseName.equals("system"))
                        count1withOutSystem++;
                }
                log.debug("--------- SEP ---------");
                int count2 = 0, count2withOutSystem = 0;
                while (rs2.next()) {
                    log.debug("%s.%s", rs2.getString("TABLE_CAT") , rs2.getString("TABLE_NAME"));
                    String databaseName = rs2.getString("TABLE_CAT");
                    count2++;
                    if (!databaseName.equals("system"))
                        count2withOutSystem++;
                }

                Assert.assertEquals(rs1.getRow(), count1);
                Assert.assertEquals(rs2.getRow(), count2);
                Assert.assertEquals(count1withOutSystem, count2withOutSystem);
            }
        }
    }
}
