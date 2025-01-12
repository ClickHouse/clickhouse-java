package com.clickhouse.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseSimpleResponse;
import com.clickhouse.data.ClickHouseColumn;

public class CombinedResultSetTest {
    @BeforeMethod(groups = "integration")
    public void setV1() {
        System.setProperty("clickhouse.jdbc.v1","true");
    }
    @DataProvider(name = "multipleResultSetsProvider")
    private Object[][] getMultipleResultSets() {
        ClickHouseConfig config = new ClickHouseConfig();
        return new Object[][] {
                { new CombinedResultSet(null, new ClickHouseResultSet("", "",
                        ClickHouseSimpleResponse.of(config, ClickHouseColumn.parse("s String"),
                                new Object[][] { new Object[] { "a" },
                                        new Object[] { "b" } })),
                        new ClickHouseResultSet("", "",
                                ClickHouseSimpleResponse.of(config,
                                        ClickHouseColumn.parse("s String"),
                                        new Object[][] { new Object[] { "c" },
                                                new Object[] { "d" },
                                                new Object[] { "e" } }))) },
                { new CombinedResultSet(Arrays.asList(null, null,
                        new ClickHouseResultSet("", "",
                                ClickHouseSimpleResponse.of(config,
                                        ClickHouseColumn.parse("s String"),
                                        new Object[][] { new Object[] {
                                                "a" } })),
                        null,
                        new ClickHouseResultSet("", "",
                                ClickHouseSimpleResponse.of(config,
                                        ClickHouseColumn.parse("s String"),
                                        new Object[][] { new Object[] {
                                                "b" } })),
                        new ClickHouseResultSet("", "",
                                ClickHouseSimpleResponse.of(config,
                                        ClickHouseColumn.parse("s String"),
                                        new Object[][] {
                                                new Object[] { "c" },
                                                new Object[] { "d" },
                                                new Object[] { "e" } })))) } };
    }

    @DataProvider(name = "nullOrEmptyResultSetProvider")
    private Object[][] getNullOrEmptyResultSet() {
        return new Object[][] { { new CombinedResultSet() }, { new CombinedResultSet((ResultSet) null) },
                { new CombinedResultSet(null, null) }, { new CombinedResultSet(null, null, null) },
                { new CombinedResultSet(Collections.emptyList()) },
                { new CombinedResultSet(Collections.singleton(null)) },
                { new CombinedResultSet(Arrays.asList(null, null)) },
                { new CombinedResultSet(Arrays.asList(null, null, null)) } };
    }

    @DataProvider(name = "singleResultSetProvider")
    private Object[][] getSingleResultSet() {
        ClickHouseConfig config = new ClickHouseConfig();
        return new Object[][] {
                { new CombinedResultSet(new ClickHouseResultSet("", "",
                        ClickHouseSimpleResponse.of(config, ClickHouseColumn.parse("s String"),
                                new Object[][] { new Object[] { "a" },
                                        new Object[] { "b" } }))) },
                { new CombinedResultSet(Collections.singleton(
                        new ClickHouseResultSet("", "", ClickHouseSimpleResponse.of(config,
                                ClickHouseColumn.parse("s String"),
                                new Object[][] { new Object[] { "a" },
                                        new Object[] { "b" } })))) } };
    }

    @Test(dataProvider = "multipleResultSetsProvider", groups = "unit")
    public void testMultipleResultSets(CombinedResultSet combined) throws SQLException {
        Assert.assertFalse(combined.isClosed());
        Assert.assertEquals(combined.getRow(), 0);
        Assert.assertTrue(combined.next());
        Assert.assertEquals(combined.getRow(), 1);
        Assert.assertEquals(combined.getString(1), "a");
        Assert.assertTrue(combined.next());
        Assert.assertEquals(combined.getRow(), 2);
        Assert.assertEquals(combined.getString(1), "b");
        Assert.assertTrue(combined.next());
        Assert.assertEquals(combined.getRow(), 3);
        Assert.assertEquals(combined.getString(1), "c");
        Assert.assertTrue(combined.next());
        Assert.assertEquals(combined.getRow(), 4);
        Assert.assertEquals(combined.getString(1), "d");
        Assert.assertTrue(combined.next());
        Assert.assertEquals(combined.getRow(), 5);
        Assert.assertEquals(combined.getString(1), "e");
        Assert.assertFalse(combined.next());
        Assert.assertFalse(combined.next());
        Assert.assertEquals(combined.getRow(), 5);
        combined.close();
        Assert.assertTrue(combined.isClosed());
    }

    @Test(dataProvider = "nullOrEmptyResultSetProvider", groups = "unit")
    public void testNullAndEmptyResultSet(CombinedResultSet combined) throws SQLException {
        Assert.assertFalse(combined.isClosed());
        Assert.assertEquals(combined.getRow(), 0);
        Assert.assertFalse(combined.next());
        Assert.assertEquals(combined.getRow(), 0);
        Assert.assertFalse(combined.next());
        Assert.assertEquals(combined.getRow(), 0);
        combined.close();
        Assert.assertTrue(combined.isClosed());
        Assert.assertThrows(SQLException.class, () -> combined.getString(1));
    }

    @Test(dataProvider = "singleResultSetProvider", groups = "unit")
    public void testSingleResultSet(CombinedResultSet combined) throws SQLException {
        Assert.assertFalse(combined.isClosed());
        Assert.assertEquals(combined.getRow(), 0);
        Assert.assertTrue(combined.next());
        Assert.assertEquals(combined.getRow(), 1);
        Assert.assertEquals(combined.getString(1), "a");
        Assert.assertTrue(combined.next());
        Assert.assertEquals(combined.getRow(), 2);
        Assert.assertEquals(combined.getString(1), "b");
        Assert.assertFalse(combined.next());
        Assert.assertFalse(combined.next());
        Assert.assertEquals(combined.getRow(), 2);
        combined.close();
        Assert.assertTrue(combined.isClosed());
    }

    @Test(groups = "unit")
    public void testFetchSize() throws SQLException {
        try (CombinedResultSet rs = new CombinedResultSet(new ClickHouseResultSet("", "",
                ClickHouseSimpleResponse.of(new ClickHouseConfig(), ClickHouseColumn.parse("s String"),
                        new Object[][] { new Object[] { "a" }, new Object[] { "b" } })))) {
            Assert.assertEquals(rs.getFetchSize(), 0);
            rs.setFetchSize(2);
            Assert.assertEquals(rs.getFetchSize(), 0);
            rs.setFetchSize(-1);
            Assert.assertEquals(rs.getFetchSize(), 0);
        }
    }

    @Test(groups = "unit")
    public void testFirstAndLastRow() throws SQLException {
        ClickHouseConfig config = new ClickHouseConfig();
        List<ClickHouseColumn> columns = ClickHouseColumn.parse("s String");
        // no record
        try (CombinedResultSet rs = new CombinedResultSet(new ClickHouseResultSet("", "",
                ClickHouseSimpleResponse.of(config, columns, new Object[0][])))) {
            Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
            Assert.assertFalse(rs.isFirst(), "Should NOT be the first");
            Assert.assertTrue(rs.isAfterLast(), "Should be after the last");
            Assert.assertFalse(rs.isLast(), "Should NOT be the last");

            Assert.assertFalse(rs.next(), "Should have no row");
            Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
            Assert.assertFalse(rs.isFirst(), "Should NOT be the first");
            Assert.assertTrue(rs.isAfterLast(), "Should be after the last");
            Assert.assertFalse(rs.isLast(), "Should NOT be the last");
        }

        // no record(with two empty resultsets)
        try (CombinedResultSet rs = new CombinedResultSet(new ClickHouseResultSet("", "",
                ClickHouseSimpleResponse.of(config, columns, new Object[0][])),
                new ClickHouseResultSet("", "",
                        ClickHouseSimpleResponse.of(config, columns, new Object[0][])))) {
            Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
            Assert.assertFalse(rs.isFirst(), "Should NOT be the first");
            Assert.assertTrue(rs.isAfterLast(), "Should NOT be after the last");
            Assert.assertFalse(rs.isLast(), "Should NOT be the last");

            Assert.assertFalse(rs.next(), "Should have no row");
            Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
            Assert.assertFalse(rs.isFirst(), "Should NOT be the first");
            Assert.assertTrue(rs.isAfterLast(), "Should NOT be after the last");
            Assert.assertFalse(rs.isLast(), "Should NOT be the last");
        }

        // one record
        try (CombinedResultSet rs = new CombinedResultSet(new ClickHouseResultSet("", "",
                ClickHouseSimpleResponse.of(config, columns,
                        new Object[][] { new Object[] { "a" } })))) {
            Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
            Assert.assertFalse(rs.isFirst(), "Should NOT be the first");
            Assert.assertFalse(rs.isAfterLast(), "Should NOT be after the last");
            Assert.assertFalse(rs.isLast(), "Should NOT be the last");

            Assert.assertTrue(rs.next(), "Should have one row");
            Assert.assertFalse(rs.isBeforeFirst(), "Should NOT be before the first");
            Assert.assertTrue(rs.isFirst(), "Should be the first");
            Assert.assertFalse(rs.isAfterLast(), "Should NOT be after the last");
            Assert.assertTrue(rs.isLast(), "Should be the last");

            Assert.assertFalse(rs.next(), "Should have only one row");
            Assert.assertFalse(rs.isBeforeFirst(), "Should NOT be before the first");
            Assert.assertTrue(rs.isFirst(), "Should be the first");
            Assert.assertTrue(rs.isAfterLast(), "Should be after the last");
            Assert.assertFalse(rs.isLast(), "Should NOT be the last");
        }

        try (CombinedResultSet rs = new CombinedResultSet(new ClickHouseResultSet("", "",
                ClickHouseSimpleResponse.of(config, columns,
                        new Object[][] { new Object[] { "a" }, new Object[] { "b" } })))) {
            Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertTrue(rs.isFirst(), "Should be the first row");
            Assert.assertEquals(rs.getString(1), "a");
            Assert.assertTrue(rs.next(), "Should have two rows");
            Assert.assertEquals(rs.getString(1), "b");
            Assert.assertTrue(rs.isLast(), "Should be the last row");
            Assert.assertFalse(rs.next(), "Should have only two rows");
            Assert.assertTrue(rs.isAfterLast(), "Should be after the last row");
        }
    }

    @Test(groups = "unit")
    public void testNext() throws SQLException {
        ClickHouseConfig config = new ClickHouseConfig();
        List<ClickHouseColumn> columns = ClickHouseColumn.parse("s String");
        try (CombinedResultSet rs = new CombinedResultSet(new ClickHouseResultSet("", "",
                ClickHouseSimpleResponse.of(config, columns,
                        new Object[][] { new Object[] { "a" }, new Object[] { "b" } })))) {
            Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.isFirst(), "Should be the first row");
            Assert.assertEquals(rs.getString(1), "a");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "b");
            Assert.assertTrue(rs.isLast(), "Should be the last row");
            Assert.assertFalse(rs.next());
            Assert.assertTrue(rs.isAfterLast(), "Should be after the last row");
        }

        try (CombinedResultSet rs = new CombinedResultSet(new ClickHouseResultSet("", "",
                ClickHouseSimpleResponse.of(config, columns, new Object[][] { new Object[] { "a" } })),
                new ClickHouseResultSet("", "",
                        ClickHouseSimpleResponse.of(config, columns, new Object[][] { new Object[] { "b" } })))) {
            Assert.assertTrue(rs.isBeforeFirst(), "Should be before the first");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.isFirst(), "Should be the first row");
            Assert.assertEquals(rs.getString(1), "a");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "b");
            Assert.assertTrue(rs.isLast(), "Should be the last row");
            Assert.assertFalse(rs.next());
            Assert.assertTrue(rs.isAfterLast(), "Should be after the last row");
        }
    }
}
