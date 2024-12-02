package com.clickhouse.jdbc.metadata;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.JdbcIntegrationTest;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ParameterMetaDataTest extends JdbcIntegrationTest {
    @Test(groups = { "integration" })
    public void testGetParameterCount() throws SQLException {
        ParameterMetaData metaData = new ParameterMetaData(Collections.emptyList());
        assertEquals(metaData.getParameterCount(), 0);

        metaData = new ParameterMetaData(List.of(ClickHouseColumn.of("param1", ClickHouseDataType.Int32, false)));
        assertEquals(metaData.getParameterCount(), 1);
    }

    @Test(groups = { "integration" })
    public void testIsNullable() throws SQLException {
        ClickHouseColumn column = ClickHouseColumn.of("param1", ClickHouseDataType.Int32, true);
        ParameterMetaData metaData = new ParameterMetaData(Collections.singletonList(column));
        assertEquals(metaData.isNullable(1), ParameterMetaData.parameterNullable);
    }

    @Test(groups = { "integration" })
    public void testIsSigned() throws SQLException {
        ClickHouseColumn column = ClickHouseColumn.of("param1", ClickHouseDataType.Int32, false);
        ParameterMetaData metaData = new ParameterMetaData(Collections.singletonList(column));
        assertTrue(metaData.isSigned(1));

        column = ClickHouseColumn.of("param2", ClickHouseDataType.UInt32, false);
        metaData = new ParameterMetaData(Collections.singletonList(column));
        assertFalse(metaData.isSigned(1));
    }

    @Test(groups = { "integration" })
    public void testGetPrecisionAndScale() throws SQLException {
        ClickHouseColumn column = ClickHouseColumn.of("param1", ClickHouseDataType.Int32, false, 10, 5);
        ParameterMetaData metaData = new ParameterMetaData(Collections.singletonList(column));
        assertEquals(metaData.getPrecision(1), 10);
        assertEquals(metaData.getScale(1), 5);
    }

    @Test(groups = { "integration" })
    public void testGetParameterType() throws SQLException {
        ClickHouseColumn column = ClickHouseColumn.of("param1", ClickHouseDataType.Int32, false);
        ParameterMetaData metaData = new ParameterMetaData(Collections.singletonList(column));
        assertEquals(metaData.getParameterType(1), java.sql.Types.INTEGER);
    }

    @Test(groups = { "integration" })
    public void testGetParameterTypeName() throws SQLException {
        ClickHouseColumn column = ClickHouseColumn.of("param1", ClickHouseDataType.Int32, false);
        ParameterMetaData metaData = new ParameterMetaData(Collections.singletonList(column));
        assertEquals(metaData.getParameterTypeName(1), "Int32");
    }

    @Test(groups = { "integration" })
    public void testGetParameterClassName() throws SQLException {
        ClickHouseColumn column = ClickHouseColumn.of("param1", ClickHouseDataType.Int32, false);
        ParameterMetaData metaData = new ParameterMetaData(Collections.singletonList(column));
        assertEquals(metaData.getParameterClassName(1), "java.lang.Integer");
    }

    @Test(groups = { "integration" })
    public void testGetParameterMode() throws SQLException {
        ClickHouseColumn column = ClickHouseColumn.of("param1", ClickHouseDataType.Int32, false);
        ParameterMetaData metaData = new ParameterMetaData(Collections.singletonList(column));
        assertEquals(metaData.getParameterMode(1), ParameterMetaData.parameterModeIn);
    }
}
