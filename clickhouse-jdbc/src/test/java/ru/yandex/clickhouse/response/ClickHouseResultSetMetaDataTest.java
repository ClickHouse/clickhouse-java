package ru.yandex.clickhouse.response;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClickHouseResultSetMetaDataTest {


  @Test
  public void testIsNullable() throws SQLException {

    ClickHouseResultSet resultSet = mock(ClickHouseResultSet.class);
    ClickHouseColumnInfo[] types = new ClickHouseColumnInfo[] {
        ClickHouseColumnInfo.parse("DateTime", "column1"),
        ClickHouseColumnInfo.parse("Nullable(Float64)", "column2")
    };
    when(resultSet.getColumns()).thenReturn(Arrays.asList(types));
    ClickHouseResultSetMetaData resultSetMetaData = new ClickHouseResultSetMetaData(resultSet);
    Assert.assertEquals(resultSetMetaData.isNullable(1), ResultSetMetaData.columnNoNulls);
    Assert.assertEquals(resultSetMetaData.isNullable(2), ResultSetMetaData.columnNullable);
  }

    @Test
    public void testIsNullableColumnTypeName() throws SQLException {

        ClickHouseResultSet resultSet = mock(ClickHouseResultSet.class);
        when(resultSet.getColumns()).thenReturn(Collections.singletonList(
            ClickHouseColumnInfo.parse("Nullable(Float64)", "column1")));
        ClickHouseResultSetMetaData resultSetMetaData = new ClickHouseResultSetMetaData(resultSet);
        Assert.assertEquals(resultSetMetaData.getColumnTypeName(1), "Float64");
    }

    @Test
    public void testIsNullableSigned() throws SQLException {
        ClickHouseResultSet resultSet = mock(ClickHouseResultSet.class);
        ClickHouseColumnInfo[] types = new ClickHouseColumnInfo[]{
            ClickHouseColumnInfo.parse("Nullable(Float64)", "column1"),
            ClickHouseColumnInfo.parse("Nullable(UInt64)", "column2"),
            ClickHouseColumnInfo.parse("Nullable(UFantasy)", "column3")
        };
        when(resultSet.getColumns()).thenReturn(Arrays.asList(types));
        ClickHouseResultSetMetaData resultSetMetaData = new ClickHouseResultSetMetaData(
            resultSet);
        Assert.assertTrue(resultSetMetaData.isSigned(1));
        Assert.assertFalse(resultSetMetaData.isSigned(2));
        Assert.assertFalse(resultSetMetaData.isSigned(3));
    }

    @Test
    public void testDateTimeWithTimeZone() throws SQLException {
        ClickHouseResultSet resultSet = mock(ClickHouseResultSet.class);
        when(resultSet.getColumns()).thenReturn(Collections.singletonList(
            ClickHouseColumnInfo.parse("DateTime('W-SU')", "column1")));
        ClickHouseResultSetMetaData resultSetMetaData = new ClickHouseResultSetMetaData(
            resultSet);
        Assert.assertEquals(resultSetMetaData.getColumnTypeName(1), "DateTime('W-SU')");
        Assert.assertEquals(resultSetMetaData.getColumnType(1), Types.TIMESTAMP);
    }

}