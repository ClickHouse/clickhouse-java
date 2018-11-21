package ru.yandex.clickhouse.response;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClickHouseResultSetMetaDataTest {


  @Test
  public void testIsNullable() throws SQLException {

    ClickHouseResultSet resultSet = mock(ClickHouseResultSet.class);
    String[] types = new String[] { "DateTime" , "Nullable(Float64)"};
    when(resultSet.getTypes()).thenReturn(types);


    ClickHouseResultSetMetaData resultSetMetaData = new ClickHouseResultSetMetaData(resultSet);

    Assert.assertEquals(resultSetMetaData.isNullable(1), ResultSetMetaData.columnNoNulls);
    Assert.assertEquals(resultSetMetaData.isNullable(2), ResultSetMetaData.columnNullable);
  }

    @Test
    public void testIsNullableColumnTypeName() throws SQLException {
        ClickHouseResultSet resultSet = mock(ClickHouseResultSet.class);
        String[] types = new String[]{"Nullable(Float64)"};
        when(resultSet.getTypes()).thenReturn(types);
        ClickHouseResultSetMetaData resultSetMetaData = new ClickHouseResultSetMetaData(
            resultSet);
        Assert.assertEquals(resultSetMetaData.getColumnTypeName(1), "Float64");
    }

    @Test
    public void testIsNullableSigned() throws SQLException {
        ClickHouseResultSet resultSet = mock(ClickHouseResultSet.class);
        String[] types = new String[]{
            "Nullable(Float64)",
            "Nullable(UInt64)",
            "Nullable(UFantasy)"};
        when(resultSet.getTypes()).thenReturn(types);
        ClickHouseResultSetMetaData resultSetMetaData = new ClickHouseResultSetMetaData(
            resultSet);
        Assert.assertTrue(resultSetMetaData.isSigned(1));
        Assert.assertFalse(resultSetMetaData.isSigned(2));
        Assert.assertTrue(resultSetMetaData.isSigned(3));
    }

}