package ru.yandex.clickhouse.response;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

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

}