package ru.yandex.clickhouse;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;

import java.util.Map;

public class ClickHouseStruct implements Struct {

  private Object[] attributes;

  public ClickHouseStruct(Object[] attributes) {
    this.attributes = attributes;
  }

  @Override
  public String getSQLTypeName() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object[] getAttributes() throws SQLException {
    return attributes;
  }

  @Override
  public Object[] getAttributes(Map<String,Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

}
