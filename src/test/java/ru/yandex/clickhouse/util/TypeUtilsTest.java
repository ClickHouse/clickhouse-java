package ru.yandex.clickhouse.util;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static ru.yandex.clickhouse.util.TypeUtils.NULLABLE_NO;
import static ru.yandex.clickhouse.util.TypeUtils.NULLABLE_YES;

public class TypeUtilsTest {

  @Test
  public void testTypeIsNullable() throws Exception {
    assertEquals(NULLABLE_NO,TypeUtils.isTypeNull("DateTime"));
    assertEquals(NULLABLE_NO,TypeUtils.isTypeNull("Float64"));
    assertEquals(NULLABLE_YES,TypeUtils.isTypeNull("Nullable(Float64)"));
    assertEquals(NULLABLE_YES,TypeUtils.isTypeNull("Nullable(DateTime)"));
  }

}