package ru.yandex.clickhouse.util;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
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

  @Test
  public void testGetDecimalDigits() throws Exception {
      assertEquals(TypeUtils.getDecimalDigits("DateTime"), 0);
      assertEquals(TypeUtils.getDecimalDigits("Int32"), 0);
      assertEquals(TypeUtils.getDecimalDigits("Array(String)"), 0);
      assertEquals(TypeUtils.getDecimalDigits("Nullable(Int32)"), 0);
      assertEquals(TypeUtils.getDecimalDigits("Nullable(DateTime)"), 0);

      assertEquals(TypeUtils.getDecimalDigits("Float64"), 17);
      assertEquals(TypeUtils.getDecimalDigits("Nullable(Float64)"), 17);
      assertEquals(TypeUtils.getDecimalDigits("Float32"), 8);
      assertEquals(TypeUtils.getDecimalDigits("Nullable(Float32)"), 8);
  }

}