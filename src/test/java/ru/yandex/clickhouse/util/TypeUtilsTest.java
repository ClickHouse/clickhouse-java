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

      assertEquals(TypeUtils.getDecimalDigits("Decimal(12, 3)"), 3);
      assertEquals(TypeUtils.getDecimalDigits("Decimal(12,3)"), 3);
      assertEquals(TypeUtils.getDecimalDigits("Nullable(Decimal(12,42))"), 42);
      assertEquals(TypeUtils.getDecimalDigits("Decimal(12,"), 0);
      assertEquals(TypeUtils.getDecimalDigits("Decimal(12, 0)"), 0);
  }

  @Test
  public void testGetColumnSize() throws Exception {
      assertEquals(TypeUtils.getColumnSize("DateTime"), 19);
      assertEquals(TypeUtils.getColumnSize("Date"), 10);
      assertEquals(TypeUtils.getColumnSize("UInt8"), 3);
      assertEquals(TypeUtils.getColumnSize("Int32"), 11);
      assertEquals(TypeUtils.getColumnSize("Float32"), 8);
      assertEquals(TypeUtils.getColumnSize("String"), 0);
      assertEquals(TypeUtils.getColumnSize("FixedString(12)"), 12);
      assertEquals(TypeUtils.getColumnSize("Enum8"), 0);
      assertEquals(TypeUtils.getColumnSize("Array(String)"), 0);

      assertEquals(TypeUtils.getColumnSize("Nullable(Int32)"), 11);
      assertEquals(TypeUtils.getColumnSize("Nullable(DateTime)"), 19);
      assertEquals(TypeUtils.getColumnSize("Nullable(FixedString(4))"), 4);
  }

  @Test
  public void testUnwrapNullableIfApplicable() throws Exception {
      assertEquals(TypeUtils.unwrapNullableIfApplicable("UInt32"), "UInt32");
      assertEquals(TypeUtils.unwrapNullableIfApplicable("Nullable(UInt32)"), "UInt32");
      assertEquals(TypeUtils.unwrapNullableIfApplicable("Nullable(foo)"), "foo");
      assertEquals(TypeUtils.unwrapNullableIfApplicable("Nullable(UInt32"), "Nullable(UInt32");
  }
}