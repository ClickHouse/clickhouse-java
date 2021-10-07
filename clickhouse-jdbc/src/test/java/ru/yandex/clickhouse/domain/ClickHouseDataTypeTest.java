package ru.yandex.clickhouse.domain;

import java.util.Locale;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ClickHouseDataTypeTest {
    @Test(groups = "unit", dataProvider = "clickHouseDataTypeStringsProvider", dataProviderClass = ClickHouseDataTypeTestDataProvider.class)
    public void testFromDataTypeStringSimpleTypes(String typeName, ClickHouseDataType result) {
        assertEquals(ClickHouseDataType.fromTypeString(typeName), result, typeName);
        assertEquals(ClickHouseDataType.fromTypeString(typeName.toUpperCase(Locale.ROOT)), result);
        assertEquals(ClickHouseDataType.fromTypeString(typeName.toLowerCase(Locale.ROOT)), result);
    }
}
