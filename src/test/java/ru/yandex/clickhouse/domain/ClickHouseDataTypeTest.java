package ru.yandex.clickhouse.domain;

import java.util.Locale;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ClickHouseDataTypeTest {

    @Test(
        dataProvider = "clickHouseDataTypeStringsProvider",
        dataProviderClass = ClickHouseDataTypeTestDataProvider.class
    )
    public void testFromDataTypeStringSimpleTypes(String typeName, ClickHouseDataType result) {
        assertEquals(
            ClickHouseDataType.fromTypeString(typeName),
            result,
            typeName);
        assertEquals(
            ClickHouseDataType.fromTypeString(typeName.toUpperCase(Locale.ENGLISH)),
            result);
        assertEquals(
            ClickHouseDataType.fromTypeString(typeName.toLowerCase(Locale.ENGLISH)),
            result);
    }

}
