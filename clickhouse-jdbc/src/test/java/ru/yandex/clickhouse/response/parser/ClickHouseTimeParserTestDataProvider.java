package ru.yandex.clickhouse.response.parser;

import org.testng.annotations.DataProvider;

import ru.yandex.clickhouse.domain.ClickHouseDataType;

final class ClickHouseTimeParserTestDataProvider {

    static final String OTHER_DATA_TYPES = "otherDataTypes";

    @DataProvider(name = OTHER_DATA_TYPES)
    static Object[][] provideNumberAndSimilarClickHouseDataTypes() {
        return new ClickHouseDataType[][] {
            {ClickHouseDataType.Int32},
            {ClickHouseDataType.Int64},
            {ClickHouseDataType.UInt32},
            {ClickHouseDataType.UInt64},
            {ClickHouseDataType.String},
            {ClickHouseDataType.Unknown}
        };
    }

}
