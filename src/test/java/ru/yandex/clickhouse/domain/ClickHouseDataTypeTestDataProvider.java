package ru.yandex.clickhouse.domain;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.DataProvider;

public class ClickHouseDataTypeTestDataProvider {

    private static ClickHouseDataTypeTestData[] testData;

    @DataProvider(name = "clickHouseDataTypeStringsProvider")
    public static Object[][] provideSimpleDataTypes() {
        Object[][] myData = new Object[getTestData().length][2];
        for (int i = 0; i < getTestData().length; i++) {
            myData[i][0] = getTestData()[i].simpleTypeName;
            myData[i][1] = getTestData()[i].clickHouseDataType;
        }
        return myData;
    }

    public static List<ClickHouseDataTypeTestData> provideDataTypes() {
        List<ClickHouseDataTypeTestData> filtered = new ArrayList<ClickHouseDataTypeTestData>();
        for (int i = 0; i < getTestData().length; i++) {
            ClickHouseDataTypeTestData d = getTestData()[i];
            if (d.isCheckValue()) {
                filtered.add(d);
            }
        }
        return filtered;
    }

    private static ClickHouseDataTypeTestData[] getTestData() {
        if (testData == null) {
            testData = initTestData();
        }
        return testData;
    }

    private static ClickHouseDataTypeTestData[] initTestData() {
        return new ClickHouseDataTypeTestData[] {
            create("IntervalYear", ClickHouseDataType.IntervalYear, "IntervalYear", "42", true, true),
            create("IntervalQuarter", ClickHouseDataType.IntervalQuarter, "IntervalQuarter", "42", true, true),
            create("IntervalDay", ClickHouseDataType.IntervalDay, "IntervalDay", "42", true, true),
            create("IntervalWeek", ClickHouseDataType.IntervalWeek, "IntervalWeek", "42", true, true),
            create("IntervalHour", ClickHouseDataType.IntervalHour, "IntervalHour", "42", true, true),
            create("IntervalMinute", ClickHouseDataType.IntervalMinute, "IntervalMinute", "42", true, true),
            create("Nested", ClickHouseDataType.Nested),
            create("IntervalMonth", ClickHouseDataType.IntervalMonth, "IntervalMonth", "42", true, true),
            create("Tuple", ClickHouseDataType.Tuple, "Tuple(String, UInt32)", "('foo', 42)", true, true),
            create("AggregateFunction", ClickHouseDataType.AggregateFunction),
            create("FixedString", ClickHouseDataType.FixedString, "FixedString(6)", "FOOBAR", true, true),
            create("IntervalSecond", ClickHouseDataType.IntervalSecond, "IntervalSecond", "42", true, true),
            create("UInt64", ClickHouseDataType.UInt64, "UInt64", "42", true, true),
            create("Enum8", ClickHouseDataType.Enum8, "Enum8(1 = 'foo', 2 = 'bar')", "foo", true, true),
            create("Int32", ClickHouseDataType.Int32, "Int32", "-23", true, true),
            create("Int16", ClickHouseDataType.Int16, "Int16", "-23", true, true),
            create("Int8", ClickHouseDataType.Int8, "Int8", "-42", true, true),
            create("Date", ClickHouseDataType.Date, "Date", "2019-05-02", true, true),
            create("UInt32", ClickHouseDataType.UInt32, "UInt32", "42", true, true),
            create("UInt8", ClickHouseDataType.UInt8, "UInt8", "23", true, true),
            create("Enum16", ClickHouseDataType.Enum16, "Enum16(1 = 'foo', 2 = 'bar')", "foo", true, false),
            create("DateTime", ClickHouseDataType.DateTime, "DateTime", "2019-05-02 13:37:00", true, true),
            create("UInt16", ClickHouseDataType.UInt16, "UInt16", "42", true, true),
            create("Nothing", ClickHouseDataType.Nothing),
            create("Array", ClickHouseDataType.Array),
            create("Int64", ClickHouseDataType.Int64, "Int64", "-42", true, true),
            create("Float32", ClickHouseDataType.Float32, "Float32", "0.42", true, false),
            create("Float64", ClickHouseDataType.Float64, "Float64", "-0.23", true, false),
            create("Decimal32", ClickHouseDataType.Decimal32, "Decimal32(4)", "0.4242", true, false),
            create("Decimal64", ClickHouseDataType.Decimal64, "Decimal64(4)", "1337.23", true, false),
            create("Decimal128", ClickHouseDataType.Decimal128, "Decimal128(4)", "1337.23", true, false),
            create("UUID", ClickHouseDataType.UUID, "UUID", "61f0c404-5cb3-11e7-907b-a6006ad3dba0", true, false),
            create("String", ClickHouseDataType.String, "String", "foo", true, true),
            create("Decimal", ClickHouseDataType.Decimal, "Decimal(12,3)", "23.420", true, true),
            create("LONGBLOB", ClickHouseDataType.String),
            create("MEDIUMBLOB", ClickHouseDataType.String),
            create("TINYBLOB", ClickHouseDataType.String),
            create("BIGINT", ClickHouseDataType.Int64),
            create("SMALLINT", ClickHouseDataType.Int16),
            create("TIMESTAMP", ClickHouseDataType.DateTime),
            create("INTEGER", ClickHouseDataType.Int32),
            create("INT", ClickHouseDataType.Int32),
            create("DOUBLE", ClickHouseDataType.Float64),
            create("MEDIUMTEXT", ClickHouseDataType.String),
            create("TINYINT", ClickHouseDataType.Int8),
            create("DEC", ClickHouseDataType.Decimal),
            create("BINARY", ClickHouseDataType.FixedString),
            create("FLOAT", ClickHouseDataType.Float32),
            create("CHAR", ClickHouseDataType.String),
            create("VARCHAR", ClickHouseDataType.String),
            create("TEXT", ClickHouseDataType.String),
            create("TINYTEXT", ClickHouseDataType.String),
            create("LONGTEXT", ClickHouseDataType.String),
            create("BLOB", ClickHouseDataType.String),
            create("FANTASY", ClickHouseDataType.Unknown, "Fantasy", "[42, 23]", true, true)
        };
    }

    private static ClickHouseDataTypeTestData create(String simpleTypeName,
        ClickHouseDataType clickHouseDataType, String typeName,
        String testValue, boolean nullableCandidate,
        boolean lowCardinalityCandidate)
    {
        return new ClickHouseDataTypeTestData(simpleTypeName, clickHouseDataType,
            typeName, testValue, nullableCandidate, lowCardinalityCandidate);
    }

    private static ClickHouseDataTypeTestData create(String simpleTypeName,
        ClickHouseDataType clickHouseDataType)
    {
        return new ClickHouseDataTypeTestData(simpleTypeName, clickHouseDataType,
            null, null, false, false);
    }

    public static final class ClickHouseDataTypeTestData {

        private final String simpleTypeName;
        private final ClickHouseDataType clickHouseDataType;
        private final String typeName;
        private final String testValue;
        private final boolean nullableCandidate;
        private final boolean lowCardinalityCandidate;

        ClickHouseDataTypeTestData(String simpleTypeName,
            ClickHouseDataType clickHouseDataType, String typeName,
            String testValue, boolean nullableCandidate,
            boolean lowCardinalityCandidate)
        {
            this.simpleTypeName = simpleTypeName;
            this.clickHouseDataType = clickHouseDataType;
            this.typeName = typeName;
            this.testValue = testValue;
            this.nullableCandidate = nullableCandidate;
            this.lowCardinalityCandidate = lowCardinalityCandidate;
        }

        private boolean isCheckValue() {
            return typeName != null;
        }

        public String getTypeName() {
            return typeName;
        }

        public boolean isNullableCandidate() {
            return nullableCandidate;
        }

        public boolean isLowCardinalityCandidate() {
            return lowCardinalityCandidate;
        }

        public String getTestValue() {
            return testValue;
        }

    }

}
