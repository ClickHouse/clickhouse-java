package com.clickhouse.client;


import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"integration"})
public class QueryAllReaderWriterTests extends BaseReaderWriterTests<GenericRecord> {

    @Override
    protected Map<ClickHouseDataType, BiFunction<GenericRecord, String, String>> getTypeReaders() {
        Map<ClickHouseDataType, BiFunction<GenericRecord, String, String>> readers =
                new EnumMap<>(ClickHouseDataType.class);

        readers.put(ClickHouseDataType.Int8, (r, c) -> String.valueOf(r.getByte(c)));
        readers.put(ClickHouseDataType.Int16, (r, c) -> String.valueOf(r.getShort(c)));
        readers.put(ClickHouseDataType.Int32, (r, c) -> String.valueOf(r.getInteger(c)));
        readers.put(ClickHouseDataType.Int64, (r, c) -> String.valueOf(r.getLong(c)));
        readers.put(ClickHouseDataType.Int128, (r, c) -> r.getBigInteger(c).toString());
        readers.put(ClickHouseDataType.Int256, (r, c) -> r.getBigInteger(c).toString());

        readers.put(ClickHouseDataType.UInt8, (r, c) -> String.valueOf(r.getShort(c)));
        readers.put(ClickHouseDataType.UInt16, (r, c) -> String.valueOf(r.getInteger(c)));
        readers.put(ClickHouseDataType.UInt32, (r, c) -> String.valueOf(r.getLong(c)));
        readers.put(ClickHouseDataType.UInt64, (r, c) -> r.getBigInteger(c).toString());
        readers.put(ClickHouseDataType.UInt128, (r, c) -> r.getBigInteger(c).toString());
        readers.put(ClickHouseDataType.UInt256, (r, c) -> r.getBigInteger(c).toString());

        readers.put(ClickHouseDataType.Float32, (r, c) -> String.valueOf(r.getFloat(c)));
        readers.put(ClickHouseDataType.Float64, (r, c) -> String.valueOf(r.getDouble(c)));

        readers.put(ClickHouseDataType.Bool, (r, c) -> String.valueOf(r.getBoolean(c)));

        readers.put(ClickHouseDataType.Date, (r, c) -> r.getLocalDate(c).toString());
        readers.put(ClickHouseDataType.Date32, (r, c) -> r.getLocalDate(c).toString());
        readers.put(ClickHouseDataType.DateTime, (r, c) -> r.getLocalDateTime(c).toString());
        readers.put(ClickHouseDataType.DateTime32, (r, c) -> r.getLocalDateTime(c).toString());
        readers.put(ClickHouseDataType.DateTime64, (r, c) -> r.getLocalDateTime(c).toString());

        readers.put(ClickHouseDataType.Decimal, (r, c) -> r.getBigDecimal(c).toPlainString());
        readers.put(ClickHouseDataType.Decimal32, (r, c) -> r.getBigDecimal(c).toPlainString());
        readers.put(ClickHouseDataType.Decimal64, (r, c) -> r.getBigDecimal(c).toPlainString());
        readers.put(ClickHouseDataType.Decimal128, (r, c) -> r.getBigDecimal(c).toPlainString());
        readers.put(ClickHouseDataType.Decimal256, (r, c) -> r.getBigDecimal(c).toPlainString());

        readers.put(ClickHouseDataType.IPv4, (r, c) -> r.getInet4Address(c).getHostAddress());
        readers.put(ClickHouseDataType.IPv6, (r, c) -> r.getInet6Address(c).getHostAddress());

        readers.put(ClickHouseDataType.UUID, (r, c) -> r.getUUID(c).toString());

        readers.put(ClickHouseDataType.Enum8, (r, c) -> String.valueOf(r.getEnum8(c)));
        readers.put(ClickHouseDataType.Enum16, (r, c) -> String.valueOf(r.getEnum16(c)));

        return readers;
    }

    @Override
    protected void readAndVerifyRecords(String query, DatasetBuilder.DatasetSnapshot snapshot,
                                        List<ClickHouseColumn> columns) throws Exception {
        Map<ClickHouseDataType, BiFunction<GenericRecord, String, String>> typeReaders = getTypeReaders();
        List<List<String>> expectedValues = snapshot.getValues();

        List<GenericRecord> records = getSharedClient().queryAll(query);

        Assert.assertEquals(records.size(), expectedValues.size(),
                "Row count mismatch: expected " + expectedValues.size() + " but got " + records.size());

        for (int rowIndex = 0; rowIndex < records.size(); rowIndex++) {
            verifyRecord(records.get(rowIndex), expectedValues.get(rowIndex), columns, rowIndex, typeReaders);
        }
    }
}
