package ru.yandex.clickhouse.response.parser;

import java.io.IOException;
import java.sql.SQLException;
import java.util.TimeZone;

import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;
import ru.yandex.clickhouse.util.ClickHouseBitmap;

final class ClickHouseBitmapParser extends ClickHouseValueParser<ClickHouseBitmap> {

    private static ClickHouseBitmapParser instance;

    static ClickHouseBitmapParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseBitmapParser();
        }
        return instance;
    }

    private ClickHouseBitmapParser() {
        // prevent instantiation
    }

    @Override
    public ClickHouseBitmap parse(ByteFragment value, ClickHouseColumnInfo columnInfo, TimeZone resultTimeZone)
            throws SQLException {
        if (value.isNull()) {
            return null;
        }

        // https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionGroupBitmapData.h#L100
        ClickHouseBitmap rb = ClickHouseBitmap.wrap();

        // FIXME use DataInput/DataOutput for stream after switching to RowBinary
        byte[] bytes = value.unescape();
        if (bytes.length == 0) {
            return rb;
        }

        try {
            rb = ClickHouseBitmap.deserialize(bytes, columnInfo.getArrayBaseType());
        } catch (IOException e) {
            throw new SQLException("Failed to deserialize ClickHouseBitmap", e);
        }

        return rb;
    }

    @Override
    protected ClickHouseBitmap getDefaultValue() {
        return ClickHouseBitmap.wrap();
    }
}
