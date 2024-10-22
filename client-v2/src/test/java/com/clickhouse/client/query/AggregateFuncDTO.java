package com.clickhouse.client.query;

import com.clickhouse.data.value.ClickHouseBitmap;

import java.util.Objects;
import java.util.Random;

public class AggregateFuncDTO {

    private ClickHouseBitmap groupBitmapUint32;
    private ClickHouseBitmap groupBitmapUint64;

    public AggregateFuncDTO() {
        Random random = new Random();
        this.groupBitmapUint32 = ClickHouseBitmap.wrap(random.ints(5, Integer.MAX_VALUE - 100, Integer.MAX_VALUE).toArray());
        this.groupBitmapUint64 = ClickHouseBitmap.wrap(random.longs(5, Long.MAX_VALUE - 100, Long.MAX_VALUE).toArray());
    }

    public ClickHouseBitmap getGroupBitmapUint32() {
        return groupBitmapUint32;
    }

    public void setGroupBitmapUint32(ClickHouseBitmap groupBitmapUint32) {
        this.groupBitmapUint32 = groupBitmapUint32;
    }

    public ClickHouseBitmap getGroupBitmapUint64() {
        return groupBitmapUint64;
    }

    public void setGroupBitmapUint64(ClickHouseBitmap groupBitmapUint64) {
        this.groupBitmapUint64 = groupBitmapUint64;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateFuncDTO that = (AggregateFuncDTO) o;
        return Objects.equals(groupBitmapUint32, that.groupBitmapUint32) && Objects.equals(groupBitmapUint64, that.groupBitmapUint64);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupBitmapUint32, groupBitmapUint64);
    }

    public static String generateTableCreateSQL(String tableName) {
        return "CREATE TABLE " + tableName + " (" +
                "groupBitmapUint32 AggregateFunction(groupBitmap, UInt32), " +
                "groupBitmapUint64 AggregateFunction(groupBitmap, UInt64) " +
                ") ENGINE = MergeTree() ORDER BY tuple()";
    }
}
