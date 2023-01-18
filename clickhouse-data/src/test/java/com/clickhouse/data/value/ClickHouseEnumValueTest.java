package com.clickhouse.data.value;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.data.BaseClickHouseValueTest;
import com.clickhouse.data.ClickHouseDataType;

public class ClickHouseEnumValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testCopy() {
        sameValue(ClickHouseEnumValue.ofNull(ClickHouseDataType.class),
                ClickHouseEnumValue.ofNull(ClickHouseDataType.class), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.String),
                ClickHouseEnumValue.of(ClickHouseDataType.String),
                3, 9, Object.class, ClickHouseDataType.class, Object.class, Object.class);
        ClickHouseEnumValue v = ClickHouseEnumValue.of(ClickHouseDataType.Array);
        sameValue(v, v.copy(), 3, 9, Object.class, ClickHouseDataType.class, Object.class, Object.class);
    }

    @Test(groups = { "unit" })
    public void testUpdate() {
        sameValue(ClickHouseEnumValue.ofNull(ClickHouseDataType.class),
                ClickHouseEnumValue.ofNull(ClickHouseDataType.class).update(ClickHouseDataType.Date32).set(true, 0), 3,
                9,
                Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.Date32),
                ClickHouseEnumValue.ofNull(ClickHouseDataType.class).update(ClickHouseDataType.Date32), 3, 9,
                Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.Date32),
                ClickHouseEnumValue.of(ClickHouseDataType.Array).update(ClickHouseDataType.Date32), 3, 9,
                Object.class,
                ClickHouseDataType.class, Object.class, Object.class);

        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.Bool),
                ClickHouseEnumValue.of(ClickHouseDataType.String).update(false), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.Bool),
                ClickHouseEnumValue.of(ClickHouseDataType.String).update(new boolean[] { false }), 3, 9,
                Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.Bool),
                ClickHouseEnumValue.of(ClickHouseDataType.String).update('\0'), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.Bool),
                ClickHouseEnumValue.of(ClickHouseDataType.String).update((byte) 0), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.Bool),
                ClickHouseEnumValue.of(ClickHouseDataType.String).update((short) 0), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.Bool),
                ClickHouseEnumValue.of(ClickHouseDataType.String).update(0), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.Bool),
                ClickHouseEnumValue.of(ClickHouseDataType.String).update(0L), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.Bool),
                ClickHouseEnumValue.of(ClickHouseDataType.String).update("Bool"), 3, 9,
                Object.class, ClickHouseDataType.class, Object.class, Object.class);
    }
}
