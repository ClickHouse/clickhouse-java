package com.clickhouse.data.value;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.clickhouse.data.BaseClickHouseValueTest;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseEnum;

public class ClickHouseEnumValueTest extends BaseClickHouseValueTest {
    enum EnumWithConstantBody {
        PLAIN,
        WITH_BODY {
            @Override
            public String toString() {
                return "with body";
            }
        },
        LAST
    }

    @DataProvider(name = "enumClassProvider")
    private Object[][] getEnumClasses() {
        String[] names = { "PLAIN", "WITH_BODY", "LAST" };
        int[] values = { 0, 1, 2 };
        return new Object[][] {
                { EnumWithConstantBody.class, names, values },
                { EnumWithConstantBody.WITH_BODY.getClass(), names, values },
                { null, new String[0], new int[0] },
                { String.class, new String[0], new int[0] } };
    }

    @Test(dataProvider = "enumClassProvider", groups = { "unit" })
    public void testEnumTypeOfClass(Class<? extends Enum> clazz, String[] names, int[] values) {
        ClickHouseEnum type = ClickHouseEnum.of(clazz);
        Assert.assertEquals(type.getNames(), names);
        Assert.assertEquals(type.getValues(), values);
    }

    @Test(groups = { "unit" })
    public void testOfEnumWithConstantBody() {
        ClickHouseEnumValue v = ClickHouseEnumValue.of(EnumWithConstantBody.WITH_BODY);
        Assert.assertEquals(v.asInteger(), 1);
        Assert.assertEquals(v.asString(), "WITH_BODY");
        Assert.assertEquals(v.asEnum(EnumWithConstantBody.class), EnumWithConstantBody.WITH_BODY);
        Assert.assertEquals(v.update(EnumWithConstantBody.LAST).asString(), "LAST");
    }

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
