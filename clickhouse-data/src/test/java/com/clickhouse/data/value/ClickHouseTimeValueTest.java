package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalTime;
import java.util.TimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.data.BaseClickHouseValueTest;

public class ClickHouseTimeValueTest extends BaseClickHouseValueTest<LocalTime> {
    @Override
    protected ClickHouseTimeValue create(LocalTime value) {
        return ClickHouseTimeValue.of(value, TimeZone.getDefault());
    }

    @Override
    protected ClickHouseTimeValue createNull() {
        return ClickHouseTimeValue.ofNull(TimeZone.getDefault());
    }

    @Override
    protected LocalTime[] getTestValues() {
        return new LocalTime[] {
            LocalTime.ofSecondOfDay(0L), // 00:00:00
            LocalTime.ofSecondOfDay(3600L), // 01:00:00
            LocalTime.ofSecondOfDay(3661L), // 01:01:01
            LocalTime.ofSecondOfDay(86399L), // 23:59:59
            LocalTime.of(12, 34, 56), // 12:34:56
            LocalTime.of(23, 59, 59), // 23:59:59
        };
    }

    @Test(groups = { "unit" })
    public void testCreate() {
        ClickHouseTimeValue value = create(LocalTime.of(12, 34, 56));
        Assert.assertEquals(value.asTime(), LocalTime.of(12, 34, 56));
        Assert.assertEquals(value.asString(), "12:34:56");
    }

    @Test(groups = { "unit" })
    public void testCreateFromString() {
        ClickHouseTimeValue value = ClickHouseTimeValue.of("12:34:56", TimeZone.getDefault());
        Assert.assertEquals(value.asTime(), LocalTime.of(12, 34, 56));
        Assert.assertEquals(value.asString(), "12:34:56");
    }

    @Test(groups = { "unit" })
    public void testCreateNull() {
        ClickHouseTimeValue value = createNull();
        Assert.assertNull(value.asTime());
        Assert.assertNull(value.asString());
    }

    @Test(groups = { "unit" })
    public void testUpdateFromNumber() {
        ClickHouseTimeValue value = create(LocalTime.of(12, 34, 56));
        
        // Update from seconds since midnight
        value.update(3661); // 01:01:01
        Assert.assertEquals(value.asTime(), LocalTime.of(1, 1, 1));
        
        value.update(3600L); // 01:00:00
        Assert.assertEquals(value.asTime(), LocalTime.of(1, 0, 0));
    }

    @Test(groups = { "unit" })
    public void testUpdateFromString() {
        ClickHouseTimeValue value = create(LocalTime.of(12, 34, 56));
        value.update("23:59:59");
        Assert.assertEquals(value.asTime(), LocalTime.of(23, 59, 59));
    }

    @Test(groups = { "unit" })
    public void testUpdateFromLocalTime() {
        ClickHouseTimeValue value = create(LocalTime.of(12, 34, 56));
        value.update(LocalTime.of(23, 59, 59));
        Assert.assertEquals(value.asTime(), LocalTime.of(23, 59, 59));
    }

    @Test(groups = { "unit" })
    public void testAsNumber() {
        ClickHouseTimeValue value = create(LocalTime.of(1, 1, 1)); // 3661 seconds
        
        Assert.assertEquals(value.asInteger(), 3661);
        Assert.assertEquals(value.asLong(), 3661L);
        Assert.assertEquals(value.asShort(), (short) 3661);
        Assert.assertEquals(value.asByte(), (byte) 3661);
        Assert.assertEquals(value.asFloat(), 3661.0f);
        Assert.assertEquals(value.asDouble(), 3661.0);
    }

    @Test(groups = { "unit" })
    public void testAsBigDecimal() {
        ClickHouseTimeValue value = create(LocalTime.of(1, 1, 1)); // 3661 seconds
        BigDecimal bd = value.asBigDecimal(0);
        Assert.assertEquals(bd, new BigDecimal("3661"));
    }

    @Test(groups = { "unit" })
    public void testAsBigInteger() {
        ClickHouseTimeValue value = create(LocalTime.of(1, 1, 1)); // 3661 seconds
        BigInteger bi = value.asBigInteger();
        Assert.assertEquals(bi, BigInteger.valueOf(3661));
    }

    @Test(groups = { "unit" })
    public void testToSqlExpression() {
        ClickHouseTimeValue value = create(LocalTime.of(12, 34, 56));
        Assert.assertEquals(value.toSqlExpression(), "'12:34:56'");
        
        ClickHouseTimeValue nullValue = createNull();
        Assert.assertEquals(nullValue.toSqlExpression(), "NULL");
    }

    @Test(groups = { "unit" })
    public void testCopy() {
        ClickHouseTimeValue original = create(LocalTime.of(12, 34, 56));
        ClickHouseTimeValue copy = original.copy(false);
        
        Assert.assertEquals(copy.asTime(), original.asTime());
        Assert.assertNotSame(copy, original);
    }

    @Test(groups = { "unit" })
    public void testResetToDefault() {
        ClickHouseTimeValue value = create(LocalTime.of(12, 34, 56));
        value.resetToDefault();
        Assert.assertEquals(value.asTime(), LocalTime.ofSecondOfDay(0L));
    }
} 