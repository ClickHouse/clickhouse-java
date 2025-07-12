package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalTime;
import java.util.TimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.data.BaseClickHouseValueTest;

public class ClickHouseTime64ValueTest extends BaseClickHouseValueTest<LocalTime> {
    @Override
    protected ClickHouseTime64Value create(LocalTime value) {
        return ClickHouseTime64Value.of(value, 3, TimeZone.getDefault());
    }

    @Override
    protected ClickHouseTime64Value createNull() {
        return ClickHouseTime64Value.ofNull(3, TimeZone.getDefault());
    }

    @Override
    protected LocalTime[] getTestValues() {
        return new LocalTime[] {
            LocalTime.ofNanoOfDay(0L), // 00:00:00.000000000
            LocalTime.ofNanoOfDay(3600_000_000_000L), // 01:00:00.000000000
            LocalTime.ofNanoOfDay(3661_123_000_000L), // 01:01:01.123000000
            LocalTime.ofNanoOfDay(86399_999_999_999L), // 23:59:59.999999999
            LocalTime.of(12, 34, 56, 123_456_789), // 12:34:56.123456789
            LocalTime.of(23, 59, 59, 999_999_999), // 23:59:59.999999999
        };
    }

    @Test(groups = { "unit" })
    public void testCreate() {
        ClickHouseTime64Value value = create(LocalTime.of(12, 34, 56, 123_456_789));
        Assert.assertEquals(value.asTime(), LocalTime.of(12, 34, 56, 123_456_789));
        Assert.assertEquals(value.asString(), "12:34:56.123456789");
    }

    @Test(groups = { "unit" })
    public void testCreateFromString() {
        ClickHouseTime64Value value = ClickHouseTime64Value.of("12:34:56.123456789", 3, TimeZone.getDefault());
        Assert.assertEquals(value.asTime(), LocalTime.of(12, 34, 56, 123_456_789));
        Assert.assertEquals(value.asString(), "12:34:56.123456789");
    }

    @Test(groups = { "unit" })
    public void testCreateNull() {
        ClickHouseTime64Value value = createNull();
        Assert.assertNull(value.asTime());
        Assert.assertNull(value.asString());
    }

    @Test(groups = { "unit" })
    public void testUpdateFromNumber() {
        ClickHouseTime64Value value = create(LocalTime.of(12, 34, 56, 123_456_789));
        
        // Update from nanoseconds since midnight
        value.update(3661_123_000_000L); // 01:01:01.123000000
        Assert.assertEquals(value.asTime(), LocalTime.of(1, 1, 1, 123_000_000));
        
        value.update(3600_000_000_000L); // 01:00:00.000000000
        Assert.assertEquals(value.asTime(), LocalTime.of(1, 0, 0));
    }

    @Test(groups = { "unit" })
    public void testUpdateFromString() {
        ClickHouseTime64Value value = create(LocalTime.of(12, 34, 56, 123_456_789));
        value.update("23:59:59.999999999");
        Assert.assertEquals(value.asTime(), LocalTime.of(23, 59, 59, 999_999_999));
    }

    @Test(groups = { "unit" })
    public void testUpdateFromLocalTime() {
        ClickHouseTime64Value value = create(LocalTime.of(12, 34, 56, 123_456_789));
        value.update(LocalTime.of(23, 59, 59, 999_999_999));
        Assert.assertEquals(value.asTime(), LocalTime.of(23, 59, 59, 999_999_999));
    }

    @Test(groups = { "unit" })
    public void testAsNumber() {
        ClickHouseTime64Value value = create(LocalTime.of(1, 1, 1, 123_000_000)); // 3661_123_000_000 nanoseconds
        
        Assert.assertEquals(value.asLong(), 3661_123_000_000L);
        Assert.assertEquals(value.asInteger(), (int) 3661_123_000_000L);
        Assert.assertEquals(value.asShort(), (short) 3661_123_000_000L);
        Assert.assertEquals(value.asByte(), (byte) 3661_123_000_000L);
        Assert.assertEquals(value.asFloat(), 3661_123_000_000.0f);
        Assert.assertEquals(value.asDouble(), 3661_123_000_000.0);
    }

    @Test(groups = { "unit" })
    public void testAsBigDecimal() {
        ClickHouseTime64Value value = create(LocalTime.of(1, 1, 1, 123_000_000)); // 3661_123_000_000 nanoseconds
        BigDecimal bd = value.asBigDecimal(0);
        Assert.assertEquals(bd, new BigDecimal("3661123000000"));
    }

    @Test(groups = { "unit" })
    public void testAsBigInteger() {
        ClickHouseTime64Value value = create(LocalTime.of(1, 1, 1, 123_000_000)); // 3661_123_000_000 nanoseconds
        BigInteger bi = value.asBigInteger();
        Assert.assertEquals(bi, BigInteger.valueOf(3661_123_000_000L));
    }

    @Test(groups = { "unit" })
    public void testToSqlExpression() {
        ClickHouseTime64Value value = create(LocalTime.of(12, 34, 56, 123_456_789));
        Assert.assertEquals(value.toSqlExpression(), "'12:34:56.123456789'");
        
        ClickHouseTime64Value nullValue = createNull();
        Assert.assertEquals(nullValue.toSqlExpression(), "NULL");
    }

    @Test(groups = { "unit" })
    public void testCopy() {
        ClickHouseTime64Value original = create(LocalTime.of(12, 34, 56, 123_456_789));
        ClickHouseTime64Value copy = original.copy(false);
        
        Assert.assertEquals(copy.asTime(), original.asTime());
        Assert.assertEquals(copy.getScale(), original.getScale());
        Assert.assertNotSame(copy, original);
    }

    @Test(groups = { "unit" })
    public void testResetToDefault() {
        ClickHouseTime64Value value = create(LocalTime.of(12, 34, 56, 123_456_789));
        value.resetToDefault();
        Assert.assertEquals(value.asTime(), LocalTime.ofNanoOfDay(0L));
    }

    @Test(groups = { "unit" })
    public void testGetScale() {
        ClickHouseTime64Value value = ClickHouseTime64Value.of(LocalTime.of(12, 34, 56), 5, TimeZone.getDefault());
        Assert.assertEquals(value.getScale(), 5);
    }
} 