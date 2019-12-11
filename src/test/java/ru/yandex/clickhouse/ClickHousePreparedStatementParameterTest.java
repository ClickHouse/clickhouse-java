package ru.yandex.clickhouse;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class ClickHousePreparedStatementParameterTest {

    @Test
    public void testNullParam() {
        ClickHousePreparedStatementParameter p0 =
            ClickHousePreparedStatementParameter.nullParameter();
        assertEquals(p0.getRegularValue(), "null");
        assertEquals(p0.getBatchValue(), "\\N");

        ClickHousePreparedStatementParameter p1 =
            ClickHousePreparedStatementParameter.nullParameter();
        assertEquals(p1, p0);
        assertSame(p1, p0);
    }

    @Test
    public void testArrayAndCollectionParam() {
        ClickHousePreparedStatementParameter p0 =
                ClickHousePreparedStatementParameter.fromObject(Arrays.asList("A", "B", "C"), TimeZone.getDefault(), TimeZone.getDefault());
        ClickHousePreparedStatementParameter p1 =
                ClickHousePreparedStatementParameter.fromObject(new String[]{"A", "B", "C"}, TimeZone.getDefault(), TimeZone.getDefault());
        assertEquals(p0.getRegularValue(), p1.getRegularValue());
        assertEquals(p0.getBatchValue(), p1.getBatchValue());
    }

    @Test
    public void testBooleanParam() {
        assertEquals(ClickHousePreparedStatementParameter.fromObject(Boolean.TRUE,
            TimeZone.getDefault(), TimeZone.getDefault()).getRegularValue(), "1");
        assertEquals(ClickHousePreparedStatementParameter.fromObject(Boolean.FALSE,
            TimeZone.getDefault(), TimeZone.getDefault()).getRegularValue(), "0");
    }

    @Test
    public void testNumberParam() {
        assertEquals(ClickHousePreparedStatementParameter.fromObject(10,
            TimeZone.getDefault(), TimeZone.getDefault()).getRegularValue(), "10");
        assertEquals(ClickHousePreparedStatementParameter.fromObject(10.5,
            TimeZone.getDefault(), TimeZone.getDefault()).getRegularValue(), "10.5");
    }

    @Test
    public void testStringParam() {
        assertEquals(ClickHousePreparedStatementParameter.fromObject("someString",
            TimeZone.getDefault(), TimeZone.getDefault()).getRegularValue(), "'someString'");
    }
}
