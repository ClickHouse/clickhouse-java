package ru.yandex.clickhouse;

import org.testng.annotations.Test;

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

}
