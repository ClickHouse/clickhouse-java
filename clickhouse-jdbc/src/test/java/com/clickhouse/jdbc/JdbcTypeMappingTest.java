package com.clickhouse.jdbc;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;

import static org.testng.Assert.assertEquals;

public class JdbcTypeMappingTest {

    private final JdbcTypeMapping mapping = JdbcTypeMapping.getDefaultMapping();

    @Test(groups = {"unit"})
    public void testInt128Mapping() {
        ClickHouseColumn col = ClickHouseColumn.of("col", ClickHouseDataType.Int128, false, false);
        assertEquals(mapping.toSqlType(col, null), Types.NUMERIC);
        assertEquals(mapping.toJavaClass(col, null), BigInteger.class);
    }

    @Test(groups = {"unit"})
    public void testInt256Mapping() {
        ClickHouseColumn col = ClickHouseColumn.of("col", ClickHouseDataType.Int256, false, false);
        assertEquals(mapping.toSqlType(col, null), Types.NUMERIC);
        assertEquals(mapping.toJavaClass(col, null), BigInteger.class);
    }

    @Test(groups = {"unit"})
    public void testUInt64Mapping() {
        ClickHouseColumn col = ClickHouseColumn.of("col", ClickHouseDataType.UInt64, false, false);
        assertEquals(mapping.toSqlType(col, null), Types.NUMERIC);
        assertEquals(mapping.toJavaClass(col, null), BigInteger.class);
    }

    @Test(groups = {"unit"})
    public void testUInt128Mapping() {
        ClickHouseColumn col = ClickHouseColumn.of("col", ClickHouseDataType.UInt128, false, false);
        assertEquals(mapping.toSqlType(col, null), Types.NUMERIC);
        assertEquals(mapping.toJavaClass(col, null), BigInteger.class);
    }

    @Test(groups = {"unit"})
    public void testUInt256Mapping() {
        ClickHouseColumn col = ClickHouseColumn.of("col", ClickHouseDataType.UInt256, false, false);
        assertEquals(mapping.toSqlType(col, null), Types.NUMERIC);
        assertEquals(mapping.toJavaClass(col, null), BigInteger.class);
    }

    @Test(groups = {"unit"})
    public void testBigIntegerToSqlType() {
        // Test that BigInteger class is mapped to Types.NUMERIC (not DECIMAL)
        assertEquals(mapping.getSqlType(BigInteger.class), Types.NUMERIC);
    }

    @Test(groups = {"unit"})
    public void testBigDecimalToSqlType() {
        // Test that BigDecimal class is still mapped to Types.DECIMAL
        assertEquals(mapping.getSqlType(BigDecimal.class), Types.DECIMAL);
    }
}
 