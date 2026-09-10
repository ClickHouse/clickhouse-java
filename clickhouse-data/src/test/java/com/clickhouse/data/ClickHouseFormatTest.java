package com.clickhouse.data;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseFormatTest {

    @Test(groups = { "unit" })
    public void testFromStringNullAndEmpty() {
        Assert.assertNull(ClickHouseFormat.fromString(null));
        Assert.assertNull(ClickHouseFormat.fromString(""));
        Assert.assertNull(ClickHouseFormat.fromString("   "));
    }

    @Test(groups = { "unit" })
    public void testFromStringValid() {
        Assert.assertEquals(ClickHouseFormat.fromString("CSV"), ClickHouseFormat.CSV);
        Assert.assertEquals(ClickHouseFormat.fromString("csv"), ClickHouseFormat.CSV);
        Assert.assertEquals(ClickHouseFormat.fromString("  jsoneachrow  "), ClickHouseFormat.JSONEachRow);
        Assert.assertEquals(ClickHouseFormat.fromString("RowBinaryWithNamesAndTypes"), ClickHouseFormat.RowBinaryWithNamesAndTypes);
        Assert.assertEquals(ClickHouseFormat.fromString("rowbinarywithnamesandtypes"), ClickHouseFormat.RowBinaryWithNamesAndTypes);
    }

    @Test(groups = { "unit" })
    public void testFromStringInvalid() {
        Assert.expectThrows(IllegalArgumentException.class, () -> ClickHouseFormat.fromString("invalid_format_name_123"));
    }
}
