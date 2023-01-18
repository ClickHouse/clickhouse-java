package com.clickhouse.data;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseDataTypeTest {
    @Test(groups = { "unit" })
    public void testAlias() {
        for (String alias : ClickHouseDataType.allAliases) {
            Assert.assertTrue(ClickHouseDataType.isAlias(alias));
        }

        for (ClickHouseDataType t : ClickHouseDataType.values()) {
            Assert.assertFalse(ClickHouseDataType.isAlias(t.name()), t.name() + " should not be an alias");
        }
    }

    @Test(groups = { "unit" })
    public void testMapping() {
        for (ClickHouseDataType t : ClickHouseDataType.values()) {
            Assert.assertEquals(ClickHouseDataType.of(t.name()), t);
            if (!t.isCaseSensitive()) {
                Assert.assertEquals(ClickHouseDataType.of(t.name().toLowerCase()), t);
                Assert.assertEquals(ClickHouseDataType.of(t.name().toUpperCase()), t);
            }

            for (String alias : t.getAliases()) {
                Assert.assertEquals(ClickHouseDataType.of(alias), t);
                Assert.assertEquals(ClickHouseDataType.of(alias.toLowerCase()), t);
                Assert.assertEquals(ClickHouseDataType.of(alias.toUpperCase()), t);
            }
        }
    }

    @Test(groups = { "unit" })
    public void testMatch() {
        List<String> matched = ClickHouseDataType.match("INT1");
        Assert.assertEquals(matched.size(), 3);
        Assert.assertEquals(matched.get(0), "INT1");
        Assert.assertEquals(matched.get(1), "INT1 SIGNED");
        Assert.assertEquals(matched.get(2), "INT1 UNSIGNED");

        matched = ClickHouseDataType.match("UInt32");
        Assert.assertEquals(matched.size(), 1);
        Assert.assertEquals(matched.get(0), "UInt32");
    }
}
