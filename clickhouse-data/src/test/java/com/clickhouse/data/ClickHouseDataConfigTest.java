package com.clickhouse.data;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseDataConfigTest {
    @Test(groups = { "unit" })
    public void testGetBufferSize() {
        int[] values = new int[] { -1, 0 };
        for (int i : values) {
            for (int j : values) {
                for (int k : values) {
                    Assert.assertEquals(ClickHouseDataConfig.getBufferSize(i, j, k),
                            ClickHouseDataConfig.DEFAULT_BUFFER_SIZE);
                }
            }
        }

        for (int i : values) {
            Assert.assertEquals(ClickHouseDataConfig.getBufferSize(i, 3, 2), 2);
        }
        Assert.assertEquals(ClickHouseDataConfig.getBufferSize(1, 3, 2), 1);
        Assert.assertEquals(ClickHouseDataConfig.getBufferSize(3, 2, 1), 1);
        Assert.assertEquals(ClickHouseDataConfig.getBufferSize(3, 1, 2), 2);
    }
}
