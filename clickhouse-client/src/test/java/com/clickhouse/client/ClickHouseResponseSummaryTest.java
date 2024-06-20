package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.client.ClickHouseResponseSummary.Progress;
import com.clickhouse.client.ClickHouseResponseSummary.Statistics;

public class ClickHouseResponseSummaryTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        ClickHouseResponseSummary summary = new ClickHouseResponseSummary(null, null);
        Assert.assertNotNull(summary.getProgress());
        Assert.assertNotNull(summary.getStatistics());
        Assert.assertEquals(summary.getReadBytes(), 0L);
        Assert.assertEquals(summary.getReadRows(), 0L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 0L);
        Assert.assertEquals(summary.getUpdateCount(), 0L);
        Assert.assertEquals(summary.getWrittenBytes(), 0L);
        Assert.assertEquals(summary.getWrittenRows(), 0L);

        Progress progress = new Progress(1L, 2L, 3L, 4L, 5L,
                6L, 7L);
        Statistics stats = new Statistics(6L, 7L, 8L, true, 9L);
        summary = new ClickHouseResponseSummary(progress, stats);
        Assert.assertTrue(summary.getProgress() == progress);
        Assert.assertTrue(summary.getStatistics() == stats);
        Assert.assertEquals(summary.getReadBytes(), 2L);
        Assert.assertEquals(summary.getReadRows(), 1L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 3L);
        Assert.assertEquals(summary.getUpdateCount(), 1L);
        Assert.assertEquals(summary.getWrittenBytes(), 5L);
        Assert.assertEquals(summary.getWrittenRows(), 4L);
    }

    @Test(groups = { "unit" })
    public void testAdd() {
        ClickHouseResponseSummary summary = new ClickHouseResponseSummary(null, null);
        Assert.assertEquals(summary.getReadBytes(), 0L);
        Assert.assertEquals(summary.getReadRows(), 0L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 0L);
        Assert.assertEquals(summary.getUpdateCount(), 0L);
        Assert.assertEquals(summary.getWrittenBytes(), 0L);
        Assert.assertEquals(summary.getWrittenRows(), 0L);

        summary.add(summary);
        Assert.assertEquals(summary.getReadBytes(), 0L);
        Assert.assertEquals(summary.getReadRows(), 0L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 0L);
        Assert.assertEquals(summary.getUpdateCount(), 1L);
        Assert.assertEquals(summary.getWrittenBytes(), 0L);
        Assert.assertEquals(summary.getWrittenRows(), 0L);

        summary.add(ClickHouseResponseSummary.EMPTY);
        Assert.assertEquals(summary.getReadBytes(), 0L);
        Assert.assertEquals(summary.getReadRows(), 0L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 0L);
        Assert.assertEquals(summary.getUpdateCount(), 2L);
        Assert.assertEquals(summary.getWrittenBytes(), 0L);
        Assert.assertEquals(summary.getWrittenRows(), 0L);

        summary.add(new Progress(1L, 2L, 3L, 4L, 5L, 6L, 7L));
        Assert.assertEquals(summary.getReadBytes(), 2L);
        Assert.assertEquals(summary.getReadRows(), 1L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 3L);
        Assert.assertEquals(summary.getUpdateCount(), 3L);
        Assert.assertEquals(summary.getWrittenBytes(), 5L);
        Assert.assertEquals(summary.getWrittenRows(), 4L);

        summary.add(new Statistics(6L, 7L, 8L, true, 9L));
        Assert.assertEquals(summary.getReadBytes(), 2L);
        Assert.assertEquals(summary.getReadRows(), 1L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 3L);
        Assert.assertEquals(summary.getUpdateCount(), 3L);
        Assert.assertEquals(summary.getWrittenBytes(), 5L);
        Assert.assertEquals(summary.getWrittenRows(), 4L);
    }

    @Test(groups = { "unit" })
    public void testUpdate() {
        ClickHouseResponseSummary summary = new ClickHouseResponseSummary(null, null);
        Assert.assertEquals(summary.getReadBytes(), 0L);
        Assert.assertEquals(summary.getReadRows(), 0L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 0L);
        Assert.assertEquals(summary.getUpdateCount(), 0L);
        Assert.assertEquals(summary.getWrittenBytes(), 0L);
        Assert.assertEquals(summary.getWrittenRows(), 0L);

        summary.update();
        Assert.assertEquals(summary.getReadBytes(), 0L);
        Assert.assertEquals(summary.getReadRows(), 0L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 0L);
        Assert.assertEquals(summary.getUpdateCount(), 1L);
        Assert.assertEquals(summary.getWrittenBytes(), 0L);
        Assert.assertEquals(summary.getWrittenRows(), 0L);

        summary.update(new Progress(1L, 2L, 3L, 4L, 5L, 6L, 7L));
        Assert.assertEquals(summary.getReadBytes(), 2L);
        Assert.assertEquals(summary.getReadRows(), 1L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 3L);
        Assert.assertEquals(summary.getUpdateCount(), 1L);
        Assert.assertEquals(summary.getWrittenBytes(), 5L);
        Assert.assertEquals(summary.getWrittenRows(), 4L);
        Assert.assertEquals(summary.getProgress().getElapsedTime(), 6L);
        Assert.assertEquals(summary.getProgress().getResultRows(), 7L);

        summary.update(new Statistics(6L, 7L, 8L, true, 9L));
        Assert.assertEquals(summary.getReadBytes(), 2L);
        Assert.assertEquals(summary.getReadRows(), 1L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 3L);
        Assert.assertEquals(summary.getUpdateCount(), 1L);
        Assert.assertEquals(summary.getWrittenBytes(), 5L);
        Assert.assertEquals(summary.getWrittenRows(), 4L);
    }

    @Test(groups = { "unit" })
    public void testSeal() {
        ClickHouseResponseSummary summary = new ClickHouseResponseSummary(null, null);
        summary.add(summary);
        summary.add(summary.getProgress());
        summary.add(summary.getStatistics());
        summary.update();
        summary.update(summary.getProgress());
        summary.update(summary.getStatistics());
        Assert.assertEquals(summary.getReadBytes(), 0L);
        Assert.assertEquals(summary.getReadRows(), 0L);
        Assert.assertEquals(summary.getTotalRowsToRead(), 0L);
        Assert.assertEquals(summary.getUpdateCount(), 3L);
        Assert.assertEquals(summary.getWrittenBytes(), 0L);
        Assert.assertEquals(summary.getWrittenRows(), 0L);

        summary.seal();

        Assert.assertThrows(IllegalStateException.class, () -> summary.add(summary));
        Assert.assertThrows(IllegalStateException.class, () -> summary.add(summary.getProgress()));
        Assert.assertThrows(IllegalStateException.class, () -> summary.add(summary.getStatistics()));

        Assert.assertThrows(IllegalStateException.class, () -> summary.update());
        Assert.assertThrows(IllegalStateException.class, () -> summary.update(summary.getProgress()));
        Assert.assertThrows(IllegalStateException.class, () -> summary.update(summary.getStatistics()));
    }
}