package com.clickhouse.client;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Summary of ClickHouse response.
 */
public class ClickHouseResponseSummary implements Serializable {
    private static final long serialVersionUID = 6241261266635143197L;

    public static final ClickHouseResponseSummary EMPTY = new ClickHouseResponseSummary(null, null, true);

    /**
     * Progress of a query.
     */
    public static final class Progress implements Serializable {
        private static final long serialVersionUID = -1447066780591278108L;

        private final long read_rows;
        private final long read_bytes;
        private final long total_rows_to_read;
        private final long written_rows;
        private final long written_bytes;

        /**
         * Default constructor.
         *
         * @param read_rows          Number of rows read
         * @param read_bytes         Volume of data read in bytes
         * @param total_rows_to_read Total number of rows to be read
         * @param written_rows       Number of rows written
         * @param written_bytes      Volume of data written in bytes
         */
        public Progress(long read_rows, long read_bytes, long total_rows_to_read, long written_rows,
                long written_bytes) {
            this.read_rows = read_rows;
            this.read_bytes = read_bytes;
            this.total_rows_to_read = total_rows_to_read;
            this.written_rows = written_rows;
            this.written_bytes = written_bytes;
        }

        public long getReadRows() {
            return read_rows;
        }

        public long getReadBytes() {
            return read_bytes;
        }

        public long getTotalRowsToRead() {
            return total_rows_to_read;
        }

        public long getWrittenRows() {
            return written_rows;
        }

        public long getWrittenBytes() {
            return written_bytes;
        }
    }

    /**
     * Statistics of a query.
     */
    public static class Statistics implements Serializable {
        private static final long serialVersionUID = -7744796632866829161L;

        private final long rows;
        private final long blocks;
        private final long allocated_bytes;
        private final boolean applied_limit;
        private final long rows_before_limit;

        /**
         * Default constructor.
         *
         * @param rows              The total number of output rows
         * @param blocks
         * @param allocated_bytes
         * @param applied_limit
         * @param rows_before_limit The minimal number of rows there would have been
         *                          without LIMIT
         */
        public Statistics(long rows, long blocks, long allocated_bytes, boolean applied_limit, long rows_before_limit) {
            this.rows = rows;
            this.blocks = blocks;
            this.allocated_bytes = allocated_bytes;
            this.applied_limit = applied_limit;
            this.rows_before_limit = rows_before_limit;
        }

        public long getRows() {
            return rows;
        }

        public long getBlocks() {
            return blocks;
        }

        public long getAllocatedBytes() {
            return allocated_bytes;
        }

        public boolean hasAppliedLimit() {
            return applied_limit;
        }

        public long getRowsBeforeLimit() {
            return rows_before_limit;
        }
    }

    private final AtomicReference<Progress> progress;
    private final AtomicReference<Statistics> stats;
    private final AtomicInteger updates;

    private volatile boolean sealed;

    /**
     * Default constructor.
     *
     * @param progress progress which may or may not be null
     * @param stats    statistics which may or may not be null
     */

    public ClickHouseResponseSummary(Progress progress, Statistics stats) {
        this(progress, stats, false);
    }

    /**
     * Default constructor.
     *
     * @param progress progress which may or may not be null
     * @param stats    statistics which may or may not be null
     * @param sealed   whether the summary is sealed
     */
    protected ClickHouseResponseSummary(Progress progress, Statistics stats, boolean sealed) {
        this.progress = new AtomicReference<>(progress != null ? progress : new Progress(0L, 0L, 0L, 0L, 0L));
        this.stats = new AtomicReference<>(stats != null ? stats : new Statistics(0L, 0L, 0L, false, 0L));
        this.updates = new AtomicInteger(1);

        this.sealed = sealed;
    }

    /**
     * Seals the object so that it cannot be updated any more.
     */
    public void seal() {
        this.sealed = true;
    }

    /**
     * Increases update counter.
     *
     * @return increased update counter
     */
    public int update() {
        return this.updates.incrementAndGet();
    }

    /**
     * Updates query progress.
     *
     * @param progress query progress, null value will be simply ignored
     */
    public void update(Progress progress) {
        if (sealed) {
            throw new IllegalStateException("Sealed summary cannot be updated");
        }

        if (progress != null) {
            this.progress.set(progress);
        }
    }

    public void update(Statistics stats) {
        if (sealed) {
            throw new IllegalStateException("Sealed summary cannot be updated");
        }

        if (stats != null) {
            this.stats.set(stats);
        }
    }

    /**
     * Gets current progress of the query.
     *
     * @return non-null progress
     */
    public Progress getProgress() {
        return progress.get();
    }

    /**
     * Gets statistics of the query.
     *
     * @return non-null statistics
     */
    public Statistics getStatistics() {
        return stats.get();
    }

    public long getReadRows() {
        return progress.get().getReadRows();
    }

    public long getReadBytes() {
        return progress.get().getReadBytes();
    }

    public long getTotalRowsToRead() {
        return progress.get().getTotalRowsToRead();
    }

    public long getWrittenRows() {
        return progress.get().getWrittenRows();
    }

    public long getWrittenBytes() {
        return progress.get().getWrittenBytes();
    }

    public int getUpdateCount() {
        return updates.get();
    }
}
