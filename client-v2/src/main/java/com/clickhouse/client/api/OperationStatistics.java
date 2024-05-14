package com.clickhouse.client.api;

import com.clickhouse.client.ClickHouseResponseSummary;

/**
 * OperationStatistics objects hold various stats for complete operations.
 * <p>
 * It can be used for logging or monitoring purposes.
 */
public class OperationStatistics {

    public static final ServerStatistics EMPTY_SERVER_STATS = new ServerStatistics(-1, -1, -1, -1, -1, -1, -1);

    public ServerStatistics statsByServer;

    public ClientStatistics statsByClient;

    public OperationStatistics(long startTimestamp) {
        this.statsByServer = EMPTY_SERVER_STATS;
        this.statsByClient = new ClientStatistics(startTimestamp);
    }

    public void setStatsByClient(ClientStatistics statsByClient) {
        this.statsByClient = statsByClient;
    }

    public void updateServerStats(ClickHouseResponseSummary summary) {
        if (summary == null || summary.equals(ClickHouseResponseSummary.EMPTY)) {
            this.statsByServer = EMPTY_SERVER_STATS;
            return;
        }

        this.statsByServer = new ServerStatistics(
                summary.getReadRows(),
                summary.getReadBytes(),
                summary.getTotalRowsToRead(),
                summary.getWrittenRows(),
                summary.getWrittenBytes(),
                summary.getResultRows(),
                summary.getElapsedTime()
        );
    }

    /**
     * Stats returned by the server.
     * <p>
     * `-1` means the value is not available.
     */
    public static class ServerStatistics {

        /**
         * Number of rows read by server from the storage.
         */
        public final long numRowsRead;

        /**
         * Number of rows written by server to the storage.
         */
        public final long numRowsWritten;

        /**
         * Estimated number of rows to read from the storage.
         * <p>
         */
        public final long totalRowsToRead;

        /**
         * Number of bytes read by server from the storage.
         */
        public final long numBytesRead;

        /**
         * Number of bytes written by server to the storage.
         */
        public final long numBytesWritten;

        /**
         * Number of returned rows.
         */
        public final long resultRows;

        /**
         * Elapsed time in nanoseconds.
         */
        public final long elapsedTime;


        public ServerStatistics(long numRowsRead, long numBytesRead, long totalRowsToRead, long numRowsWritten, long numBytesWritten, long resultRows, long elapsedTime) {
            this.numRowsRead = numRowsRead;
            this.numBytesRead = numBytesRead;
            this.totalRowsToRead = totalRowsToRead;
            this.numRowsWritten = numRowsWritten;
            this.numBytesWritten = numBytesWritten;
            this.resultRows = resultRows;
            this.elapsedTime = elapsedTime;
        }

        @Override
        public String toString() {
            return "ServerStatistics{" +
                    "\"numRowsRead\"=" + numRowsRead +
                    ", \"numRowsWritten\"=" + numRowsWritten +
                    ", \"totalRowsToRead\"=" + totalRowsToRead +
                    ", \"numBytesRead\"=" + numBytesRead +
                    ", \"numBytesWritten\"=" + numBytesWritten +
                    ", \"resultRows\"=" + resultRows +
                    ", \"elapsedTime\"=\"" + elapsedTime + "ns\"" +
                    '}';
        }
    }


    public static class ClientStatistics {

        private final long startTimestamp;
        public long elapsedTime;

        public ClientStatistics(long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        public void setEndTimestamp() {
            elapsedTime = System.nanoTime() - startTimestamp;
        }

        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public String toString() {
            return "ClientStatistics{" +
                    "\"elapsedTime\"=\"" + elapsedTime + "ns\"" +
                    '}';
        }
    }

}
