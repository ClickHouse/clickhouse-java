package com.clickhouse.client.api;

import com.clickhouse.client.ClickHouseResponseSummary;

/**
 * OperationStatistics objects hold various stats for complete operations.
 * <p>
 * It can be used for logging or monitoring purposes.
 */
public class OperationStatistics {

    public static final ServerStatistics EMPTY_SERVER_STATS = new ServerStatistics(-1, -1, -1, -1, -1, -1, -1);

    public final ServerStatistics statsByServer;


    public OperationStatistics(ClickHouseResponseSummary summaryFromServer) {
        this.statsByServer = parseServerStats(summaryFromServer);
    }

    private ServerStatistics parseServerStats(ClickHouseResponseSummary summaryFromServer) {
        if (summaryFromServer == null || summaryFromServer.equals(ClickHouseResponseSummary.EMPTY)) {
            return EMPTY_SERVER_STATS;
        }

        return new ServerStatistics(
                summaryFromServer.getReadRows(),
                summaryFromServer.getReadBytes(),
                summaryFromServer.getTotalRowsToRead(),
                summaryFromServer.getWrittenRows(),
                summaryFromServer.getWrittenBytes(),
                summaryFromServer.getResultRows(),
                summaryFromServer.getElapsedTime()
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


}
