package com.clickhouse.client.grpc;

import java.io.IOException;
import java.util.Map;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.data.ClickHouseStreamResponse;
import com.clickhouse.client.grpc.impl.Result;

public class ClickHouseGrpcResponse extends ClickHouseStreamResponse {
    private final ClickHouseStreamObserver observer;
    private final Result result;

    protected ClickHouseGrpcResponse(ClickHouseConfig config, Map<String, Object> settings,
            ClickHouseStreamObserver observer) throws IOException {
        super(config, settings, observer.getInputStream(), null);

        this.observer = observer;
        this.result = null;
    }

    protected ClickHouseGrpcResponse(ClickHouseConfig config, Map<String, Object> settings, Result result)
            throws IOException {
        super(config, settings, result.getOutput().newInput(), null);

        this.observer = null;
        this.result = result;
    }

    @Override
    public ClickHouseResponseSummary getSummary() {
        ClickHouseResponseSummary summary = super.getSummary();

        if (result != null && (result.hasProgress() || result.hasStats())) {
            summary = new ClickHouseResponseSummary() {
                @Override
                public long getAllocatedBytes() {
                    return result.getStats().getAllocatedBytes();
                }

                @Override
                public long getBlocks() {
                    return result.getStats().getBlocks();
                }

                @Override
                public long getReadBytes() {
                    return result.getProgress().getReadBytes();
                }

                @Override
                public long getReadRows() {
                    return result.getProgress().getReadRows();
                }

                @Override
                public long getRows() {
                    return result.getStats().getRows();
                }

                @Override
                public long getRowsBeforeLimit() {
                    return result.getStats().getRowsBeforeLimit();
                }

                @Override
                public long getTotalRowsToRead() {
                    return result.getProgress().getTotalRowsToRead();
                }

                @Override
                public long getWriteBytes() {
                    return result.getProgress().getWrittenBytes();
                }

                @Override
                public long getWriteRows() {
                    return result.getProgress().getWrittenRows();
                }
            };
        } else if (observer != null) {
            summary = observer.getSummary();
        }
        return summary;
    }
}
