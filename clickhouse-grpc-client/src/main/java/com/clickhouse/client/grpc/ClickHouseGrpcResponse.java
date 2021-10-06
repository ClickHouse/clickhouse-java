package com.clickhouse.client.grpc;

import java.util.Map;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.exception.ClickHouseException;
import com.clickhouse.client.grpc.impl.Result;

public class ClickHouseGrpcResponse extends ClickHouseResponse {
    private final ClickHouseStreamObserver observer;
    private final Result result;

    protected ClickHouseGrpcResponse(ClickHouseConfig config, ClickHouseNode server, Map<String, Object> settings,
            ClickHouseStreamObserver observer) throws ClickHouseException {
        super(config, server, settings, observer.getInputStream(), null, observer.getError());

        this.observer = observer;
        this.result = null;

        throwErrorIfAny();
    }

    protected ClickHouseGrpcResponse(ClickHouseConfig config, ClickHouseNode server, Map<String, Object> settings,
            Result result) throws ClickHouseException {
        super(config, server, settings, result.getOutput().newInput(), null,
                result.hasException()
                        ? new ClickHouseException(result.getException().getCode(),
                                result.getException().getDisplayText(), null)
                        : null);

        this.observer = null;
        this.result = result;

        throwErrorIfAny();
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
