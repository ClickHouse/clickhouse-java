package com.clickhouse.client.grpc;

import java.io.IOException;
import java.util.Map;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.data.ClickHouseStreamResponse;
import com.clickhouse.client.grpc.impl.Progress;
import com.clickhouse.client.grpc.impl.Result;
import com.clickhouse.client.grpc.impl.Stats;

public class ClickHouseGrpcResponse extends ClickHouseStreamResponse {
    private final ClickHouseStreamObserver observer;
    private final Result result;

    protected ClickHouseGrpcResponse(ClickHouseConfig config, Map<String, Object> settings,
            ClickHouseStreamObserver observer) throws IOException {
        super(config, ClickHouseInputStream.of(observer.getInputStream()), settings, null, observer.getSummary());

        this.observer = observer;
        this.result = null;
    }

    protected ClickHouseGrpcResponse(ClickHouseConfig config, Map<String, Object> settings, Result result)
            throws IOException {
        super(config, ClickHouseInputStream.of(result.getOutput().newInput()), settings, null,
                new ClickHouseResponseSummary(null, null));

        this.observer = null;
        this.result = result;
        if (result.hasProgress()) {
            Progress p = result.getProgress();
            summary.update(new ClickHouseResponseSummary.Progress(p.getReadRows(), p.getReadBytes(),
                    p.getTotalRowsToRead(), p.getWrittenRows(), p.getWrittenBytes()));
        }

        if (result.hasStats()) {
            Stats s = result.getStats();
            summary.update(new ClickHouseResponseSummary.Statistics(s.getRows(), s.getBlocks(), s.getAllocatedBytes(),
                    s.getAppliedLimit(), s.getRowsBeforeLimit()));
        }
    }

    @Override
    public ClickHouseResponseSummary getSummary() {
        return summary;
    }
}
