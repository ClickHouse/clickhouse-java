package com.clickhouse.client.grpc;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseStreamResponse;
import com.clickhouse.client.grpc.impl.Progress;
import com.clickhouse.client.grpc.impl.Result;
import com.clickhouse.client.grpc.impl.Stats;
import com.clickhouse.data.ClickHouseInputStream;
@Deprecated
public class ClickHouseGrpcResponse extends ClickHouseStreamResponse {
    private final transient ClickHouseStreamObserver observer;

    static void checkError(Result result) {
        if (result != null && result.hasException()) {
            throw new UncheckedIOException(new IOException(ClickHouseException
                    .buildErrorMessage(result.getException().getCode(),
                            result.getException().getDisplayText())));
        }
    }

    protected ClickHouseGrpcResponse(ClickHouseConfig config, Map<String, Serializable> settings,
            ClickHouseStreamObserver observer) throws IOException {
        super(config, observer.getInputStream(), settings, null, observer.getSummary());

        this.observer = observer;
    }

    protected ClickHouseGrpcResponse(ClickHouseConfig config, Map<String, Serializable> settings, Result result)
            throws IOException {
        super(config,
                result.getOutput().isEmpty()
                        ? ClickHouseInputStream.of(result.getOutput().newInput(), config.getReadBufferSize(),
                                () -> checkError(result))
                        : ClickHouseGrpcClientImpl.getInput(config, result.getOutput().newInput(),
                                () -> checkError(result)),
                settings, null,
                new ClickHouseResponseSummary(null, null));

        this.observer = null;
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

    @Override
    public void close() {
        try {
            if (observer != null) {
                observer.awaitCompletion(config.getSocketTimeout(), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            super.close();
        }
    }
}
