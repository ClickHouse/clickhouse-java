package com.clickhouse.client.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCompression;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDeferredValue;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.data.ClickHouseStreamResponse;
import com.clickhouse.client.grpc.impl.Progress;
import com.clickhouse.client.grpc.impl.Result;
import com.clickhouse.client.grpc.impl.Stats;

import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;

public class ClickHouseGrpcResponse extends ClickHouseStreamResponse {
    private final ClickHouseStreamObserver observer;
    private final Result result;

    static ClickHouseInputStream getInput(ClickHouseConfig config, InputStream input) {
        final ClickHouseInputStream in;
        if (config.getResponseCompressAlgorithm() == ClickHouseCompression.LZ4) {
            in = ClickHouseInputStream.of(
                    ClickHouseDeferredValue.of(() -> {
                        try {
                            return new FramedLZ4CompressorInputStream(input);
                        } catch (IOException e) {
                            return input;
                        }
                    }),
                    config.getReadBufferSize(), null);
        } else {
            in = ClickHouseInputStream.of(input, config.getBufferSize(), config.getResponseCompressAlgorithm(), null);
        }

        return in;
    }

    protected ClickHouseGrpcResponse(ClickHouseConfig config, Map<String, Object> settings,
            ClickHouseStreamObserver observer) throws IOException {
        super(config, observer.getInputStream(), settings, null, observer.getSummary());

        this.observer = observer;
        this.result = null;
    }

    protected ClickHouseGrpcResponse(ClickHouseConfig config, Map<String, Object> settings, Result result)
            throws IOException {
        super(config,
                result.getOutput().isEmpty() ? ClickHouseInputStream.of(result.getOutput().newInput())
                        : getInput(config, result.getOutput().newInput()),
                settings, null,
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
