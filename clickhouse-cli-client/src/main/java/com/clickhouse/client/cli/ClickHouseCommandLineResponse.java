package com.clickhouse.client.cli;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseStreamResponse;
// deprecate from version 0.6.0
@Deprecated
public class ClickHouseCommandLineResponse extends ClickHouseStreamResponse {
    private static final long serialVersionUID = 4253185543390807162L;

    private final transient ClickHouseCommandLine cli;

    protected ClickHouseCommandLineResponse(ClickHouseConfig config, ClickHouseCommandLine cli) throws IOException {
        super(config, cli.getInputStream(), null, null, ClickHouseResponseSummary.EMPTY, null);
        this.cli = cli;

        if (processor.getInputStream().available() < 1) {
            IOException exp = cli.getError();
            if (exp != null) {
                throw exp;
            }
        }
    }

    @Override
    public ClickHouseResponseSummary getSummary() {
        return summary;
    }

    @Override
    public void close() {
        try {
            if (cli != null) {
                IOException exp = cli.getError();
                if (exp != null) {
                    throw new UncheckedIOException(exp);
                }
            }
        } finally {
            super.close();
        }
    }
}
