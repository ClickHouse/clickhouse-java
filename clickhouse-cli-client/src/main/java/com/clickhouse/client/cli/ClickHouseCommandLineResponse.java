package com.clickhouse.client.cli;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.data.ClickHouseStreamResponse;

public class ClickHouseCommandLineResponse extends ClickHouseStreamResponse {
    private final transient ClickHouseCommandLine cli;

    protected ClickHouseCommandLineResponse(ClickHouseConfig config, ClickHouseCommandLine cli) throws IOException {
        super(config, cli.getInputStream(), null, null, ClickHouseResponseSummary.EMPTY);

        if (this.input.available() < 1) {
            IOException exp = cli.getError();
            if (exp != null) {
                throw exp;
            }
        }

        this.cli = cli;
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
