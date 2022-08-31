package com.clickhouse.client.cli;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.clickhouse.client.AbstractClient;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import com.clickhouse.client.cli.config.ClickHouseCommandLineOption;

/**
 * Wrapper of ClickHouse native command-line client.
 */
public class ClickHouseCommandLineClient extends AbstractClient<ClickHouseCommandLine> {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseCommandLineClient.class);

    static final List<ClickHouseProtocol> SUPPORTED = Collections
            .unmodifiableList(Arrays.asList(ClickHouseProtocol.LOCAL, ClickHouseProtocol.TCP));

    private String getCommandLine(String option) {
        ClickHouseConfig config = getConfig();
        int timeout = config.getConnectionTimeout();
        String cli = config.getStrOption(ClickHouseCommandLineOption.CLICKHOUSE_CLI_PATH);
        if (ClickHouseChecker.isNullOrBlank(cli)) {
            cli = ClickHouseCommandLine.DEFAULT_CLICKHOUSE_CLI_PATH;
        }
        if (!ClickHouseCommandLine.check(timeout, cli, option, ClickHouseCommandLine.DEFAULT_CLI_ARG_VERSION)) {
            cli = config.getStrOption(ClickHouseCommandLineOption.DOCKER_CLI_PATH);
            if (ClickHouseChecker.isNullOrBlank(cli)) {
                cli = ClickHouseCommandLine.DEFAULT_DOCKER_CLI_PATH;
            }
            if (!ClickHouseCommandLine.check(timeout, cli, option, ClickHouseCommandLine.DEFAULT_CLI_ARG_VERSION)) {
                cli = null;
            }
        }
        return cli;
    }

    @Override
    protected boolean checkHealth(ClickHouseNode server, int timeout) {
        try (ClickHouseCommandLine cli = getConnection(connect(server).query("SELECT 1"));
                ClickHouseCommandLineResponse response = new ClickHouseCommandLineResponse(getConfig(), cli)) {
            return response.firstRecord().getValue(0).asInteger() == 1;
        } catch (Exception e) {
            // ignore
        }
        return false;
    }

    @Override
    protected ClickHouseCommandLine newConnection(ClickHouseCommandLine conn, ClickHouseNode server,
            ClickHouseRequest<?> request) {
        if (conn != null) {
            closeConnection(conn, false);
        }

        return new ClickHouseCommandLine(request);
    }

    @Override
    protected boolean checkConnection(ClickHouseCommandLine connection, ClickHouseNode requestServer,
            ClickHouseNode currentServer, ClickHouseRequest<?> request) {
        return false;
    }

    @Override
    protected void closeConnection(ClickHouseCommandLine conn, boolean force) {
        try {
            conn.close();
        } catch (Exception e) {
            log.warn("Failed to close http connection due to: %s", e.getMessage());
        }
    }

    @Override
    protected Collection<ClickHouseProtocol> getSupportedProtocols() {
        return SUPPORTED;
    }

    @Override
    protected ClickHouseResponse send(ClickHouseRequest<?> sealedRequest) throws ClickHouseException, IOException {
        return new ClickHouseCommandLineResponse(sealedRequest.getConfig(), getConnection(sealedRequest));
    }

    @Override
    public boolean accept(ClickHouseProtocol protocol) {
        final String option;
        switch (protocol) {
            case LOCAL:
                option = ClickHouseCommandLine.DEFAULT_LOCAL_OPTION;
                break;
            case TCP:
                option = ClickHouseCommandLine.DEFAULT_CLIENT_OPTION;
                break;
            default:
                option = null;
                break;
        }
        return option != null && getCommandLine(option) != null;
    }

    @Override
    public final Class<? extends ClickHouseOption> getOptionClass() {
        return ClickHouseCommandLineOption.class;
    }
}
