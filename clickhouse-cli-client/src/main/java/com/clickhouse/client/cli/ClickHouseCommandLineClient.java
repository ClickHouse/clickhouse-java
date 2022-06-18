package com.clickhouse.client.cli;

import java.io.IOException;
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
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import com.clickhouse.client.cli.config.ClickHouseCommandLineOption;

/**
 * Wrapper of ClickHouse native command-line client.
 */
public class ClickHouseCommandLineClient extends AbstractClient<ClickHouseCommandLine> {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseCommandLineClient.class);

    static final List<ClickHouseProtocol> SUPPORTED = Collections.singletonList(ClickHouseProtocol.TCP);

    @Override
    protected boolean checkHealth(ClickHouseNode server, int timeout) {
        try (ClickHouseCommandLine cli = getConnection(connect(server).query("select 1"));
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

        return new ClickHouseCommandLine(server, request);
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
        ClickHouseConfig config = getConfig();
        int timeout = config != null ? config.getConnectionTimeout()
                : (int) ClickHouseClientOption.CONNECTION_TIMEOUT.getEffectiveDefaultValue();
        String cli = config != null ? (String) config.getOption(ClickHouseCommandLineOption.CLICKHOUSE_CLI_PATH)
                : (String) ClickHouseCommandLineOption.CLICKHOUSE_CLI_PATH.getEffectiveDefaultValue();
        if (ClickHouseChecker.isNullOrBlank(cli)) {
            cli = ClickHouseCommandLine.DEFAULT_CLICKHOUSE_CLI_PATH;
        }
        String docker = config != null ? (String) config.getOption(ClickHouseCommandLineOption.DOCKER_CLI_PATH)
                : (String) ClickHouseCommandLineOption.DOCKER_CLI_PATH.getEffectiveDefaultValue();
        if (ClickHouseChecker.isNullOrBlank(docker)) {
            docker = ClickHouseCommandLine.DEFAULT_DOCKER_CLI_PATH;
        }
        return ClickHouseProtocol.TCP == protocol
                && (ClickHouseCommandLine.check(timeout, cli, ClickHouseCommandLine.DEFAULT_CLIENT_OPTION,
                        ClickHouseCommandLine.DEFAULT_CLI_ARG_VERSION)
                        || ClickHouseCommandLine.check(timeout, docker, ClickHouseCommandLine.DEFAULT_CLIENT_OPTION,
                                ClickHouseCommandLine.DEFAULT_CLI_ARG_VERSION));
    }

    @Override
    public final Class<? extends ClickHouseOption> getOptionClass() {
        return ClickHouseCommandLineOption.class;
    }
}
