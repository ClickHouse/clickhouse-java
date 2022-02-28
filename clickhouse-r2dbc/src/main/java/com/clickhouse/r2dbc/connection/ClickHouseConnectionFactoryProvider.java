package com.clickhouse.r2dbc.connection;

import com.clickhouse.client.ClickHouseNodes;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class ClickHouseConnectionFactoryProvider implements io.r2dbc.spi.ConnectionFactoryProvider {

    /**
     * The name of the driver used for discovery, should not be changed.
     */
    public static final String CLICKHOUSE_DRIVER = "clickhouse";

    private static final List<String> connQueryParams = Arrays.asList("auto_discovery", "node_discovery_interval",
            "node_discovery_limit", "load_balancing_policy", "load_balancing_tags", "health_check_method",
            "health_check_interval", "check_all_nodes", "node_check_interval", "node_group_size", "failover",
            "retry");

    @Override
    public ConnectionFactory create(ConnectionFactoryOptions cfOpt) {
        String hosts = getHosts(cfOpt);
        String database = cfOpt.getValue(DATABASE).toString();
        String protocol = cfOpt.getValue(PROTOCOL).toString();
        if (cfOpt.getValue(USER) == null ) {
            throw new IllegalArgumentException("User and password is mandatory.");
        }
        String username = cfOpt.getValue(USER).toString();
        String password = "";
        if (cfOpt.getValue(PASSWORD) != null) {
            password = cfOpt.getValue(PASSWORD).toString();
        }

        StringBuilder urlBuilder = new StringBuilder(String.format("%s://%s/%s?user=%s&password=%s", protocol, hosts, database, username, password));
        String params = connQueryParams.stream().filter(queryParam -> cfOpt.getValue(Option.valueOf(queryParam)) != null)
                .map(queryParam -> String.format("%s=%s", queryParam, cfOpt.getValue(Option.valueOf(queryParam))))
                .collect(Collectors.joining("%"));
        urlBuilder.append(params.isEmpty() ? "" : ("&" + params));

        ClickHouseNodes nodes = ClickHouseNodes.of(urlBuilder.toString());
        return new ClickHouseConnectionFactory(nodes);
    }

    private String getHosts(ConnectionFactoryOptions cfOpt) {
        String hosts = cfOpt.getValue(HOST).toString();
        if (!hosts.contains(",") && !hosts.contains(":")){
            return hosts + ":" + cfOpt.getValue(PORT);
        }
        return hosts;
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        return connectionFactoryOptions.getValue(DRIVER).equals(CLICKHOUSE_DRIVER);
    }

    @Override
    public String getDriver() {
        return CLICKHOUSE_DRIVER;
    }
}
