package com.clickhouse.r2dbc.connection;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNodes;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseUtils;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class ClickHouseConnectionFactoryProvider implements io.r2dbc.spi.ConnectionFactoryProvider {

    /**
     * The name of the driver used for discovery, should not be changed.
     */
    public static final String CLICKHOUSE_DRIVER = "clickhouse";

    /**
     * Data format option.
     */
    public static final Option<String> FORMAT = Option.valueOf(ClickHouseClientOption.FORMAT.getKey());

    private static final List<Option<?>> connQueryParams;

    static {
        Set<Option<?>> allOptions = new LinkedHashSet<>();
        for (ClickHouseOption option : ClickHouseClientOption.values()) {
            allOptions.add(Option.valueOf(option.getKey()));
        }
        try {
            for (ClickHouseClient client : ServiceLoader.load(ClickHouseClient.class,
                    ClickHouseConnectionFactoryProvider.class.getClassLoader())) {
                for (ClickHouseOption option : client.getOptionClass().getEnumConstants()) {
                    allOptions.add(Option.valueOf(option.getKey()));
                }
            }
        } catch (Exception e) {
            // ignore
        }
        connQueryParams = Collections.unmodifiableList(new ArrayList<>(allOptions));
    }

    private String getOptionValueAsString(ConnectionFactoryOptions cfOpt, Option<?> option, Serializable defaultValue) {
        Object value = cfOpt.getValue(option);
        return value != null ? value.toString() : defaultValue.toString();
    }

    private String getHosts(ConnectionFactoryOptions cfOpt) {
        String hosts = getOptionValueAsString(cfOpt, HOST, ClickHouseDefaults.HOST.getEffectiveDefaultValue());
        if (!hosts.contains(",") && !hosts.contains(":")) {
            return hosts + ":" + cfOpt.getValue(PORT);
        }
        return hosts;
    }

    @Override
    public ConnectionFactory create(ConnectionFactoryOptions cfOpt) {
        String hosts = getHosts(cfOpt);
        String database = getOptionValueAsString(cfOpt, DATABASE, "");
        String protocol = getOptionValueAsString(cfOpt, PROTOCOL, ClickHouseProtocol.HTTP.name())
                .toLowerCase(Locale.ROOT);
        if (Boolean.parseBoolean(getOptionValueAsString(cfOpt, SSL, "false"))) {
            protocol += "s";
        }

        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append(protocol).append("://").append(hosts).append('/');
        if (!database.isEmpty()) {
            urlBuilder.append(database);
        }
        String user = getOptionValueAsString(cfOpt, USER, "");
        String password = getOptionValueAsString(cfOpt, PASSWORD, "");
        urlBuilder.append("?user=").append(ClickHouseUtils.encode(user)).append("&password=")
                .append(ClickHouseUtils.encode(password));
        for (Option<?> option : connQueryParams) {
            Object value = cfOpt.getValue(option);
            if (value != null) {
                urlBuilder.append('&').append(option.name()).append('=')
                        .append(ClickHouseUtils.encode(cfOpt.getValue(option).toString()));
            }
        }
        ClickHouseNodes nodes = ClickHouseNodes.of(urlBuilder.toString());
        return new ClickHouseConnectionFactory(nodes);
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        String driverIdentifier = Objects.toString(connectionFactoryOptions.getValue(DRIVER));
        return "ch".equalsIgnoreCase(driverIdentifier) || CLICKHOUSE_DRIVER.equalsIgnoreCase(driverIdentifier);
    }

    @Override
    public String getDriver() {
        return CLICKHOUSE_DRIVER;
    }
}
