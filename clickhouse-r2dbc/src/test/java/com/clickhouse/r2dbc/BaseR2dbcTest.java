package com.clickhouse.r2dbc;

import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.config.ClickHouseClientOption;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;

public abstract class BaseR2dbcTest extends BaseIntegrationTest {
    protected static final String CUSTOM_PROTOCOL_NAME = System.getProperty("protocol", "http").toUpperCase();
    protected static final ClickHouseProtocol DEFAULT_PROTOCOL = ClickHouseProtocol
            .valueOf(CUSTOM_PROTOCOL_NAME.indexOf("HTTP") >= 0 ? "HTTP" : CUSTOM_PROTOCOL_NAME);
    protected static final String EXTRA_PARAM = CUSTOM_PROTOCOL_NAME.indexOf("HTTP") >= 0
            && !"HTTP".equals(CUSTOM_PROTOCOL_NAME) ? "http_connection_provider=" + CUSTOM_PROTOCOL_NAME : "";

    @BeforeAll
    public static void beforeSuite() throws Exception {
        ClickHouseServerForTest.beforeSuite();
    }

    @AfterAll
    public static void afterSuite() throws Exception {
        ClickHouseServerForTest.afterSuite();
    }

    private ClickHouseNode addCustomSettings(ClickHouseNode node) {
        if (node == null) {
            return null;
        }
        String key = ClickHouseClientOption.CUSTOM_SETTINGS.getKey();
        String setting = "network_compression_method=lz4";
        String existing = node.getOptions().get(key);
        if (existing != null && !existing.isEmpty()) {
            if (!existing.contains("network_compression_method")) {
                setting = existing + "," + setting;
            } else {
                setting = existing;
            }
        }
        String httpKey = "custom_http_params";
        String httpSetting = "network_compression_method=lz4";
        String httpExisting = node.getOptions().get(httpKey);
        if (httpExisting != null && !httpExisting.isEmpty()) {
            if (!httpExisting.contains("network_compression_method")) {
                httpSetting = httpExisting + "," + httpSetting;
            } else {
                httpSetting = httpExisting;
            }
        }
        return ClickHouseNode.builder(node)
                .addOption(key, setting)
                .addOption(httpKey, httpSetting)
                .build();
    }

    @Override
    protected ClickHouseNode getSecureServer(ClickHouseProtocol protocol) {
        return addCustomSettings(super.getSecureServer(protocol));
    }

    @Override
    protected ClickHouseNode getSecureServer(ClickHouseProtocol protocol, ClickHouseNode base) {
        return addCustomSettings(super.getSecureServer(protocol, base));
    }

    @Override
    protected ClickHouseNode getServer(ClickHouseProtocol protocol) {
        return addCustomSettings(super.getServer(protocol));
    }

    @Override
    protected ClickHouseNode getServer(ClickHouseProtocol protocol, ClickHouseNode base) {
        return addCustomSettings(super.getServer(protocol, base));
    }

    @Override
    protected ClickHouseNode getServer(ClickHouseProtocol protocol, int port) {
        return addCustomSettings(super.getServer(protocol, port));
    }

    @Override
    protected ClickHouseNode getServer(ClickHouseProtocol protocol, Map<String, String> options) {
        return addCustomSettings(super.getServer(protocol, options));
    }

    protected ConnectionFactory getConnectionFactory(ClickHouseProtocol protocol, String... parameters) {
        StringBuilder builder = new StringBuilder(getServer(protocol).toUri("r2dbc:ch:").toString());
        for (String queryString : parameters) {
            if (queryString != null && !queryString.isEmpty()) {
                char sep = builder.indexOf("?") >= 0 ? '&' : '?';
                if (queryString.charAt(0) == '&' || queryString.charAt(0) == '?') {
                    queryString = queryString.substring(1);
                }
                builder.append(sep).append(queryString);
            }
        }
        if (builder.indexOf("custom_settings") == -1) {
            char sep = builder.indexOf("?") >= 0 ? '&' : '?';
            builder.append(sep).append("custom_settings=network_compression_method=lz4");
        }
        if (builder.indexOf("custom_http_params") == -1) {
            char sep = builder.indexOf("?") >= 0 ? '&' : '?';
            builder.append(sep).append("custom_http_params=network_compression_method=lz4");
        }
        ConnectionFactory connectionFactory = ConnectionFactories.get(builder.toString());
        return connectionFactory;
    }
}
