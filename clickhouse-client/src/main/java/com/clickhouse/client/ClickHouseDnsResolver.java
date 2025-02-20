package com.clickhouse.client;

import java.net.InetSocketAddress;

import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.naming.SrvResolver;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * Default DNS resolver. It tries to look up service record (SRV record) when
 * {@link com.clickhouse.client.config.ClickHouseDefaults#SRV_RESOLVE} is set to
 * {@code true}.
 */
@Deprecated
public class ClickHouseDnsResolver {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseDnsResolver.class);

    private static final ClickHouseDnsResolver instance = ClickHouseUtils.getService(ClickHouseDnsResolver.class,
            new ClickHouseDnsResolver());

    protected static ClickHouseDnsResolver newInstance() {
        ClickHouseDnsResolver resolver = null;

        if ((boolean) ClickHouseDefaults.SRV_RESOLVE.getEffectiveDefaultValue()) {
            try {
                resolver = new SrvResolver();
            } catch (Throwable e) {
                log.warn("Failed to enable SRV resolver due to:", e);
            }
        }

        return resolver == null ? new ClickHouseDnsResolver() : resolver;
    }

    public static ClickHouseDnsResolver getInstance() {
        return instance;
    }

    public InetSocketAddress resolve(ClickHouseProtocol protocol, String host, int port) {
        return new InetSocketAddress(host, port);
    }
}
