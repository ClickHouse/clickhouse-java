package com.clickhouse.client;

import java.net.InetSocketAddress;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

/**
 * Default DNS resolver. It tries to look up service record (SRV record) when
 * {@link com.clickhouse.client.config.ClickHouseDefaults#DNS_RESOLVE} is set to
 * {@code true}.
 */
public class ClickHouseDnsResolver {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseDnsResolver.class);

    private static final ClickHouseDnsResolver instance = ClickHouseUtils.getService(ClickHouseDnsResolver.class,
            new ClickHouseDnsResolver());

    public static ClickHouseDnsResolver getInstance() {
        return instance;
    }

    protected ClickHouseDnsResolver() {
    }

    public InetSocketAddress resolve(ClickHouseProtocol protocol, String host, int port) {
        if (protocol == null || host == null) {
            throw new IllegalArgumentException("Non-null protocol and host are required");
        }

        if ((boolean) ClickHouseDefaults.DNS_RESOLVE.getEffectiveDefaultValue()) {
            SRVRecord r = resolve(host, false);
            if (r != null) {
                host = r.getName().canonicalize().toString(true);
                port = r.getPort();
            }
        } else {
            host = ClickHouseDefaults.HOST.getEffectiveValue(host);
            port = port <= 0 ? protocol.getDefaultPort() : port;
        }

        return new InetSocketAddress(host, port);
    }

    // TODO register a callback for DNS change?

    protected SRVRecord resolve(String srvDns, boolean basedOnWeight) {
        Record[] records = null;
        try {
            records = new Lookup(srvDns, Type.SRV).run();
        } catch (TextParseException e) {
            // fallback to a cached entry?
            log.warn("Not able to resolve given DNS query: [%s]", srvDns, e);
        }

        SRVRecord record = null;
        if (records != null) {
            if (basedOnWeight) {
                for (int i = 0; i < records.length; i++) {
                    SRVRecord rec = (SRVRecord) records[i];
                    if (record == null || record.getWeight() > rec.getWeight()) {
                        record = rec;
                    }
                }
            } else {
                record = (SRVRecord) records[0];
            }
        }

        return record;
    }
}
