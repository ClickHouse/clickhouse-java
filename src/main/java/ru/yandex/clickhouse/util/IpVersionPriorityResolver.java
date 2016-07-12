package ru.yandex.clickhouse.util;

import org.apache.http.conn.DnsResolver;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;


public class IpVersionPriorityResolver implements DnsResolver {

    private DnsResolver defaultResolver = new SystemDefaultDnsResolver();

    private boolean preferV6 = true;

    public IpVersionPriorityResolver() {
    }

    public IpVersionPriorityResolver(boolean preferV6) {
        this.preferV6 = preferV6;
    }

    @Override
    public InetAddress[] resolve(String host) throws UnknownHostException {
        InetAddress[] resolve = defaultResolver.resolve(host);
        Comparator<InetAddress> comparator = new Comparator<InetAddress>() {
            @Override
            public int compare(InetAddress o1, InetAddress o2) {
                boolean o16 = o1 instanceof Inet6Address;
                boolean o26 = o2 instanceof Inet6Address;
                if (o16 == o26) return 0;
                if (o16) return -1;
                if (o26) return 1;
                return 0;
            }
        };
        if (!preferV6) comparator = Collections.reverseOrder(comparator);
        Arrays.sort(resolve, comparator);
        return resolve;
    }

    public void setPreferV6(boolean preferV6) {
        this.preferV6 = preferV6;
    }
}
