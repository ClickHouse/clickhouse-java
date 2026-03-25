package com.clickhouse.client.api;

import java.net.InetAddress;
import java.net.UnknownHostException;

@FunctionalInterface
public interface HostResolver {
    HostResolver DEFAULT = InetAddress::getByName;

    InetAddress resolve(String host) throws UnknownHostException;
}
