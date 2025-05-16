package com.clickhouse.client.api.data_formats.internal;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class InetAddressConverter {

    /**
     * Converts IPv4 address to IPv6 address.
     *
     * @param value IPv4 address
     * @return IPv6 address
     * @throws IllegalArgumentException when failed to convert to IPv6 address
     */
    public static Inet6Address convertToIpv6(InetAddress value) {
        if (value == null || value instanceof Inet6Address) {
            return (Inet6Address) value;
        }

        // https://en.wikipedia.org/wiki/IPv6#IPv4-mapped_IPv6_addresses
        byte[] bytes = new byte[16];
        bytes[10] = (byte) 0xFF;
        bytes[11] = (byte) 0xFF;
        System.arraycopy(value.getAddress(), 0, bytes, 12, 4);

        try {
            return Inet6Address.getByAddress(null, bytes, null);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Converts IPv6 address to IPv4 address if applicable.
     *
     * @param value IPv6 address
     * @return IPv4 address
     * @throws IllegalArgumentException when failed to convert to IPv4 address
     */
    public static Inet4Address convertToIpv4(InetAddress value) {
        if (value == null || value instanceof Inet4Address) {
            return (Inet4Address) value;
        }

        byte[] bytes = value.getAddress();
        boolean invalid = false;
        for (int i = 0; i < 10; i++) {
            if (bytes[i] != (byte) 0) {
                invalid = true;
                break;
            }
        }

        if (!invalid) {
            invalid = bytes[10] != 0xFF || bytes[11] != 0xFF;
        }

        if (invalid) {
            throw new IllegalArgumentException("Failed to convert IPv6 to IPv4");
        }

        byte[] addr = new byte[4];
        System.arraycopy(bytes, 12, addr, 0, 4);
        try {
            return (Inet4Address) InetAddress.getByAddress(addr);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
