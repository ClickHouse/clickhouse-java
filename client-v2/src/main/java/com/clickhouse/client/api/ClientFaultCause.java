package com.clickhouse.client.api;

public enum ClientFaultCause {

    None,

    NoHttpResponse,
    ConnectTimeout,
    ConnectionRequestTimeout,
    SocketTimeout,
}
