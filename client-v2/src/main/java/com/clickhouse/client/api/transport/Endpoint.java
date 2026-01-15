package com.clickhouse.client.api.transport;

/**
 * Interface defining the behavior of transport endpoint.
 * It is transport responsibility to provide suitable implementation.
 */
public interface Endpoint {

    String getBaseURL();

    String getHost();

    int getPort();

}
