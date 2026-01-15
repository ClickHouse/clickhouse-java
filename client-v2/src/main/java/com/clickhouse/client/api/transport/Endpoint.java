package com.clickhouse.client.api.transport;

import java.net.URI;

/**
 * Interface defining the behavior of transport endpoint.
 * It is transport responsibility to provide suitable implementation.
 */
public interface Endpoint {

    /**
     * Returns URI without query parameters
     * @return endpoint url
     */
    URI getURI();

    /**
     * Returns hostname of target server
     * @return dns hostname
     */
    String getHost();

    /**
     * Returns port of target server
     * @return port number
     */
    int getPort();

}
