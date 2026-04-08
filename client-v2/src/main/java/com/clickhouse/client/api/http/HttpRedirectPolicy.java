package com.clickhouse.client.api.http;

/**
 * Controls high-level redirect policy for HTTP requests.
 */
public interface HttpRedirectPolicy {
    /**
     * @return true when cross-origin redirects are allowed
     */
    boolean isCrossOriginRedirectAllowed();
}
