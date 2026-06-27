package com.clickhouse.client.api.transport.internal;

import java.util.Map;

public interface TransportRequest {


    /**
     * Gives access to transport delegate. Used strictly only by transport.
     * @return internal transport request object
     * @param <T> - Type of delegate
     */
    <T> T getDelegate();

    /**
     * Returns reference to request configuration. Implementation should
     * store only copy because configuration map is created for each request separately
     * @return request configuration map
     */
    Map<String, Object> getConfig();

    /**
     * Cancels request associated with the object. Implementation should
     * treat this method like close() and release all resource.
     * When request is canceled it cannot be reused. All reusable objects
     * should be saved elsewhere.
     * In many cases cancellation of IO operation is problematic and result
     * of this method should not be used in core logic.
     * Operation is idempotent and can be called on canceled request multiple
     * times.
     *
     * @return result of operation. True if request was canceled for sure. False when result cannot be known.
     */
    boolean cancel();

    boolean isCancelled();
}
