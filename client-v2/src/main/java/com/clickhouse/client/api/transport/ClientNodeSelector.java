package com.clickhouse.client.api.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * So we will look up for the first-alive node and then assign that node to 
 * client to talk.
 * 
 * <p>Endpoints are tried in the order they were registered (index 0 is the
 * "primary"). When a request fails, the failed endpoint is quarantined for
 * {@link #DEFAULT_QUARANTINE_MS} milliseconds and the next alive endpoint
 * is returned. Once the quarantine expires the node automatically becomes
 * eligible again, so traffic returns to the primary without any explicit
 * reset.</p>
 *
 * <p>If all endpoints are quarantined, the primary (index 0) is returned
 * as a fallback to avoid a complete lockout.</p>
 *
 * <p>This class is thread-safe: concurrent callers may invoke
 * {@link #getEndpoint()} and {@link #getNextAliveNode(Endpoint)}
 * from different threads.</p>
 */
public class ClientNodeSelector {

    private static final Logger LOG = LoggerFactory.getLogger(ClientNodeSelector.class);

    static final long DEFAULT_QUARANTINE_MS = 30_000;

    private final List<EndpointState> endpointStates;

    /** Set while all endpoints are quarantined, so that the fallback is logged once per outage, not per request. */
    private final AtomicBoolean allEndpointsDownLogged = new AtomicBoolean();

    public ClientNodeSelector(List<Endpoint> endpoints) {
        List<EndpointState> states = new ArrayList<>(endpoints.size());
        for (Endpoint ep : endpoints) {
            states.add(new EndpointState(ep));
        }
        this.endpointStates = Collections.unmodifiableList(states);
    }

    public Endpoint getEndpoint() {
        for (EndpointState state : endpointStates) {
            if (state.isAlive()) {
                if (allEndpointsDownLogged.get()) {
                    allEndpointsDownLogged.set(false);
                }
                return state.getEndpoint();
            }
        }
        if (allEndpointsDownLogged.compareAndSet(false, true)) {
            LOG.warn("All endpoints are non-responsive, falling back to primary endpoint");
        }
        return endpointStates.get(0).getEndpoint();
    }

    public Endpoint getNextAliveNode(Endpoint failedEndpoint) {
        for (EndpointState state : endpointStates) {
            if (state.getEndpoint().equals(failedEndpoint)) {
                // A failure of an endpoint that is quarantined already only extends the quarantine.
                if (state.markFailed(DEFAULT_QUARANTINE_MS)) {
                    LOG.warn("Endpoint {} quarantined for {} ms", failedEndpoint.getHost(), DEFAULT_QUARANTINE_MS);
                }
                break;
            }
        }
        return getEndpoint();
    }
}
