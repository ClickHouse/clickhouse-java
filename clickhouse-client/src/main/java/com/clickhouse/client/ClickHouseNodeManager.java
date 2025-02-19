package com.clickhouse.client;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;

import com.clickhouse.client.ClickHouseNode.Status;

/**
 * Node manager is responsible for managing list of nodes and their status. It
 * also runs scheduled tasks in background for node discovery and health check.
 */
@Deprecated
public interface ClickHouseNodeManager extends Function<ClickHouseNodeSelector, ClickHouseNode>, Serializable {
    /**
     * Gets a copy of nodes, which in most cases are in healthy status. However,
     * this really depends on how {@link ClickHouseLoadBalancingPolicy} manages node
     * status. In first-alive policy, it's acutally a full list regardless node
     * status.
     *
     * @return non-null nodes
     */
    List<ClickHouseNode> getNodes();

    /**
     * Gets a copy of filtered nodes.
     *
     * @param selector  node selector for filtering out nodes, null means no filter
     * @param groupSize maximum number of nodes to get, zero or negative value
     *                  means all
     * @return non-null nodes
     */
    List<ClickHouseNode> getNodes(ClickHouseNodeSelector selector, int groupSize);

    /**
     * Gets a copy of faulty nodes.
     *
     * @return non-null faulty nodes
     */
    List<ClickHouseNode> getFaultyNodes();

    /**
     * Gets a copy of filtered faulty nodes.
     *
     * @param selector  node selector for filtering out nodes, null means no filter
     * @param groupSize maximum number of nodes to get, zero or negative value means
     *                  all
     * @return non-null faulty nodes
     */
    List<ClickHouseNode> getFaultyNodes(ClickHouseNodeSelector selector, int groupSize);

    /**
     * Gets load balancing policy.
     *
     * @return non-null load balancing policy
     */
    ClickHouseLoadBalancingPolicy getPolicy();

    /**
     * Gets node selector for filtering out nodes.
     *
     * @return non-null node selector
     */
    ClickHouseNodeSelector getNodeSelector();

    /**
     * Schedule node discovery task immediately. Nothing will happen when task
     * scheduler does not exist(e.g. {@code getPolicy().getScheduler()} returns
     * null) or there's a task running for node discovery.
     *
     * @return optional future for retrieving the running task
     */
    Optional<ScheduledFuture<?>> scheduleDiscovery();

    /**
     * Schedule node discovery task immediately. Nothing will happen when task
     * scheduler does not exist(e.g. {@code getPolicy().getScheduler()} returns
     * null) or there's a task running for health check.
     *
     * @return optional future for retrieving the running task
     */
    Optional<ScheduledFuture<?>> scheduleHealthCheck();

    /**
     * Suggests a different node in order to recover from a failure, which is
     * usually a connection error.
     *
     * @param server  node related to the failure(e.g. the node couldn't be
     *                connected)
     * @param failure recoverable failure
     * @return non-null node which may or may not be same as the given one
     */
    ClickHouseNode suggestNode(ClickHouseNode server, Throwable failure);

    /**
     * Updates node status to one of {@link ClickHouseNode.Status}. It simply
     * delegates the call to {@code getPolicy().update(node, status)} in a
     * thread-safe manner.
     *
     * @param node   non-null node to update
     * @param status non-null status of the node
     */
    void update(ClickHouseNode node, Status status);

    /**
     * Shuts down scheduled tasks if any.
     */
    void shutdown();
}
