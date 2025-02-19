package com.clickhouse.client;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.clickhouse.client.ClickHouseNode.Status;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseUtils;

/**
 * Load balancing policy. In general, a policy is responsible for 3 things: 1)
 * get node from a managed list; 2) managing node's status; and 3) optionally
 * schedule background tasks like node discovery and health check.
 */
@Deprecated
public abstract class ClickHouseLoadBalancingPolicy implements Serializable {
    static class DefaultPolicy extends ClickHouseLoadBalancingPolicy {
        @Override
        protected ScheduledExecutorService getScheduler() {
            return null;
        }
    }

    static class FirstAlivePolicy extends ClickHouseLoadBalancingPolicy {
        @Override
        protected ClickHouseNode get(ClickHouseNodes manager, ClickHouseNodeSelector t) {
            boolean noSelector = t == null || t == ClickHouseNodeSelector.EMPTY;
            ClickHouseNode node = null;
            for (ClickHouseNode n : manager.nodes) {
                if (noSelector || t.match(n)) {
                    node = n;
                }
                if (node != null && !manager.faultyNodes.contains(node)) {
                    break;
                }
            }
            if (node == null) {
                throw new IllegalArgumentException(ClickHouseUtils.format(ERROR_NO_SUITABLE_NODE, manager, t));
            }
            return node;
        }

        @Override
        protected void update(ClickHouseNodes manager, ClickHouseNode node, Status status) {
            if (status != Status.HEALTHY && status != Status.FAULTY) {
                super.update(manager, node, status);
                return;
            }

            if (status == Status.HEALTHY) {
                manager.faultyNodes.remove(node);
            } else if (!manager.faultyNodes.contains(node)) {
                manager.faultyNodes.add(node);
                manager.scheduleHealthCheck();
            }
        }
    }

    static class RandomPolicy extends ClickHouseLoadBalancingPolicy {
        private final Random rand;

        protected RandomPolicy() {
            this.rand = new Random(System.currentTimeMillis()); // NOSONAR
        }

        @Override
        protected ClickHouseNode get(ClickHouseNodes manager, ClickHouseNodeSelector t) {
            int size = manager.nodes.size();
            manager.index.set(size < 1 ? 0 : rand.nextInt(size));
            return super.get(manager, t);
        }
    }

    static class RoundRobinPolicy extends ClickHouseLoadBalancingPolicy {
        @Override
        protected ClickHouseNode get(ClickHouseNodes manager, ClickHouseNodeSelector t) {
            boolean noSelector = t == null || t == ClickHouseNodeSelector.EMPTY;
            int idx = manager.index.getAndUpdate(v -> v + 1 >= manager.nodes.size() ? 0 : v + 1);
            int i = 0;
            ClickHouseNode node = null;
            for (ClickHouseNode n : manager.nodes) {
                if (noSelector || t.match(n)) {
                    node = n;
                }
                if (i++ >= idx && node != null) {
                    break;
                }
            }
            if (node == null) {
                throw new IllegalArgumentException(ClickHouseUtils.format(ERROR_NO_SUITABLE_NODE, manager, t));
            }
            return node;
        }
    }

    private static final long serialVersionUID = 1481796695764210324L;
    private static final Map<String, ClickHouseLoadBalancingPolicy> policies = new ConcurrentHashMap<>();

    static final ClickHouseLoadBalancingPolicy DEFAULT = new DefaultPolicy();

    static final String ERROR_NO_SUITABLE_NODE = "%s does not contain suitable node for %s";

    public static final String QUERY_GET_ALL_NODES = "select cluster, host_address, host_name, "
            + "replica_num, shard_num, shard_weight from system.clusters where is_local=1";
    public static final String QUERY_GET_CLUSTER_NODES = "select cluster, host_address, host_name, "
            + "replica_num, shard_num, shard_weight from system.clusters where is_local=1 and cluster=:cluster";
    public static final String QUERY_GET_OTHER_NODES = "select distinct cluster, :host, replica_num, shard_num, shard_weight "
            + "from system.clusters where is_local=0 and cluster in :cluster "
            + "order by estimated_recovery_time asc, shard_weight desc, shard_num asc, (errors_count + slowdowns_count) desc";

    /**
     * Similar as the default policy, which always picks the first healthy node.
     * Besides, it provides scheduler for node discovery and health check.
     */
    public static final String FIRST_ALIVE = "firstAlive";
    /**
     * Policy to pick a healthy node randomly from the list.
     */
    public static final String RANDOM = "random";
    /**
     * Policy to pick healthy node one after another based their order in the list.
     */
    public static final String ROUND_ROBIN = "roundRobin";

    /**
     * Creates policy.
     *
     * @param name policy name or a full qualified class name
     * @return non-null policy
     * @throw IllegalArgumentException when failed to create policy
     */
    static ClickHouseLoadBalancingPolicy create(String name) {
        ClickHouseLoadBalancingPolicy policy;

        if (FIRST_ALIVE.equalsIgnoreCase(name)) {
            policy = new FirstAlivePolicy();
        } else if (RANDOM.equalsIgnoreCase(name)) {
            policy = new RandomPolicy();
        } else if (ROUND_ROBIN.equalsIgnoreCase(name)) {
            policy = new RoundRobinPolicy();
        } else {
            try {
                Class<?> clazz = ClickHouseLoadBalancingPolicy.class.getClassLoader().loadClass(name);
                if (!ClickHouseLoadBalancingPolicy.class.isAssignableFrom(clazz)) {
                    throw new IllegalArgumentException(
                            ClickHouseUtils.format("Unsupported policy: class [%s] must extend [%s]", name,
                                    ClickHouseLoadBalancingPolicy.class.getName()));
                }
                policy = (ClickHouseLoadBalancingPolicy) clazz.getDeclaredConstructor().newInstance();
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Unknown policy: " + name, e);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException
                    | SecurityException e) {
                throw new IllegalArgumentException("Failed to instantiate policy: " + name, e);
            }

        }
        return policy;
    }

    /**
     * Gets or creates singleton load balancing policy.
     *
     * @param name policy name, one of {@link #FIRST_ALIVE},{@link #RANDOM} and
     *             {@link #ROUND_ROBIN}, or a fully qualified class name
     * @return non-null load balancing policy
     */
    public static ClickHouseLoadBalancingPolicy of(String name) {
        return ClickHouseChecker.isNullOrEmpty(name) ? DEFAULT
                : policies.computeIfAbsent(name.toLowerCase(Locale.ROOT), k -> create(name));
    }

    /**
     * Gets a SQL query for finding all local nodes regardless which cluster it
     * belongs to. Defaults to {@link #QUERY_GET_ALL_NODES}.
     *
     * @return non-null SQL query
     */
    protected String getQueryForAllLocalNodes() {
        return QUERY_GET_ALL_NODES;
    }

    /**
     * Gets a parameterized SQL query for finding all local nodes of a specific
     * cluster. Defaults to {@link #QUERY_GET_CLUSTER_NODES}.
     *
     * @return non-null SQL query
     */
    protected String getQueryForClusterLocalNodes() {
        return QUERY_GET_CLUSTER_NODES;
    }

    /**
     * Gets a parameterized SQL query for finding all non-local nodes in one or many
     * clusters. Defaults to {@link #QUERY_GET_OTHER_NODES}.
     *
     * @return non-null SQL query
     */
    protected String getQueryForNonLocalNodes() {
        return QUERY_GET_OTHER_NODES;
    }

    /**
     * Gets next node available in the list.
     *
     * @param manager managed nodes
     * @return next node
     */
    protected final ClickHouseNode get(ClickHouseNodes manager) {
        return get(manager, manager.getNodeSelector());
    }

    /**
     * Gets next node available in the list according to the given node selector.
     *
     * @param manager managed nodes
     * @param t       node selector
     * @return next node
     * @throws IllegalArgumentException when no node available to use
     */
    protected ClickHouseNode get(ClickHouseNodes manager, ClickHouseNodeSelector t) {
        boolean noSelector = t == null || t == ClickHouseNodeSelector.EMPTY;
        int idx = manager.index.get();
        int i = 0;
        ClickHouseNode node = null;
        for (ClickHouseNode n : manager.nodes) {
            if (noSelector || t.match(n)) {
                node = n;
            }
            if (i++ >= idx && node != null) {
                break;
            }
        }
        if (node == null) {
            for (ClickHouseNode n : manager.faultyNodes) {
                ClickHouseNode probed = n.probe();
                if (noSelector || t.match(probed)) {
                    node = probed;
                }
                if (i++ >= idx && node != null) {
                    break;
                }
            }
        }

        if (node == null) {
            throw new IllegalArgumentException(ClickHouseUtils.format(ERROR_NO_SUITABLE_NODE, manager, t));
        }
        return node;
    }

    /**
     * Sugguests a new node as replacement of the given one in order to recover from
     * the failure, which is usually a connection error.
     *
     * @param manager non-null manager for finding suitable node
     * @param server  non-null server to replace
     * @param failure non-null recoverable exception
     * @return new server, or the exact same server from input when running out of
     *         options
     */
    protected ClickHouseNode suggest(ClickHouseNodes manager, ClickHouseNode server, Throwable failure) {
        if (manager == null || server == null || !(failure instanceof ClickHouseException)) {
            return server;
        }

        ClickHouseException exp = (ClickHouseException) failure;
        // only connection errors at this point
        if (exp.getErrorCode() == ClickHouseException.ERROR_NETWORK
                || ClickHouseException.isConnectTimedOut(exp.getCause())) {
            ClickHouseNodeSelector selector = manager.getNodeSelector();
            for (ClickHouseNode node : manager.nodes) {
                if (selector.match(node) && !node.isSameEndpoint(server)) {
                    return node;
                }
            }
        }
        return server;
    }

    /**
     * Updates node status to one of {@link ClickHouseNode.Status}.
     *
     * @param manager non-null node manager
     * @param node    non-null node to update
     * @param status  non-null status of the node
     */
    protected void update(ClickHouseNodes manager, ClickHouseNode node, Status status) {
        switch (status) {
            case MANAGED:
                node.setManager(manager);
                if (!manager.nodes.contains(node) && !manager.faultyNodes.contains(node)) {
                    if (node.getProtocol() == ClickHouseProtocol.ANY) {
                        manager.faultyNodes.add(node);
                    } else {
                        manager.nodes.add(node);
                    }
                }
                break;
            case HEALTHY:
                manager.faultyNodes.remove(node);
                if (!manager.nodes.contains(node)) {
                    manager.nodes.add(node);
                }
                break;
            case FAULTY:
                manager.nodes.remove(node);
                if (!manager.faultyNodes.contains(node)) {
                    manager.faultyNodes.add(node);
                    // in case scheduled check was stopped(e.g. aborted or no faulty node)
                    manager.scheduleHealthCheck();
                }
                break;
            case STANDALONE:
                boolean removed = manager.nodes.remove(node);
                removed = manager.faultyNodes.remove(node) || removed;
                if (removed) {
                    node.setManager(null);
                }
                break;
            default:
                break;
        }
    }

    /**
     * Gets scheduled executor service for auto discovery and health check.
     *
     * @return scheduled executor service, null will turn off auto discovery and
     *         health check
     */
    protected ScheduledExecutorService getScheduler() {
        return ClickHouseDataStreamFactory.getInstance().getScheduler();
    }

    /**
     * Schedules the {@code task} to run only when {@code current} run has been
     * completed/cancelled or does not exist.
     *
     * @param current  current task, which could be null or in running status
     * @param task     scheduled task to run
     * @param interval interval between each run
     * @return future object representing the scheduled task, could be null when no
     *         scheduler available({@link #getScheduler()} returns null)
     */
    protected ScheduledFuture<?> schedule(ScheduledFuture<?> current, Runnable task, long interval) {
        ScheduledExecutorService scheduler = getScheduler();
        if (scheduler == null || task == null || (current != null && !current.isDone() && !current.isCancelled())) {
            return null;
        }
        return interval < 1L ? scheduler.schedule(task, 0L, TimeUnit.MILLISECONDS)
                : scheduler.scheduleAtFixedRate(task, 0L, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public String toString() {
        return new StringBuilder(110).append("ClickHouseLoadBalancingPolicy [name=").append(getClass().getSimpleName())
                .append(", scheduler=").append(getScheduler() != null).append("]@").append(hashCode()).toString();
    }
}
