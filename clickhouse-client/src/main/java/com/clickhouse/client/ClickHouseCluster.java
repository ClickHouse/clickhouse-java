package com.clickhouse.client;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import com.clickhouse.client.ClickHouseNode.Status;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

/**
 * List of {@link ClickHouseNode}. By default, all nodes are considered as
 * healthy. When connection issue happened, corresponding node will be moved to
 * unhealthy list, where a background thread will validate its status time from
 * time and eventually bring them back to healthy list if no issue.
 *
 * <p>
 * When a node's protocol is {@link ClickHouseProtocol#ANY}, this class will
 * also try to probe the protocol by sending a packet to the port and analyze
 * response from server.
 */
public class ClickHouseCluster implements Function<ClickHouseNodeSelector, ClickHouseNode>, Serializable {
    private static final long serialVersionUID = 8684489015067906319L;

    private static final Logger log = LoggerFactory.getLogger(ClickHouseCluster.class);

    private static final String PARAM_NODES = "nodes";

    /**
     * Enum of load balancing policy.
     */
    public enum LoadBalancingPolicy {
        ROUND_ROBIN, // nothing fancy
        PICK_FIRST // stick with the first healthy node
    }

    /**
     * Builder class for creating {@link ClickHouseCluster}.
     */
    public static class Builder {
        private final List<ClickHouseNode> nodes;
        private LoadBalancingPolicy lbPolicy;

        private Builder() {
            nodes = new LinkedList<>();
        }

        /**
         * Add node.
         *
         * @param node node to be added
         * @return this builder
         */
        protected Builder addNode(ClickHouseNode node) {
            if (!nodes.contains(ClickHouseChecker.nonNull(node, "node"))) {
                nodes.add(node);
            }

            return this;
        }

        /**
         * Add nodes.
         *
         * @param node node to be added
         * @param more more nodes to be added
         * @return this builder
         */
        public Builder addNodes(ClickHouseNode node, ClickHouseNode... more) {
            addNode(node);

            if (more != null) {
                for (ClickHouseNode n : more) {
                    addNode(n);
                }
            }

            return this;
        }

        /**
         * Add nodes.
         *
         * @param nodes list of nodes to be added
         * @return this builder
         */
        public Builder addNodes(Collection<ClickHouseNode> nodes) {
            for (ClickHouseNode node : ClickHouseChecker.nonNull(nodes, PARAM_NODES)) {
                addNode(node);
            }

            return this;
        }

        /**
         * Merge nodes from the given cluster.
         *
         * @param cluster list of nodes to merge
         * @return this builder
         */
        public Builder merge(ClickHouseCluster cluster) {
            for (ClickHouseNode node : ClickHouseChecker.nonNull(cluster, "cluster").nodes) {
                addNode(node);
            }

            return this;
        }

        public Builder withLbPolicy(LoadBalancingPolicy policy) {
            this.lbPolicy = policy;
            return this;
        }

        /**
         * Build the cluster object.
         *
         * @return cluster
         */
        public ClickHouseCluster build() {
            return new ClickHouseCluster(lbPolicy, nodes);
        }
    }

    /**
     * Get builder for building cluster object.
     *
     * @return builder for building cluster object
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Same as {@link #probe(ClickHouseNode, int)} except it uses default
     * timeout(3000 milliseconds).
     *
     * @param node non-null target node
     * @return probed node, which may or may not be the same as given node
     */
    public static ClickHouseNode probe(ClickHouseNode node) {
        return probe(node, 3000);
    }

    /**
     * Probe the node by resolving its DNS and detect protocol as needed.
     *
     * @param node    non-null target node
     * @param timeout timeout in milliseconds
     * @return probed node, which may or may not be the same as given node
     */
    public static ClickHouseNode probe(ClickHouseNode node, int timeout) {
        ClickHouseDnsResolver resolver = ClickHouseDnsResolver.getInstance();
        if (ClickHouseChecker.nonNull(node, "node").getProtocol() == ClickHouseProtocol.ANY) {
            InetSocketAddress address = resolver != null
                    ? resolver.resolve(ClickHouseProtocol.ANY, node.getHost(), node.getPort())
                    : new InetSocketAddress(node.getHost(), node.getPort());

            ClickHouseProtocol p = ClickHouseProtocol.HTTP;
            // TODO needs a better way so that we can detect PostgreSQL port as well
            try (Socket client = new Socket()) {
                client.setKeepAlive(false);
                client.connect(address, timeout);
                client.setSoTimeout(timeout);
                OutputStream out = client.getOutputStream();
                out.write("GET /ping HTTP/1.1\r\n\r\n".getBytes());
                out.flush();
                byte[] buf = new byte[12]; // HTTP/1.x xxx
                if (client.getInputStream().read(buf) == buf.length) {
                    if (buf[0] == 0) {
                        p = ClickHouseProtocol.GRPC;
                    } else if (buf[3] == 0) {
                        p = ClickHouseProtocol.MYSQL;
                    } else if (buf[0] == 72 && buf[9] == 52) {
                        p = ClickHouseProtocol.NATIVE;
                    }
                }
            } catch (IOException e) {
                log.debug("Failed to probe: " + address, e);
            }

            node = ClickHouseNode.builder(node).port(p).build();
        }

        return node;
    }

    /**
     * Create cluster object from list of nodes.
     *
     * @param nodes list of nodes
     * @return cluster object
     */
    public static ClickHouseCluster of(ClickHouseNode... nodes) {
        return new ClickHouseCluster(null, nodes);
    }

    /**
     * Create cluster object from list of nodes.
     *
     * @param nodes list of nodes
     * @return cluster object
     */
    public static ClickHouseCluster of(Collection<ClickHouseNode> nodes) {
        return new ClickHouseCluster(null, nodes);
    }

    protected static void handleUncaughtException(Thread r, Throwable t) {
        log.warn("Exception caught from thread: " + r, t);
    }

    private final AtomicBoolean checking;
    private final transient ScheduledExecutorService scheduledExecutor;
    private final List<ClickHouseNode> unhealthyNodes;

    private final AtomicInteger index;
    private final List<ClickHouseNode> nodes;
    private final LoadBalancingPolicy lbPolicy;

    /**
     * Constructor cluster object using list of nodes.
     *
     * @param policy load balancing policy
     * @param nodes  list of nodes
     */
    protected ClickHouseCluster(LoadBalancingPolicy policy, ClickHouseNode... nodes) {
        this(policy, Arrays.asList(ClickHouseChecker.nonNull(nodes, PARAM_NODES)));
    }

    /**
     * Constructor cluster object using list of nodes.
     *
     * @param policy load balancing policy
     * @param nodes  list of nodes
     */
    protected ClickHouseCluster(LoadBalancingPolicy policy, Collection<ClickHouseNode> nodes) {
        this.lbPolicy = policy == null ? LoadBalancingPolicy.ROUND_ROBIN : policy;

        this.checking = new AtomicBoolean(false);
        this.index = new AtomicInteger(0);

        int size = ClickHouseChecker.nonNull(nodes, PARAM_NODES).size();

        this.nodes = Collections.synchronizedList(new ArrayList<>(size));
        this.unhealthyNodes = Collections.synchronizedList(new ArrayList<>(size));

        // should make it a static member
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, ClickHouseCluster.class.getSimpleName());
                thread.setDaemon(true);
                thread.setUncaughtExceptionHandler(ClickHouseCluster::handleUncaughtException);
                return thread;
            }
        });

        for (ClickHouseNode node : nodes) {
            if (node == null) {
                continue;
            }

            probe(node).setManager(this::update);
        }
    }

    protected synchronized void update(ClickHouseNode node, Status status) {
        switch (status) {
            case UNMANAGED:
                nodes.remove(node);
                unhealthyNodes.remove(node);
                break;
            case MANAGED:
            case HEALTHY:
                unhealthyNodes.remove(node);
                if (!nodes.contains(node)) {
                    nodes.add(node);
                }
                break;
            case UNHEALTHY:
                nodes.remove(node);
                if (!unhealthyNodes.contains(node)) {
                    unhealthyNodes.add(node);

                    if (!checking.get()) {
                        this.scheduledExecutor.execute(this::check);
                    }
                }
                break;
            default:
                break;
        }
    }

    protected void check() {
        if (checking.compareAndSet(false, true)) {
            return;
        }

        // detect flaky node and check it in a different way(less frequency)
        try {
            boolean passed = true;
            for (int i = 0; i < unhealthyNodes.size(); i++) {
                ClickHouseNode node = unhealthyNodes.get(i);
                if (ClickHouseClient.test(node, 5000)) { // another configuration?
                    update(node, Status.HEALTHY);
                } else {
                    passed = false;
                }
            }

            if (!passed) {
                this.scheduledExecutor.schedule(this::check, 3L, TimeUnit.SECONDS);
            }
        } finally {
            checking.set(false);
        }
    }

    /**
     * Get load balancing policy.
     *
     * @return load balancing policy
     */
    public LoadBalancingPolicy getLbPolicy() {
        return lbPolicy;
    }

    /**
     * Check if the cluster has any node available for access.
     *
     * @return if there's at least one node is available for access
     */
    public boolean hasNode() {
        return !this.nodes.isEmpty();
    }

    /**
     * Get all available nodes in the cluster.
     *
     * @return unmodifible list of nodes
     */
    public List<ClickHouseNode> getAvailableNodes() {
        return Collections.unmodifiableList(nodes);
    }

    @Override
    public synchronized ClickHouseNode apply(ClickHouseNodeSelector t) {
        boolean noSelector = t == null || t == ClickHouseNodeSelector.EMPTY;

        if (nodes.isEmpty()) {
            // TODO wait until timed out?
            throw new IllegalArgumentException("No healthy node available");
        }

        if (lbPolicy == LoadBalancingPolicy.PICK_FIRST) {
            return nodes.get(0);
        }

        int idx = index.get();
        ClickHouseNode matched = null;
        for (int i = idx; i < nodes.size(); i++) {
            ClickHouseNode node = nodes.get(i);
            if (noSelector || t.match(node)) {
                matched = node;
                index.compareAndSet(idx, i + 1);
                break;
            }
        }

        if (matched == null && idx > 0) {
            for (int i = 0; i < Math.min(idx, nodes.size()); i++) {
                ClickHouseNode node = nodes.get(i);
                if (noSelector || t.match(node)) {
                    matched = node;
                    index.compareAndSet(idx, i + 1);
                    break;
                }
            }
        }

        if (matched == null) {
            throw new IllegalArgumentException(ClickHouseUtils
                    .format("No healthy node found from a list of %d(index=%d)", nodes.size(), index.get()));
        }

        return matched;
    }
}
