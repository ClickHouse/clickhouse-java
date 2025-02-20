package com.clickhouse.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.clickhouse.client.ClickHouseNode.Status;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * A generic node manager for managing one or more nodes which may or may not
 * belong to same cluster. It maintains two lists - one for healthy nodes and
 * the other for faulty ones. Behind the scene, there's background thread(s) for
 * discovering nodes and health check. Besides,
 * {@link ClickHouseLoadBalancingPolicy} is used to pickup available node and
 * moving node between lists according to its status.
 */
@Deprecated
public class ClickHouseNodes implements ClickHouseNodeManager {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseNodes.class);
    private static final long serialVersionUID = 4931904980127690349L;

    private static final Map<String, ClickHouseNodes> cache = Collections.synchronizedMap(new WeakHashMap<>());
    private static final char[] separators = new char[] { '/', '?', '#' };

    /**
     * Creates list of managed {@link ClickHouseNode} for load balancing and
     * fail-over.
     *
     * @param endpoints      non-empty URIs separated by comma
     * @param defaultOptions default options
     * @return non-null list of nodes
     */
    static ClickHouseNodes create(String endpoints, Map<?, ?> defaultOptions) {
        int index = endpoints.indexOf(ClickHouseNode.SCHEME_DELIMITER);
        String defaultProtocol = ((ClickHouseProtocol) ClickHouseDefaults.PROTOCOL
                .getEffectiveDefaultValue()).name();
        if (index > 0) {
            defaultProtocol = endpoints.substring(0, index);
            if (ClickHouseProtocol.fromUriScheme(defaultProtocol) == ClickHouseProtocol.ANY) {
                defaultProtocol = ClickHouseProtocol.ANY.name();
                index = 0;
            } else {
                index += 3;
            }
        } else {
            index = 0;
        }
        

        String defaultParams = "";
        Set<String> list = new LinkedHashSet<>();
        char stopChar = ',';
        for (int i = index, len = endpoints.length(); i < len; i++) {
            char ch = endpoints.charAt(i);
            if (ch == ',' || Character.isWhitespace(ch)) {
                index++;
                continue;
            } else if (ch == '/' || ch == '?' || ch == '#') {
                defaultParams = endpoints.substring(i);
                break;
            }
            switch (ch) {
                case '(':
                    stopChar = ')';
                    index++;
                    break;
                case '{':
                    stopChar = '}';
                    index++;
                    break;
                default:
                    break;
            }

            int endIndex = i;
            // parsing host name
            for (int j = i + 1; j < len; j++) {
                ch = endpoints.charAt(j);
                if (ch == stopChar || Character.isWhitespace(ch)) {
                    endIndex = j;
                    break;
                } else if ( stopChar == ',' && ( ch == '/' || ch == '?' || ch == '#') ) {
                    break;
                }
            }

            if (endIndex > i) {
                // add host name to list
                list.add(endpoints.substring(index, endIndex).trim());
                i = endIndex;
                index = endIndex + 1;
                stopChar = ',';
            } else {
                String last = endpoints.substring(index);
                int sepIndex = last.indexOf(ClickHouseNode.SCHEME_DELIMITER);
                int startIndex = sepIndex < 0 ? 0 : sepIndex + 3;
                for (char spec : separators) {
                    sepIndex = last.indexOf(spec, startIndex);
                    if (sepIndex > 0) {
                        break;
                    }
                }
                if (sepIndex > 0) {
                    defaultParams = last.substring(sepIndex);
                    list.add(last.substring(0, sepIndex).trim());
                } else {
                    list.add(last.trim());
                }
                break;
            }
        }
        list.remove("");
        if (list.isEmpty()) {
            throw new IllegalArgumentException(
                    "No valid URI found, please try 'http://localhost:8123' or simply 'localhost:8123' if you don't know protocol");
        }

        if (list.size() == 1 && defaultParams.isEmpty()) {
            endpoints = new StringBuilder().append(defaultProtocol).append(ClickHouseNode.SCHEME_DELIMITER)
                    .append(list.iterator().next()).toString();
            return new ClickHouseNodes(Collections.singletonList(ClickHouseNode.of(endpoints, defaultOptions)));
        }

        ClickHouseNode defaultNode = ClickHouseNode.of(defaultProtocol + "://localhost" + defaultParams,
                defaultOptions);
        List<ClickHouseNode> nodes = new LinkedList<>();
        for (String uri : list) {
            nodes.add(ClickHouseNode.of(uri, defaultNode));
        }
        return new ClickHouseNodes(nodes, defaultNode);
    }

    /**
     * Picks node from {@code source} and put into {@code target}.
     *
     * @param source    list of nodes to pick
     * @param selector  node selector for filtering out nodes
     * @param groupSize maximum number of nodes allowed
     * @return non-null selected nodes
     */
    static List<ClickHouseNode> pickNodes(Collection<ClickHouseNode> source, ClickHouseNodeSelector selector,
            int groupSize) {
        boolean hasSelector = selector != null && selector != ClickHouseNodeSelector.EMPTY;
        final List<ClickHouseNode> list;
        if (groupSize < 1) {
            if (hasSelector) {
                list = new LinkedList<>();
                for (ClickHouseNode node : source) {
                    if (selector.match(node)) {
                        list.add(node);
                    }
                }
            } else {
                list = new ArrayList<>(source);
            }
        } else {
            list = new ArrayList<>(groupSize);
            int count = 0;
            for (ClickHouseNode node : source) {
                if (!hasSelector || selector.match(node)) {
                    list.add(node);
                    count++;
                }
                if (count >= groupSize) {
                    break;
                }
            }
        }
        return list;
    }

    /**
     * Picks node from {@code source} and put into {@code target}.
     *
     * @param source      list of nodes to pick
     * @param selector    node selector for filtering out nodes
     * @param target      container for selected nodes
     * @param groupSize   maximum number of nodes allowed
     * @param currentTime current timestamp for filtering
     */
    static void pickNodes(Collection<ClickHouseNode> source, ClickHouseNodeSelector selector,
            Set<ClickHouseNode> target, int groupSize, long currentTime) {
        boolean hasSelector = selector != null && selector != ClickHouseNodeSelector.EMPTY;
        int count = target.size();
        for (ClickHouseNode node : source) {
            if (!hasSelector || selector.match(node)) {
                int interval = node.config.getNodeCheckInterval();
                if (interval < 1 || (currentTime - node.lastUpdateTime.get()) >= interval) {
                    target.add(node);
                    count++;
                }
            }
            if (groupSize > 0 && count >= groupSize) {
                break;
            }
        }
    }

    /**
     * Build unique key according to the given base URI and options for caching.
     *
     * @param uri     non-null URI
     * @param options options
     * @return non-empty unique key for caching
     */
    public static String buildCacheKey(String uri, Map<?, ?> options) {
        if (uri == null) {
            throw new IllegalArgumentException("Non-null URI required");
        } else if ((uri = uri.trim()).isEmpty()) {
            throw new IllegalArgumentException("Non-blank URI required");
        }
        if (options == null || options.isEmpty()) {
            return uri;
        }

        SortedMap<Object, Object> sorted;
        if (options instanceof SortedMap) {
            sorted = (SortedMap<Object, Object>) options;
        } else {
            sorted = new TreeMap<>();
            for (Entry<?, ?> entry : options.entrySet()) {
                if (entry.getKey() != null) {
                    sorted.put(entry.getKey(), entry.getValue());
                }
            }
        }

        StringBuilder builder = new StringBuilder(uri).append('|');
        for (Entry<Object, Object> entry : sorted.entrySet()) {
            if (entry.getKey() != null) {
                builder.append(entry.getKey()).append('=').append(entry.getValue()).append(',');
            }
        }
        return builder.toString();
    }

    /**
     * Gets or creates list of managed {@link ClickHouseNode} for load balancing
     * and fail-over.
     *
     * @param endpoints non-empty URIs separated by comma
     * @return non-null list of nodes
     */
    public static ClickHouseNodes of(String endpoints) {
        return of(endpoints, Collections.emptyMap());
    }

    /**
     * Gets or creates list of managed {@link ClickHouseNode} for load balancing
     * and fail-over.
     *
     * @param endpoints non-empty URIs separated by comma
     * @param options   default options
     * @return non-null list of nodes
     */
    public static ClickHouseNodes of(String endpoints, Map<?, ?> options) {
        return cache.computeIfAbsent(buildCacheKey(ClickHouseChecker.nonEmpty(endpoints, "Endpoints"), options),
                k -> create(endpoints, options));
    }

    /**
     * Gets or creates list of managed {@link ClickHouseNode} for load balancing
     * and fail-over. Since the list will be cached in a {@link WeakHashMap}, as
     * long as you hold strong reference to the {@code cacheKey}, same combination
     * of {@code endpoints} and {@code options} will be always mapped to the exact
     * same list.
     *
     * @param cacheKey  non-empty cache key
     * @param endpoints non-empty URIs separated by comma
     * @param options   default options
     * @return non-null list of nodes
     */
    public static ClickHouseNodes of(String cacheKey, String endpoints, Map<?, ?> options) {
        // TODO discover endpoints from a URL or custom service, for examples:
        // discover://(smb://fs1/ch-list.txt),(smb://fs1/ch-dc.json)
        // discover:com.mycompany.integration.clickhouse.Endpoints
        if (ClickHouseChecker.isNullOrEmpty(cacheKey) || ClickHouseChecker.isNullOrEmpty(endpoints)) {
            throw new IllegalArgumentException("Non-empty cache key and endpoints are required");
        }
        return cache.computeIfAbsent(cacheKey, k -> create(endpoints, options));
    }

    /**
     * Flag for exclusive health check.
     */
    protected final AtomicBoolean checking;
    /**
     * Index for retrieving next node.
     */
    protected final AtomicInteger index;
    /**
     * Lock for read and write {@code nodes} and {@code faultyNodes}.
     */
    protected final ReentrantReadWriteLock lock;
    /**
     * Maximum number of nodes can be used for operation at a time.
     */
    protected final int groupSize;
    /**
     * List of healthy nodes.
     */
    protected final LinkedList<ClickHouseNode> nodes;
    /**
     * List of faulty nodes.
     */
    protected final LinkedList<ClickHouseNode> faultyNodes;
    /**
     * Reference holding future of scheduled discovery.
     */
    protected final AtomicReference<ScheduledFuture<?>> discoveryFuture;
    /**
     * Reference holding future of scheduled health check.
     */
    protected final AtomicReference<ScheduledFuture<?>> healthCheckFuture;
    /**
     * Load balancing policy.
     */
    protected final ClickHouseLoadBalancingPolicy policy;
    /**
     * Load balancing tags for filtering out nodes.
     */
    protected final ClickHouseNodeSelector selector;
    /**
     * Flag indicating whether it's single node or not.
     */
    protected final boolean singleNode;
    /**
     * Template node.
     */
    protected final ClickHouseNode template;

    /**
     * Constructor for testing purpose.
     *
     * @param nodes non-empty list of nodes
     */
    ClickHouseNodes(Collection<ClickHouseNode> nodes) {
        this(nodes, nodes.iterator().next());
    }

    /**
     * Default constructor.
     *
     * @param nodes    non-empty list of nodes
     * @param template non-null template node
     */
    protected ClickHouseNodes(Collection<ClickHouseNode> nodes, ClickHouseNode template) {
        this.checking = new AtomicBoolean(false);
        this.index = new AtomicInteger(0);
        this.lock = new ReentrantReadWriteLock();
        this.nodes = new LinkedList<>(); // usually just healthy nodes
        this.faultyNodes = new LinkedList<>();

        this.discoveryFuture = new AtomicReference<>(null);
        this.healthCheckFuture = new AtomicReference<>(null);

        this.template = template;
        this.groupSize = template.config.getIntOption(ClickHouseClientOption.NODE_GROUP_SIZE);

        Set<String> tags = new LinkedHashSet<>();
        ClickHouseNode.parseTags(template.config.getStrOption(ClickHouseClientOption.LOAD_BALANCING_TAGS), tags);
        this.policy = ClickHouseLoadBalancingPolicy
                .of(template.config.getStrOption(ClickHouseClientOption.LOAD_BALANCING_POLICY));
        this.selector = tags.isEmpty() ? ClickHouseNodeSelector.EMPTY : ClickHouseNodeSelector.of(null, tags);
        boolean autoDiscovery = false;
        for (ClickHouseNode n : nodes) {
            autoDiscovery = autoDiscovery || n.config.isAutoDiscovery();
            n.setManager(this);
        }
        if (autoDiscovery) {
            this.singleNode = false;
            this.discoveryFuture.getAndUpdate(current -> policy.schedule(current, ClickHouseNodes.this::discover,
                    template.config.getIntOption(ClickHouseClientOption.NODE_DISCOVERY_INTERVAL)));
        } else {
            this.singleNode = nodes.size() == 1;
        }
        this.healthCheckFuture.getAndUpdate(current -> policy.schedule(current, ClickHouseNodes.this::check,
                template.config.getIntOption(ClickHouseClientOption.HEALTH_CHECK_INTERVAL)));
    }

    protected void queryClusterNodes(Collection<ClickHouseNode> seeds, Collection<ClickHouseNode> allNodes,
            Collection<ClickHouseNode> newHealthyNodes, Collection<ClickHouseNode> newFaultyNodes,
            Collection<ClickHouseNode> useless) {
        for (ClickHouseNode node : seeds) {
            ClickHouseNode server = null;
            try {
                server = node.probe();
            } catch (Exception e) {
                // ignore
            }
            if (server == null) {
                newFaultyNodes.add(node);
                continue;
            } else if (!server.equals(node)) {
                allNodes.add(server);
                useless.add(node);
            }

            try (ClickHouseClient client = ClickHouseClient.builder().agent(false)
                    .nodeSelector(ClickHouseNodeSelector.of(server.getProtocol())).build()) {
                ClickHouseRequest<?> request = client.read(server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
                String clusterName = server.getCluster();
                Set<String> clusters = new LinkedHashSet<>();
                if (!ClickHouseChecker.isNullOrEmpty(clusterName)) {
                    clusters.add(clusterName);
                }
                boolean isAddress = true;
                boolean proceed = false;
                try (ClickHouseResponse response = request
                        .query(clusters.isEmpty() ? policy.getQueryForAllLocalNodes()
                                : policy.getQueryForClusterLocalNodes())
                        .params(ClickHouseValues.convertToQuotedString(clusterName))
                        .executeAndWait()) {
                    for (ClickHouseRecord r : response.records()) {
                        int idx = 0;
                        String cluster = r.getValue(idx++).asString();
                        clusters.add(cluster);
                        String addr = r.getValue(idx++).asString();
                        String name = r.getValue(idx++).asString();
                        if (!(isAddress = server.getHost().equals(addr)) && !server.getHost().equals(name)) {
                            log.warn(
                                    "Auto discovery may not work as no host_name and host_address in system.clusters matched with %s",
                                    server);
                            isAddress = true; // fall back to host_address
                        }
                        ClickHouseNode discovered = ClickHouseNode.builder(server).cluster(cluster)
                                .replica(r.getValue(idx++).asInteger())
                                .shard(r.getValue(idx++).asInteger(), r.getValue(idx++).asInteger()).build();
                        if (!discovered.equals(server)) {
                            allNodes.remove(server);
                            allNodes.add(discovered);
                            newHealthyNodes.add(discovered);
                            useless.add(server);
                        }
                        proceed = true;
                    }
                } catch (Exception e) {
                    log.warn("Failed to query system.clusters using %s, due to: %s", server, e.getMessage());
                    if (e.getCause() instanceof IOException) {
                        useless.add(server);
                        continue;
                    }
                }

                if (!proceed) {
                    continue;
                }

                String query = policy.getQueryForNonLocalNodes();
                int limit = template.config.getIntOption(ClickHouseClientOption.NODE_DISCOVERY_LIMIT);
                if (limit > 0) {
                    query = query + " limit " + limit;
                }
                try (ClickHouseResponse response = request.query(query)
                        .params(isAddress ? "host_address" : "host_name",
                                ClickHouseValues.convertToSqlExpression(clusters))
                        .executeAndWait()) {
                    for (ClickHouseRecord r : response.records()) {
                        int idx = 0;
                        ClickHouseNode n = ClickHouseNode.builder(server).cluster(r.getValue(idx++).asString())
                                .addOption(ClickHouseClientOption.AUTO_DISCOVERY.getKey(), "false")
                                .host(r.getValue(idx++).asString()).replica(r.getValue(idx++).asInteger())
                                .shard(r.getValue(idx++).asInteger(), r.getValue(idx++).asInteger()).build();
                        allNodes.add(n);
                        // we have seed nodes so here we can be more defensive
                        newFaultyNodes.add(n);
                    }
                } catch (Exception e) {
                    log.warn("Failed to query system.clusters using %s", server, e);
                    newHealthyNodes.remove(server);
                    newFaultyNodes.add(server);
                }
            }
        }
    }

    /**
     * Gets next node available.
     *
     * @return non-null node
     */
    protected ClickHouseNode get() {
        return apply(selector);
    }

    /**
     * Checks whether it's single node or not.
     *
     * @return true if it's single node; false otherwise
     */
    public boolean isSingleNode() {
        return singleNode;
    }

    @Override
    public ClickHouseNode apply(ClickHouseNodeSelector t) {
        lock.readLock().lock();
        try {
            return policy.get(this, t);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ClickHouseNode suggestNode(ClickHouseNode server, Throwable failure) {
        lock.readLock().lock();
        try {
            return policy.suggest(this, server, failure);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void update(ClickHouseNode node, Status status) {
        lock.writeLock().lock();
        try {
            if (node.config.getNodeCheckInterval() > 0) {
                node.lastUpdateTime.set(System.currentTimeMillis());
            }
            policy.update(this, node, status);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks (faulty) node status.
     */
    public void check() {
        // exclusive access
        if (!checking.compareAndSet(false, true)) {
            return;
        }

        Set<ClickHouseNode> list = new LinkedHashSet<>();
        long currentTime = System.currentTimeMillis();
        boolean checkAll = template.config.getBoolOption(ClickHouseClientOption.CHECK_ALL_NODES);
        int numberOfFaultyNodes = -1;
        lock.readLock().lock();
        // TODO:
        // 1) minimize the list;
        // 2) detect flaky node and check it again later in a less frequent way
        try {
            pickNodes(faultyNodes, selector, list, groupSize, currentTime);
            numberOfFaultyNodes = list.size();
            if (checkAll) {
                pickNodes(nodes, selector, list, groupSize, currentTime);
            }
        } finally {
            checking.set(false);
            lock.readLock().unlock();
        }

        boolean hasFaultyNode = false;
        int count = 0;
        try {
            for (ClickHouseNode node : list) {
                ClickHouseNode n = node.probe();
                // probe is faster than ping but it cannot tell if the server works or not
                boolean isAlive = false;
                try (ClickHouseClient client = ClickHouseClient.builder().agent(false).config(n.config)
                        .nodeSelector(ClickHouseNodeSelector.of(n.getProtocol())).build()) {
                    isAlive = client.ping(n, n.config.getConnectionTimeout());
                } catch (Exception e) {
                    // ignore
                }
                if (!n.equals(node)) {
                    update(n, Status.MANAGED);
                    update(node, Status.STANDALONE);
                }

                boolean wasFaultyBefore = numberOfFaultyNodes == -1 || count < numberOfFaultyNodes;
                if (isAlive) {
                    if (wasFaultyBefore) {
                        update(n, Status.HEALTHY);
                    }
                } else {
                    hasFaultyNode = true;
                    if (!wasFaultyBefore) {
                        update(n, Status.FAULTY);
                    }
                }
                count++;
            }
        } catch (Exception e) {
            log.warn("Unexpected error occurred when checking node status", e);
        } finally {
            if (checkAll || hasFaultyNode) {
                scheduleHealthCheck();
            }
        }
    }

    /**
     * Discovers nodes in the same cluster by querying against
     * {@code system.clusters} table.
     */
    public void discover() {
        // FIXME too aggressive for a large cluster, NODE_GROUP_SIZE should be
        // considered
        Set<ClickHouseNode> allNodes = new LinkedHashSet<>();
        Set<ClickHouseNode> seeds = new LinkedHashSet<>();
        lock.readLock().lock();
        try {
            // discover nodes freely only when auto discovery is enabled
            for (ClickHouseNode node : nodes) {
                if (node.config.isAutoDiscovery()) {
                    seeds.add(node);
                } else {
                    allNodes.add(node);
                }
            }

            // seeds without protocol
            for (ClickHouseNode node : faultyNodes) {
                if (node.config.isAutoDiscovery()) {
                    seeds.add(node);
                } else {
                    allNodes.add(node);
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        if (seeds.isEmpty()) {
            return;
        }

        Set<ClickHouseNode> newHealthyNodes = new LinkedHashSet<>();
        Set<ClickHouseNode> newFaultyNodes = new LinkedHashSet<>();
        Set<ClickHouseNode> useless = new LinkedHashSet<>();

        queryClusterNodes(seeds, allNodes, newHealthyNodes, newFaultyNodes, useless);

        lock.readLock().lock();
        try {
            // check if there's any new node or decommission as needed
            for (ClickHouseNode n : nodes) {
                if (!allNodes.remove(n)) {
                    useless.add(n);
                }
                newHealthyNodes.remove(n); // just in case
            }
            for (ClickHouseNode n : faultyNodes) {
                if (!allNodes.remove(n)) {
                    useless.add(n);
                }
                newFaultyNodes.remove(n); // just in case
            }
        } finally {
            lock.readLock().unlock();
        }

        boolean noUselessNode = useless.isEmpty();
        if (allNodes.isEmpty() && noUselessNode) {
            return;
        }

        for (ClickHouseNode n : allNodes) { // all new nodes
            update(n, Status.MANAGED);
        }
        for (ClickHouseNode n : newHealthyNodes) {
            update(n, Status.HEALTHY);
        }
        for (ClickHouseNode n : newFaultyNodes) {
            update(n, Status.FAULTY);
        }
        for (ClickHouseNode n : useless) {
            update(n, Status.STANDALONE);
        }

        if (!noUselessNode) {
            scheduleHealthCheck();
        }
    }

    public ClickHouseNode getTemplate() {
        return template;
    }

    @Override
    public final List<ClickHouseNode> getNodes() {
        return getNodes(selector, groupSize);
    }

    @Override
    public List<ClickHouseNode> getNodes(ClickHouseNodeSelector selector, int groupSize) {
        lock.readLock().lock();
        try {
            return pickNodes(nodes, selector, groupSize);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public final List<ClickHouseNode> getFaultyNodes() {
        return getFaultyNodes(selector, groupSize);
    }

    @Override
    public List<ClickHouseNode> getFaultyNodes(ClickHouseNodeSelector selector, int groupSize) {
        lock.readLock().lock();
        try {
            return pickNodes(faultyNodes, selector, groupSize);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ClickHouseLoadBalancingPolicy getPolicy() {
        return policy;
    }

    @Override
    public ClickHouseNodeSelector getNodeSelector() {
        return selector;
    }

    @Override
    public Optional<ScheduledFuture<?>> scheduleDiscovery() {
        return Optional.ofNullable(discoveryFuture.getAndUpdate(current -> {
            return policy.schedule(current, ClickHouseNodes.this::discover, 0L);
        }));
    }

    @Override
    public Optional<ScheduledFuture<?>> scheduleHealthCheck() {
        return Optional.ofNullable(healthCheckFuture.getAndUpdate(current -> {
            return policy.schedule(current, ClickHouseNodes.this::check, 0L);
        }));
    }

    @Override
    public void shutdown() {
        for (ScheduledFuture<?> future : new ScheduledFuture<?>[] {
                discoveryFuture.get(), healthCheckFuture.get()
        }) {
            if (future != null && !future.isDone() && !future.isCancelled()) {
                future.cancel(true);
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + checking.hashCode();
        result = prime * result + index.hashCode();
        result = prime * result + policy.hashCode();
        result = prime * result + selector.hashCode();
        result = prime * result + nodes.hashCode();
        result = prime * result + faultyNodes.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseNodes other = (ClickHouseNodes) obj;
        // FIXME ignore order when comparing node lists
        return policy.equals(other.policy) && selector.equals(other.selector) && nodes.equals(other.nodes)
                && faultyNodes.equals(other.faultyNodes);
    }

    @Override
    public String toString() {
        return new StringBuilder("ClickHouseNodes [checking=").append(checking.get()).append(", index=")
                .append(index.get()).append(", lock=r").append(lock.getReadHoldCount()).append('w')
                .append(lock.getWriteHoldCount()).append(", nodes=").append(nodes.size()).append(", faulty=")
                .append(faultyNodes.size()).append(", policy=").append(policy.getClass().getSimpleName())
                .append(", tags=").append(selector.getPreferredTags()).append("]@").append(hashCode()).toString();
    }
}
