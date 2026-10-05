package com.clickhouse.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.time.Duration;
import java.util.List;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Starts the ClickHouse containers integration tests connect to.
 * <p>
 * {@code single} starts one server. {@code cluster} starts three servers behind nginx.
 * The process that calls {@link #getHost()} always sees one host and the published service ports.
 */
public abstract class ClickHouseContainerStarter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseContainerStarter.class);

    static final String FRONTEND_ALIAS = "clickhouse";
    static final String CONTAINER_TMP_DIR = "/tmp";
    private static final String CUSTOM_DIRECTORY = "/custom";
    private static final String CONFIG_RESOURCE = "containers/clickhouse-server";

    protected final ClickHouseTestEnvironment environment;
    protected final Network network;

    ClickHouseContainerStarter(ClickHouseTestEnvironment environment, Network network) {
        this.environment = environment;
        this.network = network;
    }

    /**
     * @return a starter for {@code single} or {@code cluster}, or null when the mode is {@code none}
     */
    static ClickHouseContainerStarter create(ClickHouseTestEnvironment environment, Network network) {
        switch (environment.getContainerMode()) {
            case SINGLE:
                return new SingleClickHouseContainerStarter(environment, network);
            case CLUSTER:
                return new ClusterClickHouseContainerStarter(environment, network);
            case NONE:
                return null;
            default:
                throw new IllegalStateException("Unsupported container mode: " + environment.getContainerMode());
        }
    }

    public abstract void start();

    public abstract void stop();

    public abstract boolean isRunning();

    public abstract String getHost();

    public abstract int getMappedPort(int port);

    public abstract Container.ExecResult execInContainer(String... command) throws Exception;

    /**
     * Cluster name when these servers are replicas of one shard, otherwise null.
     */
    public String replicatedClusterName() {
        return null;
    }

    /**
     * Runs a query inside every ClickHouse server.
     */
    public final boolean execQueryOnServers(String sql, String user, String password) throws Exception {
        boolean succeeded = true;
        for (GenericContainer<?> server : clickHouseServers()) {
            Container.ExecResult result = server.execInContainer("clickhouse-client",
                    "-u", user, "--password", password, "--query", sql);
            if (result.getExitCode() != 0) {
                LOGGER.error("query failed: stderr={}, stdout={}", result.getStderr(), result.getStdout());
                succeeded = false;
            }
        }
        return succeeded;
    }

    protected abstract List<GenericContainer<?>> clickHouseServers();

    public abstract GenericContainer<?> getEndpointContainer();

    public Network getNetwork() {
        return network;
    }

    protected final GenericContainer<?> newClickHouseServer(String networkAlias, String containerName) {
        return newClickHouseServer(networkAlias, containerName, null);
    }

    /**
     * @param replicaName replica macro for a cluster node, or null for the single-server config
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected final GenericContainer<?> newClickHouseServer(String networkAlias, String containerName,
            String replicaName) {
        String additionalPackages = environment.getAdditionalPackages();
        GenericContainer container = ((additionalPackages == null)
                ? new GenericContainer<>(environment.getImageRef())
                : new GenericContainer<>(new ImageFromDockerfile().withDockerfileFromBuilder(builder -> builder
                        .from(environment.getImageRef())
                        .run("apt-get update && apt-get install -y " + additionalPackages))))
                .withCreateContainerCmdModifier(command -> {
                    command.withEntrypoint("/bin/sh");
                    if (containerName != null) {
                        command.withName(containerName);
                    }
                })
                .withCommand("-c", String.format("chmod +x %1$s/patch && %1$s/patch", CUSTOM_DIRECTORY))
                .withEnv("TZ", environment.getTimezone())
                .withExposedPorts(
                        ClickHouseProtocol.GRPC.getDefaultPort(),
                        ClickHouseProtocol.HTTP.getDefaultPort(),
                        ClickHouseProtocol.HTTP.getDefaultSecurePort(),
                        ClickHouseProtocol.MYSQL.getDefaultPort(),
                        ClickHouseProtocol.TCP.getDefaultPort(),
                        ClickHouseProtocol.TCP.getDefaultSecurePort(),
                        ClickHouseProtocol.POSTGRESQL.getDefaultPort())
                .withClasspathResourceMapping(CONFIG_RESOURCE, CUSTOM_DIRECTORY, BindMode.READ_ONLY)
                .withClasspathResourceMapping("empty.csv", "/var/lib/clickhouse/user_files/empty.csv",
                        BindMode.READ_ONLY)
                .withFileSystemBind(System.getProperty("java.io.tmpdir"), CONTAINER_TMP_DIR, BindMode.READ_WRITE)
                .withNetwork(network)
                .withNetworkAliases(networkAlias);
        if (replicaName != null) {
            container = container
                    .withEnv("CLICKHOUSE_REPLICA", replicaName)
                    .withEnv("CLICKHOUSE_INTERSERVER_HOST", networkAlias)
                    .withClasspathResourceMapping("containers/clickhouse-cluster/config.d/custom_config.xml",
                            CUSTOM_DIRECTORY + "/config.d/custom_config.xml", BindMode.READ_ONLY)
                    .withClasspathResourceMapping("containers/clickhouse-cluster/users.d/cluster_quorum.xml",
                            CUSTOM_DIRECTORY + "/users.d/cluster_quorum.xml", BindMode.READ_ONLY);
        }
        return container.waitingFor(Wait.forHttp("/ping").forPort(ClickHouseProtocol.HTTP.getDefaultPort())
                .forStatusCode(200).withStartupTimeout(Duration.of(600, SECONDS)));
    }

    protected final GenericContainer<?> newNginx(String containerName, String configResource) {
        return new GenericContainer<>("nginx:1.27-alpine")
                .withCreateContainerCmdModifier(command -> {
                    if (containerName != null) {
                        command.withName(containerName);
                    }
                })
                .withNetwork(network)
                .withNetworkAliases(FRONTEND_ALIAS)
                .withClasspathResourceMapping(configResource, "/etc/nginx/nginx.conf", BindMode.READ_ONLY)
                .withExposedPorts(
                        ClickHouseProtocol.GRPC.getDefaultPort(),
                        ClickHouseProtocol.HTTP.getDefaultPort(),
                        ClickHouseProtocol.HTTP.getDefaultSecurePort(),
                        ClickHouseProtocol.MYSQL.getDefaultPort(),
                        ClickHouseProtocol.TCP.getDefaultPort(),
                        ClickHouseProtocol.TCP.getDefaultSecurePort(),
                        ClickHouseProtocol.POSTGRESQL.getDefaultPort())
                .waitingFor(Wait.forHttp("/ping").forPort(ClickHouseProtocol.HTTP.getDefaultPort())
                        .forStatusCode(200).withStartupTimeout(Duration.of(600, SECONDS)));
    }

    protected static void stopQuietly(GenericContainer<?> container) {
        if (container == null) {
            return;
        }
        try {
            container.stop();
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to stop test container", e);
        }
    }
}
