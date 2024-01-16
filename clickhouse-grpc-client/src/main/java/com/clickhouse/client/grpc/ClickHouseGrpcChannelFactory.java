package com.clickhouse.client.grpc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.clickhouse.client.config.ClickHouseProxyType;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ProxiedSocketAddress;
import io.grpc.ProxyDetector;
import io.grpc.Status;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.grpc.config.ClickHouseGrpcOption;
import com.clickhouse.client.grpc.impl.ClickHouseGrpc;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
@Deprecated
public abstract class ClickHouseGrpcChannelFactory {
    static class NoProxyDetector implements ProxyDetector {
        static final NoProxyDetector INSTANCE = new NoProxyDetector();

        private NoProxyDetector() {
        }

        @Override
        public ProxiedSocketAddress proxyFor(SocketAddress arg0) throws IOException {
            return null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClickHouseGrpcChannelFactory.class);

    private static final String PROP_NAME = "name";
    private static final String PROP_SERVICE = "service";
    private static final String PROP_METHOD = "method";
    private static final String PROP_METHOD_CONFIG = "methodConfig";
    private static final String PROP_RETRY_POLICY = "retryPolicy";
    private static final String PROP_MAX_ATTEMPTS = "maxAttempts";

    private static final String serviceName = ClickHouseGrpc.SERVICE_NAME;
    private static final String methodName = ClickHouseGrpc.getExecuteQueryWithStreamIOMethod().getBareMethodName();

    // you can override this with ~/.clickhouse/grpc-config.json
    // more details at:
    // https://github.com/grpc/grpc-proto/blob/master/grpc/service_config/service_config.proto
    private static final Map<String, ?> defaultServiceConfig;

    static {
        Map<String, Object> name = new HashMap<>();
        name.put(PROP_SERVICE, serviceName);
        name.put(PROP_METHOD, methodName);

        Map<String, Object> retryPolicy = new HashMap<>();
        retryPolicy.put(PROP_MAX_ATTEMPTS, 5D);
        retryPolicy.put("initialBackoff", "0.5s");
        retryPolicy.put("maxBackoff", "30s");
        retryPolicy.put("backoffMultiplier", 2D);
        retryPolicy.put("retryableStatusCodes", Collections.singletonList(Status.UNAVAILABLE.getCode().name()));

        Map<String, Object> methodConfig = new HashMap<>();
        methodConfig.put(PROP_NAME, Collections.singletonList(name));
        methodConfig.put(PROP_RETRY_POLICY, retryPolicy);

        Map<String, Object> config = new HashMap<>();
        config.put(PROP_METHOD_CONFIG, Collections.singletonList(methodConfig));

        defaultServiceConfig = Collections.unmodifiableMap(config);
    }

    public static ClickHouseGrpcChannelFactory getFactory(ClickHouseConfig config, ClickHouseNode server) {
        if (!config.hasOption(ClickHouseGrpcOption.USE_OKHTTP)) { // default
            ClickHouseGrpcChannelFactory factory = null;
            try {
                factory = new NettyChannelFactoryImpl(config, server);
            } catch (NoClassDefFoundError e) {
                factory = new OkHttpChannelFactoryImpl(config, server);
            }
            return factory;
        }

        return config.getBoolOption(ClickHouseGrpcOption.USE_OKHTTP)
                ? new OkHttpChannelFactoryImpl(config, server)
                : new NettyChannelFactoryImpl(config, server);
    }

    protected final ClickHouseConfig config;
    protected final ClickHouseNode server;

    protected ClickHouseGrpcChannelFactory(ClickHouseConfig config, ClickHouseNode server) {
        this.config = ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME);
        this.server = ClickHouseChecker.nonNull(server, "server");
    }

    protected Map<String, ?> getDefaultServiceConfig() {
        Map<String, ?> config = defaultServiceConfig;
        try {
            config = new Gson().fromJson(new JsonReader(new InputStreamReader(
                    ClickHouseUtils.getFileInputStream("grpc-config.json"), StandardCharsets.UTF_8)), Map.class);
        } catch (FileNotFoundException e) {
            log.debug("Use default service config due to: %s", e.getMessage());
        } catch (Exception e) {
            log.debug("Failed to load service config", e);
        }

        return config;
    }

    protected abstract ManagedChannelBuilder<?> getChannelBuilder();

    protected abstract String getDefaultUserAgent();

    @SuppressWarnings("unchecked")
    protected void setupRetry() {
        ManagedChannelBuilder<?> builder = getChannelBuilder();

        if (config.getRetry() > 0) {
            Map<String, ?> serviceConfig = getDefaultServiceConfig();
            int maxAttempts = -1;
            Object value = serviceConfig.get(PROP_METHOD_CONFIG);
            if (value instanceof List) {
                for (Object o : ((List<?>) value)) {
                    if (!(o instanceof Map)) {
                        continue;
                    }

                    Map<String, ?> methodConfig = (Map<String, ?>) o;
                    value = methodConfig.get(PROP_NAME);
                    boolean matched = value instanceof List;
                    if (matched) {
                        matched = false;
                        for (Object n : ((List<?>) value)) {
                            if (!(n instanceof Map)) {
                                continue;
                            }

                            Map<String, ?> m = (Map<String, ?>) n;
                            Object v = m.get(PROP_SERVICE);
                            if (v != null && !serviceName.equals(v)) {
                                continue;
                            }
                            v = m.get(PROP_METHOD);
                            if (v != null && !methodName.equals(v)) {
                                continue;
                            }

                            matched = true;
                            break;
                        }
                    }

                    if (!matched) {
                        continue;
                    }

                    value = methodConfig.get(PROP_RETRY_POLICY);
                    if (value instanceof Map) {
                        Map<String, ?> m = (Map<String, ?>) value;
                        value = m.get(PROP_MAX_ATTEMPTS);
                        if (value instanceof Number) {
                            maxAttempts = ((Number) value).intValue();
                        }
                    }
                }
            }

            builder.defaultServiceConfig(serviceConfig).enableRetry();
            if (maxAttempts > 0) {
                builder.maxRetryAttempts(maxAttempts);
            }
        } else {
            builder.disableRetry();
        }
    }

    protected abstract void setupSsl();

    protected abstract void setupTimeout();

    protected void setupMisc() {
        ManagedChannelBuilder<?> builder = getChannelBuilder();
        if (config.getBoolOption(ClickHouseGrpcOption.USE_FULL_STREAM_DECOMPRESSION)) {
            builder.enableFullStreamDecompression();
        }

        if (config.getProxyType() == ClickHouseProxyType.DIRECT) {
            builder.proxyDetector(NoProxyDetector.INSTANCE);
        }
        // TODO add interceptor to customize retry
        builder.maxInboundMessageSize(config.getIntOption(ClickHouseGrpcOption.MAX_INBOUND_MESSAGE_SIZE))
                .maxInboundMetadataSize(config.getIntOption(ClickHouseGrpcOption.MAX_INBOUND_METADATA_SIZE));
    }

    public ManagedChannel create() {
        log.debug("Establishing channel to [%s]", server);

        setupRetry();
        setupSsl();
        setupMisc();

        ManagedChannelBuilder<?> builder = getChannelBuilder();
        String userAgent = config.getClientName();
        if (ClickHouseClientOption.CLIENT_NAME.getDefaultValue().equals(userAgent)) {
            userAgent = getDefaultUserAgent();

            String name = config.getProductName();
            if (!ClickHouseClientOption.PRODUCT_NAME.getDefaultValue().equals(name)) {
                userAgent = new StringBuilder(name).append(userAgent.substring(userAgent.indexOf('/'))).toString();
            }
        }
        ManagedChannel c = builder.userAgent(userAgent).build();
        log.debug("Channel established: %s", c);
        return c;
    }
}
