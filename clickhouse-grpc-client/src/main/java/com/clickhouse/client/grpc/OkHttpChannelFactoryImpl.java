package com.clickhouse.client.grpc;

import java.util.Optional;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import io.grpc.ManagedChannelBuilder;
import io.grpc.okhttp.OkHttpChannelBuilder;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.grpc.config.ClickHouseGrpcOption;
@Deprecated
final class OkHttpChannelFactoryImpl extends ClickHouseGrpcChannelFactory {
    private static final String USER_AGENT = ClickHouseClientOption.buildUserAgent(null, "gRPC-OkHttp");

    private final OkHttpChannelBuilder builder;

    OkHttpChannelFactoryImpl(ClickHouseConfig config, ClickHouseNode server) {
        super(config, server);

        builder = OkHttpChannelBuilder.forAddress(server.getHost(), server.getPort());

        int flowControlWindow = config.getIntOption(ClickHouseGrpcOption.FLOW_CONTROL_WINDOW);
        if (flowControlWindow > 0) {
            builder.flowControlWindow(flowControlWindow);
        }
    }

    @Override
    protected ManagedChannelBuilder<?> getChannelBuilder() {
        return builder;
    }

    @Override
    protected String getDefaultUserAgent() {
        return USER_AGENT;
    }

    @Override
    protected void setupSsl() {
        if (!config.isSsl()) {
            builder.usePlaintext();
        } else {
            try {
                Optional<SSLContext> sslContext = ClickHouseSslContextProvider.getProvider()
                        .getSslContext(SSLContext.class, config);
                if (sslContext.isPresent()) {
                    builder.useTransportSecurity().sslSocketFactory(sslContext.get().getSocketFactory());
                }
            } catch (SSLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    protected void setupTimeout() {
        // custom socket factory?
    }
}
