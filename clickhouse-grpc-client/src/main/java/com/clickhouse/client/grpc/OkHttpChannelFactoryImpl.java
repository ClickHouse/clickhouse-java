package com.clickhouse.client.grpc;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import io.grpc.ManagedChannelBuilder;
import io.grpc.okhttp.OkHttpChannelBuilder;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.grpc.config.ClickHouseGrpcClientOption;

final class OkHttpChannelFactoryImpl extends ClickHouseGrpcChannelFactory {
    private final OkHttpChannelBuilder builder;

    OkHttpChannelFactoryImpl(ClickHouseConfig config, ClickHouseNode server) {
        super(config, server);

        builder = OkHttpChannelBuilder.forAddress(server.getHost(), server.getPort());

        int flowControlWindow = (int) config.getOption(ClickHouseGrpcClientOption.FLOW_CONTROL_WINDOW);
        if (flowControlWindow > 0) {
            builder.flowControlWindow(flowControlWindow);
        }
    }

    @Override
    protected ManagedChannelBuilder<?> getChannelBuilder() {
        return builder;
    }

    @Override
    protected void setupSsl() {
        if (!config.isSsl()) {
            builder.usePlaintext();
        } else {
            try {
                builder.useTransportSecurity().sslSocketFactory(ClickHouseSslContextProvider.getProvider()
                        .getSslContext(SSLContext.class, config).get().getSocketFactory());
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
