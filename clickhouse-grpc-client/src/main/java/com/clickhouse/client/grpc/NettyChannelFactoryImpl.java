package com.clickhouse.client.grpc;

import java.io.FileNotFoundException;
import javax.net.ssl.SSLException;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2SecurityUtil;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.netty.shaded.io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.client.grpc.config.ClickHouseGrpcOption;

final class NettyChannelFactoryImpl extends ClickHouseGrpcChannelFactory {
    private static final String USER_AGENT = ClickHouseClientOption.buildUserAgent(null, "gRPC-Netty");

    private final NettyChannelBuilder builder;

    NettyChannelFactoryImpl(ClickHouseConfig config, ClickHouseNode server) {
        super(config, server);

        builder = NettyChannelBuilder.forAddress(server.getHost(), server.getPort());

        int flowControlWindow = config.getIntOption(ClickHouseGrpcOption.FLOW_CONTROL_WINDOW);
        if (flowControlWindow > 0) {
            builder.flowControlWindow(flowControlWindow); // what about initialFlowControlWindow?
        }
    }

    protected SslContext getSslContext() throws SSLException {
        SslContextBuilder builder = SslContextBuilder.forClient();

        ClickHouseSslMode sslMode = config.getSslMode();
        if (sslMode == ClickHouseSslMode.NONE) {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        } else if (sslMode == ClickHouseSslMode.STRICT) {
            String sslRootCert = config.getSslRootCert();
            if (!ClickHouseChecker.isNullOrEmpty(sslRootCert)) {
                try {
                    builder.trustManager(ClickHouseUtils.getFileInputStream(sslRootCert));
                } catch (FileNotFoundException e) {
                    throw new SSLException("Failed to setup trust manager using given root certificate", e);
                }
            }

            String sslCert = config.getSslCert();
            String sslKey = config.getSslKey();
            if (!ClickHouseChecker.isNullOrEmpty(sslCert) && !ClickHouseChecker.isNullOrEmpty(sslKey)) {
                try {
                    builder.keyManager(ClickHouseUtils.getFileInputStream(sslCert),
                            ClickHouseUtils.getFileInputStream(sslKey));
                } catch (FileNotFoundException e) {
                    throw new SSLException("Failed to setup key manager using given certificate and key", e);
                }
            }
        }

        builder.sslProvider(SslProvider.JDK).ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE);

        return builder.build();
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
            SslContext sslContext;
            try {
                sslContext = getSslContext();
            } catch (SSLException e) {
                throw new IllegalStateException("Failed to build ssl context", e);
            }

            builder.useTransportSecurity().sslContext(sslContext);
        }
    }

    @Override
    protected void setupTimeout() {
        builder.withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectionTimeout());
        // .withOption(ChannelOption.SO_TIMEOUT, config.getSocketTimeout());
    }
}
