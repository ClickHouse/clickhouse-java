package com.clickhouse.client.grpc;

import java.io.IOException;
import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.clickhouse.client.AbstractClient;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseCompression;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.grpc.config.ClickHouseGrpcOption;
import com.clickhouse.client.grpc.impl.ClickHouseGrpc;
import com.clickhouse.client.grpc.impl.ExternalTable;
import com.clickhouse.client.grpc.impl.NameAndType;
import com.clickhouse.client.grpc.impl.QueryInfo;
import com.clickhouse.client.grpc.impl.Result;
import com.clickhouse.client.grpc.impl.QueryInfo.Builder;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

public class ClickHouseGrpcClient extends AbstractClient<ManagedChannel> {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseGrpcClient.class);

    static final List<ClickHouseProtocol> SUPPORTED = Collections.singletonList(ClickHouseProtocol.GRPC);

    protected static QueryInfo convert(ClickHouseRequest<?> request) {
        ClickHouseConfig config = request.getConfig();
        ClickHouseNode server = request.getServer();
        ClickHouseCredentials credentials = server.getCredentials(config);

        Builder builder = QueryInfo.newBuilder();
        String database = server.getDatabase(config);
        if (!ClickHouseChecker.isNullOrEmpty(database)) {
            builder.setDatabase(server.getDatabase(config));
        }
        builder.setUserName(credentials.getUserName())
                .setPassword(credentials.getPassword()).setOutputFormat(request.getFormat().name());

        Optional<String> optionalValue = request.getSessionId();
        if (optionalValue.isPresent()) {
            builder.setSessionId(optionalValue.get());
        }
        if (config.isSessionCheck()) {
            builder.setSessionCheck(true);
        }
        if (config.getSessionTimeout() > 0) {
            builder.setSessionTimeout(config.getSessionTimeout());
        }

        optionalValue = request.getQueryId();
        if (optionalValue.isPresent()) {
            builder.setQueryId(optionalValue.get());
        }

        ClickHouseCompression outputCompression = config.getResponseCompressAlgorithm();
        builder.setOutputCompressionType(outputCompression.encoding());

        // builder.setNextQueryInfo(true);
        for (Entry<String, Serializable> s : request.getSettings().entrySet()) {
            builder.putSettings(s.getKey(), String.valueOf(s.getValue()));
        }

        ClickHouseCompression inputCompression = config.getRequestCompressAlgorithm();
        Optional<ClickHouseInputStream> input = request.getInputStream();
        if (input.isPresent()) {
            builder.setInputCompressionType(inputCompression.encoding());
            try {
                builder.setInputData(ByteString.readFrom(input.get()));
            } catch (IOException e) {
                throw new CompletionException(ClickHouseException.of(e, server));
            }
        }

        List<ClickHouseExternalTable> externalTables = request.getExternalTables();
        if (!externalTables.isEmpty()) {
            for (ClickHouseExternalTable external : externalTables) {
                ExternalTable.Builder b = ExternalTable.newBuilder().setName(external.getName());
                for (ClickHouseColumn c : ClickHouseColumn.parse(external.getStructure())) {
                    b.addColumns(NameAndType.newBuilder().setName(c.getColumnName()).setType(c.getOriginalTypeName())
                            .build());
                }
                b.setCompressionType(inputCompression.encoding());
                if (external.getFormat() != null) {
                    b.setFormat(external.getFormat().name());
                }

                try {
                    builder.addExternalTables(b.setData(ByteString.readFrom(external.getContent())).build());
                } catch (IOException e) {
                    throw new CompletionException(ClickHouseException.of(e, server));
                }
            }
        }

        List<String> stmts = request.getStatements(false);
        int size = stmts.size();
        String sql;
        if (size == 0) {
            throw new IllegalArgumentException("At least one SQL statement is required for execution");
        } else if (size == 1) {
            sql = stmts.get(0);
        } else { // consolidate statements into one
            if (!builder.getSessionCheck()) {
                builder.setSessionCheck(true);
            }

            if (ClickHouseChecker.isNullOrEmpty(builder.getSessionId())) {
                builder.setSessionId(request.getManager().createSessionId());
            }

            // builder.getSessionTimeout()
            StringBuilder sb = new StringBuilder();
            for (String s : stmts) {
                sb.append(s).append(';').append('\n');
            }
            sql = sb.toString();
        }

        log.debug("Query: %s", sql);

        return builder.setQuery(sql).build();
    }

    @Override
    protected boolean checkHealth(ClickHouseNode server, int timeout) {
        return true;
    }

    @Override
    protected void closeConnection(ManagedChannel connection, boolean force) {
        if (!force) {
            connection.shutdown();
        } else {
            connection.shutdownNow();
        }
    }

    @Override
    protected Collection<ClickHouseProtocol> getSupportedProtocols() {
        return SUPPORTED;
    }

    @Override
    protected ManagedChannel newConnection(ManagedChannel connection, ClickHouseNode server,
            ClickHouseRequest<?> request) {
        if (connection != null) {
            closeConnection(connection, false);
        }

        return ClickHouseGrpcChannelFactory.getFactory(request.getConfig(), server).create();
    }

    protected void fill(ClickHouseRequest<?> request, StreamObserver<QueryInfo> observer) {
        try {
            observer.onNext(convert(request));
        } finally {
            observer.onCompleted();
        }
    }

    @Override
    protected Object[] getAsyncExecArguments(ClickHouseRequest<?> sealedRequest) {
        // reuse stub?
        ClickHouseGrpc.ClickHouseStub stub = ClickHouseGrpc.newStub(getConnection(sealedRequest));

        final ClickHouseStreamObserver responseObserver = new ClickHouseStreamObserver(sealedRequest.getConfig(),
                sealedRequest.getServer(), sealedRequest.getOutputStream().orElse(null));
        final StreamObserver<QueryInfo> requestObserver = stub.executeQueryWithStreamIO(responseObserver);

        if (sealedRequest.hasInputStream()) {
            getExecutor().execute(() -> fill(sealedRequest, requestObserver));
        } else {
            fill(sealedRequest, requestObserver);
        }

        return new Object[] { requestObserver, responseObserver };
    }

    @Override
    protected ClickHouseResponse sendAsync(ClickHouseRequest<?> sealedRequest, Object... args)
            throws ClickHouseException, IOException {
        StreamObserver<QueryInfo> requestObserver = (StreamObserver<QueryInfo>) args[0];
        ClickHouseStreamObserver responseObserver = (ClickHouseStreamObserver) args[1];

        ClickHouseConfig config = sealedRequest.getConfig();
        int timeout = config.getConnectionTimeout() / 1000
                + Math.max(config.getSocketTimeout() / 1000, config.getMaxExecutionTime());
        try {
            if (!responseObserver.await(timeout, TimeUnit.SECONDS)) {
                if (!Context.current().withCancellation().cancel(new StatusException(Status.CANCELLED))) {
                    requestObserver.onError(new StatusException(Status.CANCELLED));
                }
                throw new SocketTimeoutException(
                        ClickHouseUtils.format("Timed out after waiting for %d %s", timeout, TimeUnit.SECONDS));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ClickHouseException.of(e, sealedRequest.getServer());
        }

        ClickHouseResponse response = new ClickHouseGrpcResponse(sealedRequest.getConfig(),
                sealedRequest.getSettings(), responseObserver);
        Throwable cause = responseObserver.getError();
        if (cause != null) {
            throw ClickHouseException.of(cause, sealedRequest.getServer());
        }
        return response;
    }

    @Override
    protected ClickHouseResponse send(ClickHouseRequest<?> sealedRequest) throws ClickHouseException, IOException {
        final ManagedChannel channel = getConnection(sealedRequest);

        ClickHouseGrpc.ClickHouseBlockingStub stub = ClickHouseGrpc.newBlockingStub(channel);

        Result result = stub.executeQuery(convert(sealedRequest));

        ClickHouseResponse response = new ClickHouseGrpcResponse(sealedRequest.getConfig(),
                sealedRequest.getSettings(), result);
        if (result.hasException()) {
            throw new ClickHouseException(result.getException().getCode(), result.getException().getDisplayText(),
                    sealedRequest.getServer());
        }

        return response;
    }

    @Override
    public boolean accept(ClickHouseProtocol protocol) {
        return ClickHouseProtocol.GRPC == protocol || super.accept(protocol);
    }

    @Override
    public Class<? extends ClickHouseOption> getOptionClass() {
        return ClickHouseGrpcOption.class;
    }
}
