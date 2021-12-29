package com.clickhouse.client.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
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
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseOption;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.client.grpc.config.ClickHouseGrpcOption;
import com.clickhouse.client.grpc.impl.ClickHouseGrpc;
import com.clickhouse.client.grpc.impl.Compression;
import com.clickhouse.client.grpc.impl.CompressionAlgorithm;
import com.clickhouse.client.grpc.impl.CompressionLevel;
import com.clickhouse.client.grpc.impl.ExternalTable;
import com.clickhouse.client.grpc.impl.NameAndType;
import com.clickhouse.client.grpc.impl.QueryInfo;
import com.clickhouse.client.grpc.impl.Result;
import com.clickhouse.client.grpc.impl.QueryInfo.Builder;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

public class ClickHouseGrpcClient extends AbstractClient<ManagedChannel> {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseGrpcClient.class);

    private static final Compression COMPRESSION_DISABLED = Compression.newBuilder()
            .setAlgorithm(CompressionAlgorithm.NO_COMPRESSION).setLevel(CompressionLevel.COMPRESSION_NONE).build();

    protected static String getRequestEncoding(ClickHouseConfig config) {
        if (config.isDecompressClientRequet()) {
            return ClickHouseCompression.NONE.encoding();
        }

        String encoding = ClickHouseCompression.GZIP.encoding();
        switch (config.getDecompressAlgorithmForClientRequest()) {
            case GZIP:
                break;
            default:
                log.warn("Unsupported algorithm [%s], change to [%s]", config.getDecompressAlgorithmForClientRequest(),
                        encoding);
                break;
        }

        return encoding;
    }

    protected static Compression getResultCompression(ClickHouseConfig config) {
        if (!config.isCompressServerResponse()) {
            return COMPRESSION_DISABLED;
        }

        Compression.Builder builder = Compression.newBuilder();
        CompressionAlgorithm algorithm = CompressionAlgorithm.GZIP;
        CompressionLevel level = CompressionLevel.COMPRESSION_MEDIUM;
        switch (config.getDecompressAlgorithmForClientRequest()) {
            case NONE:
                algorithm = CompressionAlgorithm.NO_COMPRESSION;
                break;
            case DEFLATE:
                algorithm = CompressionAlgorithm.DEFLATE;
                break;
            case GZIP:
                break;
            // case STREAM_GZIP:
            default:
                log.warn("Unsupported algorithm [%s], change to [%s]", config.getDecompressAlgorithmForClientRequest(),
                        algorithm);
                break;
        }

        int l = config.getDecompressLevelForClientRequest();
        if (l <= 0) {
            level = CompressionLevel.COMPRESSION_NONE;
        } else if (l < 3) {
            level = CompressionLevel.COMPRESSION_LOW;
        } else if (l < 7) {
            level = CompressionLevel.COMPRESSION_MEDIUM;
        } else {
            level = CompressionLevel.COMPRESSION_HIGH;
        }

        return builder.setAlgorithm(algorithm).setLevel(level).build();
    }

    protected static QueryInfo convert(ClickHouseNode server, ClickHouseRequest<?> request) {
        ClickHouseConfig config = request.getConfig();
        ClickHouseCredentials credentials = server.getCredentials(config);

        Builder builder = QueryInfo.newBuilder();
        builder.setDatabase(server.getDatabase(config)).setUserName(credentials.getUserName())
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

        builder.setResultCompression(getResultCompression(config));

        // builder.setNextQueryInfo(true);
        for (Entry<String, Object> s : request.getSettings().entrySet()) {
            builder.putSettings(s.getKey(), String.valueOf(s.getValue()));
        }

        Optional<InputStream> input = request.getInputStream();
        if (input.isPresent()) {
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
                builder.setSessionId(UUID.randomUUID().toString());
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
    protected void closeConnection(ManagedChannel connection, boolean force) {
        if (!force) {
            connection.shutdown();
        } else {
            connection.shutdownNow();
        }
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
            observer.onNext(convert(getServer(), request));
        } finally {
            observer.onCompleted();
        }
    }

    @Override
    public boolean accept(ClickHouseProtocol protocol) {
        return ClickHouseProtocol.GRPC == protocol || super.accept(protocol);
    }

    protected CompletableFuture<ClickHouseResponse> executeAsync(ClickHouseRequest<?> sealedRequest,
            ManagedChannel channel, ClickHouseNode server) {
        // reuse stub?
        ClickHouseGrpc.ClickHouseStub stub = ClickHouseGrpc.newStub(channel);
        stub.withCompression(getRequestEncoding(sealedRequest.getConfig()));

        final ClickHouseStreamObserver responseObserver = new ClickHouseStreamObserver(sealedRequest.getConfig(),
                server);
        final StreamObserver<QueryInfo> requestObserver = stub.executeQueryWithStreamIO(responseObserver);

        if (sealedRequest.hasInputStream()) {
            getExecutor().execute(() -> fill(sealedRequest, requestObserver));
        } else {
            fill(sealedRequest, requestObserver);
        }

        // return new ClickHouseGrpcFuture(server, sealedRequest, requestObserver,
        // responseObserver);
        return CompletableFuture.supplyAsync(() -> {
            int timeout = sealedRequest.getConfig().getConnectionTimeout() / 1000
                    + sealedRequest.getConfig().getMaxExecutionTime();
            try {
                if (!responseObserver.await(timeout, TimeUnit.SECONDS)) {
                    if (!Context.current().withCancellation().cancel(new StatusException(Status.CANCELLED))) {
                        requestObserver.onError(new StatusException(Status.CANCELLED));
                    }
                    throw new CompletionException(
                            ClickHouseUtils.format("Timed out after waiting for %d %s", timeout, TimeUnit.SECONDS),
                            null);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(ClickHouseException.of(e, server));
            }

            try {
                ClickHouseResponse response = new ClickHouseGrpcResponse(sealedRequest.getConfig(),
                        sealedRequest.getSettings(), responseObserver);
                Throwable cause = responseObserver.getError();
                if (cause != null) {
                    throw new CompletionException(ClickHouseException.of(cause, server));
                }
                return response;
            } catch (IOException e) {
                throw new CompletionException(ClickHouseException.of(e, server));
            }
        }, getExecutor());
    }

    protected CompletableFuture<ClickHouseResponse> executeSync(ClickHouseRequest<?> sealedRequest,
            ManagedChannel channel, ClickHouseNode server) {
        ClickHouseGrpc.ClickHouseBlockingStub stub = ClickHouseGrpc.newBlockingStub(channel);
        stub.withCompression(getRequestEncoding(sealedRequest.getConfig()));

        // TODO not as elegant as ClickHouseImmediateFuture :<
        try {
            Result result = stub.executeQuery(convert(server, sealedRequest));

            ClickHouseResponse response = new ClickHouseGrpcResponse(sealedRequest.getConfig(),
                    sealedRequest.getSettings(), result);

            return result.hasException()
                    ? failedResponse(new ClickHouseException(result.getException().getCode(),
                            result.getException().getDisplayText(), server))
                    : CompletableFuture.completedFuture(response);
        } catch (IOException e) {
            throw new CompletionException(ClickHouseException.of(e, server));
        }
    }

    @Override
    public CompletableFuture<ClickHouseResponse> execute(ClickHouseRequest<?> request) {
        // sealedRequest is an immutable copy of the original request
        final ClickHouseRequest<?> sealedRequest = request.seal();
        final ManagedChannel c = getConnection(sealedRequest);
        final ClickHouseNode s = getServer();

        return sealedRequest.getConfig().isAsync() ? executeAsync(sealedRequest, c, s)
                : executeSync(sealedRequest, c, s);
    }

    @Override
    public Class<? extends ClickHouseOption> getOptionClass() {
        return ClickHouseGrpcOption.class;
    }
}
