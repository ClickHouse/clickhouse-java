package com.clickhouse.client.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.grpc.impl.ClickHouseGrpc;
import com.clickhouse.client.grpc.impl.ExternalTable;
import com.clickhouse.client.grpc.impl.NameAndType;
import com.clickhouse.client.grpc.impl.QueryInfo;
import com.clickhouse.client.grpc.impl.Result;
import com.clickhouse.client.grpc.impl.QueryInfo.Builder;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseDeferredValue;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
@Deprecated
public class ClickHouseGrpcClientImpl extends AbstractClient<ManagedChannel> {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseGrpcClientImpl.class);

    static final List<ClickHouseProtocol> SUPPORTED = Collections.singletonList(ClickHouseProtocol.GRPC);

    static ClickHouseInputStream getInput(ClickHouseConfig config, InputStream input, Runnable postCloseAction) {
        final ClickHouseInputStream in;
        if (config.getResponseCompressAlgorithm() == ClickHouseCompression.LZ4) {
            in = ClickHouseInputStream.of(ClickHouseDeferredValue.of(() -> {
                try {
                    return FramedLZ4Utils.wrap(input);
                } catch (IOException e) {
                    return input;
                } catch (ExceptionInInitializerError | NoClassDefFoundError e) {
                    throw new UnsupportedOperationException(
                            "Framed LZ4 is not supported. Please disable compression(compress=0) or add Apache Common Compress library to the classpath.");
                }
            }), config.getReadBufferSize(), postCloseAction);
        } else {
            in = ClickHouseInputStream.wrap(null, input, config.getReadBufferSize(),
                    config.getResponseCompressAlgorithm(), config.getResponseCompressLevel(), postCloseAction);
        }
        return in;
    }

    static ClickHouseOutputStream getOutput(ClickHouseConfig config, OutputStream output, Runnable postCloseAction) {
        final ClickHouseOutputStream out;
        if (config.getRequestCompressAlgorithm() == ClickHouseCompression.LZ4) {
            out = ClickHouseOutputStream.of(ClickHouseDeferredValue.of(() -> {
                try {
                    return FramedLZ4Utils.wrap(output);
                } catch (IOException e) {
                    return output;
                } catch (ExceptionInInitializerError | NoClassDefFoundError e) {
                    throw new UnsupportedOperationException(
                            "Framed LZ4 library not found. Please disable decompression(decompress=0) or add Apache Common Compress library to the classpath.");
                }
            }), config.getWriteBufferSize(), postCloseAction);
        } else {
            out = ClickHouseOutputStream.of(output, config.getWriteBufferSize(),
                    config.getRequestCompressAlgorithm(), config.getRequestCompressLevel(), postCloseAction);
        }
        return out;
    }

    protected static ClickHouseInputStream getCompressedInputStream(ClickHouseConfig config,
            ClickHouseInputStream input) {
        if (!config.isRequestCompressed() || input.getUnderlyingStream().hasInput()) {
            return input;
        }

        final int bufferSize = config.getWriteBufferSize();
        final ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance() // NOSONAR
                .createPipedOutputStream(bufferSize, 0, config.getSocketTimeout());
        final ClickHouseInputStream compressedInput = stream.getInputStream();

        ClickHouseClient.submit(() -> {
            try (ClickHouseInputStream in = input; ClickHouseOutputStream out = getOutput(config, stream, null)) {
                in.pipe(out);
            } catch (Exception e) {
                log.warn("Failed to pipe data", e);
            }
        });
        return compressedInput;
    }

    protected static QueryInfo getChunkedInputData(ClickHouseNode server, ClickHouseInputStream input, byte[] bytes) {
        QueryInfo.Builder builder = QueryInfo.newBuilder();

        try {
            int read = input.read(bytes);
            // FIXME get rid of byte array copying
            ByteString bs = read > 0 ? ByteString.copyFrom(bytes, 0, read) : ByteString.empty();
            builder.setInputData(bs);
            builder.setNextQueryInfo(read == bytes.length && input.available() > 0);
        } catch (IOException e) {
            throw new CompletionException(ClickHouseException.of(e, server));
        }

        return builder.build();
    }

    protected static QueryInfo convert(ClickHouseRequest<?> request, boolean streaming) {
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
        if (outputCompression != ClickHouseCompression.NONE
                && config.hasOption(ClickHouseClientOption.COMPRESS_LEVEL)) {
            builder.setOutputCompressionLevel(config.getResponseCompressLevel());
        }

        for (Entry<String, Serializable> s : request.getSettings().entrySet()) {
            builder.putSettings(s.getKey(), String.valueOf(s.getValue()));
        }

        List<ClickHouseExternalTable> externalTables = request.getExternalTables();
        if (!externalTables.isEmpty()) {
            for (ClickHouseExternalTable external : externalTables) {
                ExternalTable.Builder b = ExternalTable.newBuilder().setName(external.getName());
                for (ClickHouseColumn c : ClickHouseColumn.parse(external.getStructure())) {
                    b.addColumns(NameAndType.newBuilder().setName(c.getColumnName()).setType(c.getOriginalTypeName())
                            .build());
                }
                // doesn't matter because ClickHouse does not support compressed ExternalTable
                // b.setCompressionType(inputCompression.encoding());
                if (external.getFormat() != null) {
                    b.setFormat(external.getFormat().name());
                }

                try {
                    // FIXME chunking is not supported for ExternalTable
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

        // let server to decide if transport compression should be used
        // usually it should not be used due to limited options and conflict with input
        // data compression
        // builder.setTransportCompressionType("none");
        // builder.setTransportCompressionLevel(0);

        Optional<ClickHouseInputStream> input = request.getInputStream();
        if (input.isPresent()) {
            if (config.isRequestCompressed()) {
                builder.setInputCompressionType(config.getRequestCompressAlgorithm().encoding());
            }
            if (streaming) {
                // builder.setInputData(ByteString.EMPTY);
                builder.setNextQueryInfo(true);
            } else {
                try (ClickHouseInputStream in = input.get()) {
                    builder.setInputData(ByteString.readFrom(getCompressedInputStream(config, in)));
                } catch (IOException e) {
                    throw new CompletionException(ClickHouseException.of(e, server));
                }
            }
        }

        log.debug("Query(stream=%s): %s", streaming, sql);

        return builder.setQuery(sql).build();
    }

    protected static void fill(ClickHouseRequest<?> request, StreamObserver<QueryInfo> observer) {
        try {
            QueryInfo queryInfo = convert(request, true);
            boolean hasNext = queryInfo.getNextQueryInfo();
            observer.onNext(queryInfo);
            if (hasNext) {
                final ClickHouseNode server = request.getServer();
                final ClickHouseConfig config = request.getConfig();
                try (ClickHouseInputStream input = getCompressedInputStream(config, request.getInputStream().get())) { // NOSONAR
                    byte[] bytes = new byte[config.getRequestChunkSize()];
                    while (hasNext) {
                        queryInfo = getChunkedInputData(server, input, bytes);
                        hasNext = queryInfo.getNextQueryInfo();
                        observer.onNext(queryInfo);
                    }
                } catch (IOException e) {
                    throw new CompletionException(ClickHouseException.of(e, server));
                }
            }
        } finally {
            observer.onCompleted();
        }
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
    @SuppressWarnings("unchecked")
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

        ClickHouseResponse response = new ClickHouseGrpcResponse(sealedRequest.getConfig(), // NOSONAR
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

        Result result = stub.executeQuery(convert(sealedRequest, false));

        ClickHouseResponse response = new ClickHouseGrpcResponse(sealedRequest.getConfig(), // NOSONAR
                sealedRequest.getSettings(), result);
        if (result.hasException()) {
            throw new ClickHouseException(result.getException().getCode(), result.getException().getDisplayText(),
                    sealedRequest.getServer());
        }

        return response;
    }
}
