package com.clickhouse.client;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseFile;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

@Deprecated
public class AbstractSocketClient implements AutoCloseable {
    static class SocketRequest {
        final ClickHouseConfig config;
        final ClickHouseInputStream in;
        final ClickHouseOutputStream out;

        final AtomicReference<Throwable> error;

        SocketRequest(ClickHouseConfig config, ClickHouseInputStream in, ClickHouseOutputStream out) {
            this.config = config;
            this.in = in;
            this.out = out;

            this.error = new AtomicReference<>(null);
        }

        boolean hasError() {
            return error.get() != null;
        }

        boolean isDone() {
            return hasError() || (out.isClosed() && in.isClosed());
        }
    }

    public static final String ERROR_INVALID_INPUT_STREAM = "Non-null unclosed input stream is required";
    public static final String ERROR_INVALID_OUTPUT_STREAM = "Non-null unclosed out stream is required";
    public static final String ERROR_READ_TIMEOUT = "Read timed out after waiting for more than %d ms";
    public static final String ERROR_WRITE_TIMEOUT = "Write timed out after waiting for more than %d ms";

    private static final Logger log = LoggerFactory.getLogger(AbstractSocketClient.class);

    private static final Map<String, ClickHouseSocketFactory> cache = Collections.synchronizedMap(new WeakHashMap<>());

    public static final ClickHouseSocketFactory getCustomSocketFactory(String className,
            ClickHouseSocketFactory defaultFactory, Class<?> forClass) {
        if (ClickHouseChecker.isNullOrEmpty(className) || forClass == null) {
            return defaultFactory;
        }

        ClickHouseSocketFactory factory = cache.computeIfAbsent(className,
                k -> ClickHouseUtils.newInstance(k, ClickHouseSocketFactory.class, defaultFactory.getClass()));
        return factory.supports(forClass) ? factory : defaultFactory;
    }

    /**
     * Sets socket options. May be called at any time(e.g. before or even after the
     * socket is bound or connected).
     *
     * @param config non-null configuration
     * @param socket non-null socket
     * @return the given socket
     * @throws SocketException when there's error setting socket options
     */
    public static final Socket setSocketOptions(ClickHouseConfig config, Socket socket) throws SocketException {
        if (socket == null || socket.isClosed()) {
            throw new IllegalArgumentException("Cannot set option(s) on a null or closed socket");
        } else if (config == null) {
            return socket;
        }

        if (!socket.isConnected() || !socket.isBound()) {
            socket.setSoTimeout(config.getSocketTimeout());
        } else if (config.hasOption(ClickHouseClientOption.SOCKET_TIMEOUT)) {
            socket.setSoTimeout(config.getIntOption(ClickHouseClientOption.SOCKET_TIMEOUT));
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_IP_TOS)) {
            socket.setTrafficClass(config.getIntOption(ClickHouseClientOption.SOCKET_IP_TOS));
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_KEEPALIVE)) {
            socket.setKeepAlive(config.getBoolOption(ClickHouseClientOption.SOCKET_KEEPALIVE));
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_LINGER)) {
            int solinger = config.getIntOption(ClickHouseClientOption.SOCKET_LINGER);
            socket.setSoLinger(solinger >= 0, solinger);
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_REUSEADDR)) {
            socket.setReuseAddress(config.getBoolOption(ClickHouseClientOption.SOCKET_REUSEADDR));
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_RCVBUF)) {
            int bufferSize = config.getIntOption(ClickHouseClientOption.SOCKET_RCVBUF);
            socket.setReceiveBufferSize(bufferSize > 0 ? bufferSize : config.getReadBufferSize());
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_SNDBUF)) {
            int bufferSize = config.getIntOption(ClickHouseClientOption.SOCKET_SNDBUF);
            socket.setSendBufferSize(bufferSize > 0 ? bufferSize : config.getWriteBufferSize());
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_TCP_NODELAY)) {
            socket.setTcpNoDelay(config.getBoolOption(ClickHouseClientOption.SOCKET_TCP_NODELAY));
        }
        return socket;
    }

    /**
     * Sets socket options. May be called at any time(e.g. before or even after the
     * socket is bound or connected).
     *
     * @param config non-null configuration
     * @param socket non-null socket channel
     * @return the given socket channel
     * @throws IOException when there's error setting socket options
     */
    public static final SocketChannel setSocketOptions(ClickHouseConfig config, SocketChannel socket)
            throws IOException {
        if (socket == null || socket.socket().isClosed()) {
            throw new IllegalArgumentException("Cannot set option(s) on a null or closed socket channel");
        } else if (config == null) {
            return socket;
        }

        if (config.hasOption(ClickHouseClientOption.SOCKET_IP_TOS)) {
            socket.setOption(StandardSocketOptions.IP_TOS, config.getIntOption(ClickHouseClientOption.SOCKET_IP_TOS));
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_KEEPALIVE)) {
            socket.setOption(StandardSocketOptions.SO_KEEPALIVE,
                    config.getBoolOption(ClickHouseClientOption.SOCKET_KEEPALIVE));
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_LINGER)) {
            socket.setOption(StandardSocketOptions.SO_LINGER,
                    config.getIntOption(ClickHouseClientOption.SOCKET_LINGER));
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_REUSEADDR)) {
            socket.setOption(StandardSocketOptions.SO_REUSEADDR,
                    config.getBoolOption(ClickHouseClientOption.SOCKET_REUSEADDR));
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_RCVBUF)) {
            int bufferSize = config.getIntOption(ClickHouseClientOption.SOCKET_RCVBUF);
            socket.setOption(StandardSocketOptions.SO_RCVBUF, bufferSize > 0 ? bufferSize : config.getReadBufferSize());
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_SNDBUF)) {
            int bufferSize = config.getIntOption(ClickHouseClientOption.SOCKET_SNDBUF);
            socket.setOption(StandardSocketOptions.SO_SNDBUF,
                    bufferSize > 0 ? bufferSize : config.getWriteBufferSize());
        }
        if (config.hasOption(ClickHouseClientOption.SOCKET_TCP_NODELAY)) {
            socket.setOption(StandardSocketOptions.TCP_NODELAY,
                    config.getBoolOption(ClickHouseClientOption.SOCKET_TCP_NODELAY));
        }
        return socket;
    }

    /**
     * Reads raw request from the input stream.
     */
    private final AtomicReference<SocketRequest> request;
    private final SelectionKey selectionKey;
    private final AtomicReference<CompletableFuture<Boolean>> completed;

    protected SocketChannel getSocketChannel() {
        return (SocketChannel) this.selectionKey.channel();
    }

    protected CompletableFuture<Boolean> processRequest(ClickHouseConfig config, ClickHouseInputStream in,
            ClickHouseOutputStream out) throws IOException {
        final long timeout = config.getSocketTimeout();

        log.trace("About to set request: [in=%s, out=%s, timeout=%d]", in, out, timeout);
        long startTime = 0L;
        final SocketRequest req = new SocketRequest(config, in, out);
        while (!request.compareAndSet(null, req)) {
            if (timeout <= 0L) {
                // busy wait
            } else if (startTime == 0L) {
                startTime = System.currentTimeMillis();
            } else if (System.currentTimeMillis() - startTime > timeout) {
                throw new SocketTimeoutException(ClickHouseUtils.format(ERROR_WRITE_TIMEOUT, timeout));
            }
        }
        // log.trace("Request changed to: [%s]", req);

        setInterestOp(SelectionKey.OP_WRITE);

        return ClickHouseClient.submit(() -> {
            while (req != request.get() || !req.isDone()) {
                // busy wait
            }
            return req.hasError();
        });
    }

    protected final void setInterestOp(int op) {
        final SelectionKey key = this.selectionKey;
        if (!key.isValid()) {
            return;
        }
        final int interestOps = key.interestOps();
        if ((interestOps & op) == 0) {
            key.interestOps(interestOps | op);
        }
    }

    protected final void removeInterestOp(int op) {
        final SelectionKey key = this.selectionKey;
        if (!key.isValid()) {
            return;
        }
        final int interestOps = key.interestOps();
        if ((interestOps & op) != 0) {
            key.interestOps(interestOps & ~op);
        }
    }

    protected void onConnect(ClickHouseConfig config, SocketChannel channel) throws IOException {
        if (channel.finishConnect()) {
            log.debug("Connection established: [%s] <-> [%s]", localAddress(), remoteAddress());
            setInterestOp(SelectionKey.OP_READ);
        } else {
            throw new ConnectException(ClickHouseUtils.format("Failed to connect to [%s]", remoteAddress()));
        }
    }

    /**
     * Reads byte from socket input buffer.
     *
     * @param config non-null configuration
     * @param sc     socket channel
     * @param out    output stream
     * @return true if the read was a success; false otherwise
     * @throws IOException
     */
    protected boolean onRead(ClickHouseConfig config, SocketChannel sc, ClickHouseOutputStream out) throws IOException {
        final long socketTimeout = config.getSocketTimeout();
        final long startTime = socketTimeout > 0L ? System.currentTimeMillis() : 0L;

        // final ClickHousePassThruStream s = out.getUnderlyingStream();
        ByteBuffer buffer = ByteBuffer.allocate(config.getWriteBufferSize());
        byte[] bytes = buffer.array();
        int len = 0;
        while ((len = sc.read(buffer)) > 0) {
            log.trace("Receive from [%s]: [%s]", out, new String(bytes, 0, len));
            out.write(bytes, 0, len);
            buffer.clear();
            if (startTime > 0L && System.currentTimeMillis() - startTime > socketTimeout) {
                throw new SocketTimeoutException(ClickHouseUtils.format(ERROR_READ_TIMEOUT, socketTimeout));
            }
        }

        return len != -1;
    }

    protected long onWrite(ClickHouseConfig config, SocketChannel sc, ClickHouseInputStream in, long startPosition)
            throws IOException {
        final long socketTimeout = config.getSocketTimeout();
        final long startTime = socketTimeout > 0L ? System.currentTimeMillis() : 0L;

        final ClickHousePassThruStream s = in.getUnderlyingStream();
        if (s.hasInput() && s instanceof ClickHouseFile) {
            try (FileChannel fc = FileChannel.open(((ClickHouseFile) s).getFile().toPath())) {
                long size = fc.size();
                long chunkSize = config.getRequestChunkSize();
                long offset = startPosition;
                while (size > 0) {
                    // TODO write chunk header
                    long transferred = fc.transferTo(offset, size >= chunkSize ? chunkSize : size, sc);
                    if (transferred == 0) { // output buffer is jammed
                        return offset;
                    }
                    size -= transferred;
                    offset += transferred;
                }
            }
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(config.getReadBufferSize());
            byte[] bytes = buffer.array();
            int len = 0;
            while ((len = in.read(bytes)) > 0) {
                buffer.limit(len);
                log.trace("Send to [%s]: [%s]", in, new String(bytes, 0, len));
                while (buffer.hasRemaining()) {
                    if (sc.write(buffer) == 0) {
                        return 1L;
                    }
                    if (startTime > 0L && System.currentTimeMillis() - startTime > socketTimeout) {
                        throw new SocketTimeoutException(ClickHouseUtils.format(ERROR_WRITE_TIMEOUT, socketTimeout));
                    }
                }
            }
        }

        return 0L;
    }

    protected boolean start() throws IOException {
        final ClickHouseConfig f = (ClickHouseConfig) this.selectionKey.attachment();
        final long socketTimeout = f.getSocketTimeout();

        try (SocketChannel c = getSocketChannel(); Selector s = this.selectionKey.selector()) {
            while (c.isOpen()) {
                if (s.select(c.isConnected() ? socketTimeout : f.getConnectionTimeout()) < 1) {
                    LockSupport.parkNanos(1L);
                }

                final Iterator<SelectionKey> keysIterator = s.selectedKeys().iterator();
                while (keysIterator.hasNext()) {
                    final SelectionKey key = keysIterator.next();
                    keysIterator.remove();

                    final SocketChannel sc = (SocketChannel) key.channel();
                    if (key.isValid() && key.isConnectable()) {
                        onConnect(f, sc);
                    }

                    final SocketRequest req = request.get();
                    if (req == null) {
                        continue;
                    }

                    if (key.isValid() && key.isWritable()) {
                        final ClickHouseInputStream in = req.in;
                        // socket output buffer was full due to slow/jammed network
                        long offset = (long) in.getUserData("offset", 0L);
                        if ((offset = onWrite(req.config, sc, in, offset)) > 0L && in.available() > 0) {
                            // processRequest(in, socketTimeout);
                        } else {
                            in.close();
                            removeInterestOp(SelectionKey.OP_WRITE);
                        }
                        in.setUserData("offset", offset);
                    }

                    if (key.isValid() && key.isReadable()) {
                        final ClickHouseOutputStream out = req.out;
                        if (onRead(req.config, sc, out)) {
                            log.trace("Reset request holder: %s", request.compareAndSet(req, null));
                            out.close();
                        } else {
                            throw new ConnectException("Failed to read");
                        }
                    }
                }
            }
        } catch (Throwable t) {
            final SocketRequest req = request.get();
            if (req != null && !req.isDone()) {
                req.error.compareAndSet(null, t);
                throw t;
            } else if (t instanceof ClosedSelectorException) {
                log.info("Socket channel between [%s] and [%s] was closed", localAddress(), remoteAddress());
            }
        } finally {
            close();
        }

        return true;
    }

    public AbstractSocketClient() throws IOException {
        this(new ClickHouseConfig());
    }

    public AbstractSocketClient(ClickHouseNode server) throws IOException {
        this(server.config);

        connect(server);
    }

    public AbstractSocketClient(ClickHouseConfig config) throws IOException {
        if (config == null) {
            config = new ClickHouseConfig();
        }

        this.request = new AtomicReference<>(null);

        SocketChannel channel = setSocketOptions(config, SocketChannel.open());
        channel.configureBlocking(false);
        this.selectionKey = channel.register(Selector.open(), 0, config);

        this.completed = new AtomicReference<>(null);
    }

    public CompletableFuture<Boolean> connect(ClickHouseNode server) throws IOException {
        return connect(new InetSocketAddress(server.getHost(), server.getPort()));
    }

    public CompletableFuture<Boolean> connect(InetSocketAddress address) throws IOException {
        log.trace("Connecting to [%s]", address);
        final SocketChannel channel = (SocketChannel) this.selectionKey.channel();
        if (!channel.connect(ClickHouseChecker.nonNull(address, InetSocketAddress.class.getSimpleName()))) {
            setInterestOp(SelectionKey.OP_CONNECT);
        }

        return ClickHouseClient.submit(this::start);
    }

    // public ClickHouseInputStream getInputStream() {
    // return resp.getInputStream();
    // }

    // public ClickHouseOutputStream getOutputStream() {
    // return req;
    // }

    public boolean isActive() {
        final SocketChannel channel = getSocketChannel();
        return channel.isOpen() && channel.isConnected();
    }

    public boolean isShutdown() {
        final Socket s = getSocketChannel().socket();
        return s.isInputShutdown() && s.isOutputShutdown() || !isActive();
    }

    public InetSocketAddress localAddress() throws IOException {
        return (InetSocketAddress) getSocketChannel().getLocalAddress();
    }

    public InetSocketAddress remoteAddress() throws IOException {
        return (InetSocketAddress) getSocketChannel().getRemoteAddress();
    }

    public ClickHouseInputStream send(ClickHouseConfig config, ClickHouseInputStream rawRequest) throws IOException {
        if (rawRequest == null || rawRequest.isClosed()) {
            throw new IllegalArgumentException(ERROR_INVALID_INPUT_STREAM);
        }

        ClickHousePipedOutputStream responeStream = ClickHouseDataStreamFactory.getInstance()
                .createPipedOutputStream(ClickHouseChecker.nonNull(config, ClickHouseConfig.TYPE_NAME));
        processRequest(config, rawRequest, responeStream);
        return responeStream.getInputStream();
    }

    public void send(ClickHouseConfig config, ClickHouseInputStream rawRequest, ClickHouseOutputStream responseStream)
            throws IOException {
        if (rawRequest == null || rawRequest.isClosed()) {
            throw new IllegalArgumentException(ERROR_INVALID_INPUT_STREAM);
        } else if (responseStream == null || responseStream.isClosed()) {
            throw new IllegalArgumentException(ERROR_INVALID_OUTPUT_STREAM);
        }

        processRequest(config, rawRequest, responseStream);
    }

    @Override
    public void close() throws IOException {
        final SelectionKey key = this.selectionKey;
        if (!key.isValid()) {
            return;
        }

        try {
            log.trace("Closing selector...");
            if (key.selector().isOpen()) {
                key.selector().close();
            }
        } finally {
            log.trace("Closing channel...");
            if (key.channel().isOpen()) {
                key.channel().close();
            }

            log.trace("Closing selection key...");
            log.trace("Release attached object: [%s]", key.attach(null));
            key.cancel();
        }
    }
}
