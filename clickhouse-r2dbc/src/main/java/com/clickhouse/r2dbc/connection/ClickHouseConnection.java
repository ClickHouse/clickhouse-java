package com.clickhouse.r2dbc.connection;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import com.clickhouse.r2dbc.ClickHouseBatch;
import com.clickhouse.r2dbc.ClickHouseStatement;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

import static reactor.core.publisher.Mono.just;

public class ClickHouseConnection implements Connection {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseConnection.class);

    private static final String PRODUCT_NAME = "ClickHouse-R2dbcDriver";

    public static final int DEFAULT_TIMEOUT_FOR_CONNECTION_HEALTH_CHECK = (Integer) ClickHouseClientOption.CONNECTION_TIMEOUT
            .getDefaultValue();
    final ClickHouseClient client;
    final ClickHouseNode node;
    private boolean closed = false;

    ClickHouseConnection(Function<ClickHouseNodeSelector, ClickHouseNode> nodes) {
        this.node = nodes.apply(ClickHouseNodeSelector.EMPTY);

        ClickHouseConfig config = this.node.getConfig();
        this.client = ClickHouseClient.builder()
                .option(ClickHouseClientOption.FORMAT, ClickHouseFormat.RowBinaryWithNamesAndTypes).config(config)
                .nodeSelector(ClickHouseNodeSelector.of(this.node.getProtocol())).build();
    }

    /**
     * Transactions are not supported so this is a no-op implementation,
     */
    @Override
    public Mono<Void> beginTransaction() {
        log.debug("Clickhouse does not support transactions so skipping initialization of transaction.");
        return Mono.empty();
    }

    /**
     * Transactions are not supported so this is a no-op implementation,
     */
    @Override
    public Mono<Void> beginTransaction(TransactionDefinition transactionDefinition) {
        log.debug("Clickhouse does not support transactions so skipping initialization of transaction.");
        return Mono.empty();
    }

    @Override
    public Publisher<Void> close() {
        try {
            client.close();
            closed = true;
            return Mono.empty();
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    /**
     * Transactions are not supported so this is a no-op implementation,
     */
    @Override
    public Publisher<Void> commitTransaction() {
        log.debug("Clickhouse does not support transactions so skipping commit of transaction.");
        return Mono.empty();
    }

    /**
     * Returns {@link ClickHouseBatch} for batching statements.
     *
     * @return Batch object
     */
    @Override
    public Batch createBatch() {
        ClickHouseRequest<?> req = client.read(node).option(ClickHouseClientOption.PRODUCT_NAME, PRODUCT_NAME);
        if (isHttp()) {
            req = req.set("send_progress_in_http_headers", 1);
        }
        req.option(ClickHouseClientOption.ASYNC, true);
        return new ClickHouseBatch(req);
    }

    /**
     * Returns true since there is no transaction support.
     *
     * @return true
     */
    @Override
    public Publisher<Void> createSavepoint(String s) {
        return Mono.empty();
    }

    @Override
    public Statement createStatement(String sql) {
        ClickHouseRequest<?> req = client.read(node).option(ClickHouseClientOption.PRODUCT_NAME, PRODUCT_NAME);
        if (isHttp()) {
            req = req.set("send_progress_in_http_headers", 1);
        }
        req.option(ClickHouseClientOption.ASYNC, true);
        return new ClickHouseStatement(sql, req);
    }

    private boolean isHttp() {
        return node.getProtocol() == ClickHouseProtocol.HTTP;
    }

    /**
     * Returns true since there is no transaction support.
     *
     * @return true
     */
    @Override
    public boolean isAutoCommit() {
        return true;
    }

    @Override
    public ConnectionMetadata getMetadata() {
        return new ClickHouseConnectionMetadata(client, node);
    }

    /**
     * Returns transaction isolation level.
     *
     * @return Always returns read committed.
     */
    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return IsolationLevel.READ_COMMITTED;
    }

    @Override
    public Publisher<Void> releaseSavepoint(String s) {
        return null;
    }

    /**
     * Transactions are not supported so this is a no-op implementation,
     */
    @Override
    public Publisher<Void> rollbackTransaction() {
        log.debug("Clickhouse does not support transactions so skipping rollback of transaction.");
        return Mono.empty();
    }

    @Override
    public Publisher<Void> rollbackTransactionToSavepoint(String s) {
        return null;
    }

    /**
     * Transactions are not supported so this is a no-op implementation,
     */
    @Override
    public Publisher<Void> setAutoCommit(boolean b) {
        log.debug("Clickhouse does not support transactions so skipping setting of transaction auto commit.");
        return Mono.empty();
    }

    @Override
    public Publisher<Void> setLockWaitTimeout(Duration duration) {
        return null;
    }

    @Override
    public Publisher<Void> setStatementTimeout(Duration duration) {
        return null;
    }

    /**
     * Since transactions are not supported, this method will throw exception.
     *
     * @param isolationLevel isolation level for transaction
     */
    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        return Mono.error(new UnsupportedOperationException("Transaction isolation level can not be changed."));
    }

    @Override
    public Publisher<Boolean> validate(ValidationDepth validationDepth) {
        if (validationDepth == ValidationDepth.REMOTE) {
            return closed ? just(false) : just(client.ping(node, DEFAULT_TIMEOUT_FOR_CONNECTION_HEALTH_CHECK));
        } else { // validationDepth.LOCAL
            return just(client != null && !closed);
        }
    }
}
