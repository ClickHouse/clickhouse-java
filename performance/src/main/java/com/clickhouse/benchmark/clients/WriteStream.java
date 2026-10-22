package com.clickhouse.benchmark.clients;

import com.clickhouse.benchmark.TestEnvironment;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.clickhouse.benchmark.TestEnvironment.DB_NAME;
import static com.clickhouse.benchmark.TestEnvironment.getHost;
import static com.clickhouse.benchmark.TestEnvironment.getPassword;
import static com.clickhouse.benchmark.TestEnvironment.getPort;
import static com.clickhouse.benchmark.TestEnvironment.getServer;
import static com.clickhouse.benchmark.TestEnvironment.getUsername;
import static com.clickhouse.benchmark.TestEnvironment.isSsl;

/**
 * Isolates the HTTP request-body handoff by sending the same pre-serialized RowBinary rows through both clients.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(3)
public class WriteStream {
    private static final int ROWS_PER_BATCH = 25_000;
    private static final int ROW_SIZE = 256;
    private static final String TABLE_NAME = "write_stream_null";

    private ClickHouseClient clientV1NonBlocking;
    private ClickHouseClient clientV1Blocking;
    private Client clientV2;
    private byte[][] rows;

    @Setup(Level.Trial)
    public void setup() {
        TestEnvironment.setupEnvironment();
        BenchmarkBase.runQuery(String.format(
                "CREATE TABLE `%s`.`%s` (`payload` FixedString(%d)) ENGINE = Null",
                DB_NAME, TABLE_NAME, ROW_SIZE));

        clientV1NonBlocking = createClientV1(false);
        clientV1Blocking = createClientV1(true);
        clientV2 = new Client.Builder()
                .addEndpoint(Protocol.HTTP, getHost(), getPort(), isSsl())
                .setUsername(getUsername())
                .setPassword(getPassword())
                .setDefaultDatabase(DB_NAME)
                .setMaxRetries(0)
                .build();

        rows = new byte[ROWS_PER_BATCH][ROW_SIZE];
        for (int i = 0; i < rows.length; i++) {
            Arrays.fill(rows[i], (byte) i);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        Exception failure = null;
        for (AutoCloseable closeable : new AutoCloseable[] {clientV1NonBlocking, clientV1Blocking, clientV2}) {
            if (closeable == null) {
                continue;
            }
            try {
                closeable.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        try {
            TestEnvironment.cleanupEnvironment();
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS_PER_BATCH)
    public void v1NonBlocking() throws Exception {
        insertV1(clientV1NonBlocking);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS_PER_BATCH)
    public void v1BlockingQueue() throws Exception {
        insertV1(clientV1Blocking);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS_PER_BATCH)
    public void v2Direct() throws Exception {
        try (InsertResponse response = clientV2.insert(TABLE_NAME, out -> {
            for (byte[] row : rows) {
                out.write(row);
            }
        }, ClickHouseFormat.RowBinary, new InsertSettings()).get()) {
            response.getWrittenRows();
        }
    }

    private void insertV1(ClickHouseClient client) throws Exception {
        try (ClickHouseResponse response = client.read(getServer())
                .write()
                .option(ClickHouseClientOption.ASYNC, false)
                .format(ClickHouseFormat.RowBinary)
                .query(BenchmarkBase.getInsertQuery(TABLE_NAME))
                .data(out -> {
                    for (byte[] row : rows) {
                        out.write(row);
                    }
                })
                .executeAndWait()) {
            response.getSummary();
        }
    }

    private static ClickHouseClient createClientV1(boolean useBlockingQueue) {
        return ClickHouseClient.builder()
                .option(ClickHouseClientOption.USE_BLOCKING_QUEUE, useBlockingQueue)
                .defaultCredentials(ClickHouseCredentials.fromUserAndPassword(getUsername(), getPassword()))
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
    }
}
