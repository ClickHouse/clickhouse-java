package com.clickhouse.benchmark.client;

import java.util.concurrent.Future;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import com.clickhouse.benchmark.BaseState;
import com.clickhouse.benchmark.Constants;
import com.clickhouse.benchmark.ServerState;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseClientBuilder;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.grpc.config.ClickHouseGrpcOption;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseValue;

@State(Scope.Thread)
public class ClientState extends BaseState {
    @Param(value = { "GRPC" })
    private String protocol;

    @Param(value = { Constants.REUSE_CONNECTION, Constants.NEW_CONNECTION })
    private String connection;

    @Param(value = { "RowBinaryWithNamesAndTypes", "TabSeparatedWithNamesAndTypes" })
    private String format;

    @Param(value = { "async", "sync" })
    private String mode;

    @Param(value = { "netty", "okhttp" })
    private String transport;

    private ClickHouseNode server;
    private ClickHouseClient client;

    private int randomSample;
    private int randomNum;

    private ClickHouseClient createClient() {
        String bufferSize = System.getProperty("bufferSize");
        String compression = System.getProperty("compression");
        String threads = System.getProperty("threads");
        String window = System.getProperty("window");

        ClickHouseClientBuilder builder = ClickHouseClient.builder();
        if (bufferSize != null && !bufferSize.isEmpty()) {
            builder.option(ClickHouseClientOption.BUFFER_SIZE, Integer.parseInt(bufferSize));
        }
        if (compression != null && !compression.isEmpty()) {
            // builder.option(ClickHouseClientOption.COMPRESSION,
            // compression.toUpperCase());
            if (ClickHouseCompression.NONE.name().equalsIgnoreCase(compression)) {
                builder.option(ClickHouseGrpcOption.USE_FULL_STREAM_DECOMPRESSION, true);
            }
        }
        if (threads != null && !threads.isEmpty()) {
            builder.option(ClickHouseClientOption.MAX_THREADS_PER_CLIENT, Integer.parseInt(threads));
        }

        if (window != null && !window.isEmpty()) {
            builder.option(ClickHouseGrpcOption.FLOW_CONTROL_WINDOW, Integer.parseInt(window));
        }

        return builder.option(ClickHouseClientOption.ASYNC, "async".equals(mode))
                .option(ClickHouseGrpcOption.USE_OKHTTP, "okhttp".equals(transport)).build();
    }

    @Setup(Level.Trial)
    public void doSetup(ServerState serverState) throws ClickHouseException {
        server = ClickHouseNode.builder().host(serverState.getHost()).port(ClickHouseProtocol.valueOf(protocol))
                .database(serverState.getDatabase())
                .credentials(
                        ClickHouseCredentials.fromUserAndPassword(serverState.getUser(), serverState.getPassword()))
                .build();
        client = createClient();

        String[] sqls = new String[] { "drop table if exists system.test_insert",
                "create table if not exists system.test_insert(id String, i Nullable(UInt64), s Nullable(String), t Nullable(DateTime))engine=Memory" };

        for (String sql : sqls) {
            try (ClickHouseResponse resp = client.read(server).query(sql).executeAndWait()) {

            }
        }
    }

    @TearDown(Level.Trial)
    public void doTearDown(ServerState serverState) throws ClickHouseException {
        dispose();

        try (ClickHouseResponse resp = client.read(server).query("truncate table system.test_insert")
                .executeAndWait()) {

        } finally {
            try {
                client.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Setup(Level.Iteration)
    public void prepare() {
        if (!Constants.REUSE_CONNECTION.equalsIgnoreCase(connection)) {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    // ignore
                }
            }
            client = createClient();
        }

        randomSample = getRandomNumber(Constants.SAMPLE_SIZE);
        randomNum = getRandomNumber(Constants.FLOATING_RANGE);
    }

    @TearDown(Level.Iteration)
    public void shutdown() {
        if (!Constants.REUSE_CONNECTION.equalsIgnoreCase(connection)) {
            try {
                client.close();
            } catch (Exception e) {
                // ignore
            } finally {
                client = null;
            }
        }
    }

    public ClickHouseFormat getFormat() {
        return ClickHouseFormat.valueOf(format);
    }

    public int getSampleSize() {
        return Constants.SAMPLE_SIZE;
    }

    public int getRandomSample() {
        return randomSample;
    }

    public int getRandomNumber() {
        return randomNum;
    }

    public ClickHouseRequest<?> newRequest() {
        return client.read(server);
    }

    public void consume(Blackhole blackhole, Future<ClickHouseResponse> future) throws InterruptedException {
        consume(blackhole, () -> {
            try (ClickHouseResponse resp = future.get()) {
                for (ClickHouseRecord rec : resp.records()) {
                    for (ClickHouseValue val : rec) {
                        blackhole.consume(val.asObject());
                    }
                }

                blackhole.consume(resp.getSummary());

                return resp.getSummary();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }
}
