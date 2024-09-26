package com.clickhouse.examples.client_v2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.query.QueryResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BigDatasetExamples {

    private final String endpoint;
    private final String user;
    private final String password;
    private final String database;

    public BigDatasetExamples(String endpoint, String user, String password, String database) {
        this.endpoint = endpoint;
        this.user = user;
        this.password = password;
        this.database = database;
    }

    /**
     * Reads {@code system.numbers} table into a result set of numbers of different types.
     *
     */
    void readBigSetOfNumbers(int limit, int iterations, int concurrency) {
        Client client = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .setDefaultDatabase(database)
                .compressServerResponse(false)
                .compressClientRequest(false)
                .setLZ4UncompressedBufferSize(1048576)
                .useNewImplementation(true)
                // when network buffer and socket buffer are the same size - it is less IO calls and more efficient
                .setSocketRcvbuf(1_000_000)
                .setClientNetworkBufferSize(1_000_000)
                .setMaxConnections(20)
                .build();
        try {
            client.ping(10); // warmup connections pool. required once per client.

            Runnable task = () -> {
                StringBuilder sb = new StringBuilder();

                for (int i = 0; i < iterations; i++) {
                    try {
                        long[] stats = doReadNumbersSet(client, limit);
                        for (long stat : stats) {
                            sb.append(stat).append(", ");
                        }
                        sb.append("\n");
                    } catch (Exception e) {
                        log.error("Failed to read dataset", e);
                    }
                }

                System.out.print(sb.toString());
            };

            System.out.println("Records, Read Time, Request Time, Server Time");
            if (concurrency == 1) {
                task.run();
            } else {
                ExecutorService executor = new ThreadPoolExecutor(concurrency, Integer.MAX_VALUE,
                        60L, TimeUnit.SECONDS,
                        new SynchronousQueue<Runnable>());

                for (int i = 0; i < concurrency; i++) {
                    executor.submit(task);
                }

                executor.shutdown();
                executor.awaitTermination(3, TimeUnit.MINUTES);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            client.close();
        }
    }

    /**
     * Does actual request and returns time stats in format:
     *  [number of records, read time in ms, request initiation time in ms, server time in ms]
     * @param client
     * @param limit
     * @return
     */
    private long[] doReadNumbersSet(Client client, int limit) {
        final String query = DATASET_QUERY + " LIMIT " + limit;
        try (QueryResponse response = client.query(query).get(3000, TimeUnit.MILLISECONDS)) {
            ArrayList<com.clickhouse.demo_service.data.NumbersRecord> result = new ArrayList<>();

            // iterable approach is more efficient for large datasets because it doesn't load all records into memory
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

            long start = System.nanoTime();
            while (reader.next() != null) {
                result.add(new com.clickhouse.demo_service.data.NumbersRecord(
                        reader.getUUID("id"),
                        reader.getLong("p1"),
                        reader.getBigInteger("number"),
                        reader.getFloat("p2"),
                        reader.getDouble("p3")
                ));
            }
            long duration = System.nanoTime() - start;

            return new long[] { result.size(), TimeUnit.NANOSECONDS.toMillis(duration), response.getMetrics().getMetric(ClientMetrics.OP_DURATION).getLong(),
                    TimeUnit.NANOSECONDS.toMillis(response.getServerTime()) };
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch dataset", e);
        }
    }

    private static final String DATASET_QUERY =
            "SELECT generateUUIDv4() as id, " +
                    "toUInt32(number) as p1, " +
                    "number,  " +
                    "toFloat32(number/100000) as p2, " +
                    "toFloat64(number/100000) as p3" +
                    " FROM system.numbers";

    public static void main(String[] args) {
        final String endpoint = System.getProperty("chEndpoint", "http://localhost:8123");
        final String user = System.getProperty("chUser", "default");
        final String password = System.getProperty("chPassword", "");
        final String database = System.getProperty("chDatabase", "default");

//        profilerDelay();

        BigDatasetExamples examples = new BigDatasetExamples(endpoint, user, password, database);

        examples.readBigSetOfNumbers(100_000, 100, 10);

//        profilerDelay();
    }

    private static void profilerDelay() {
        // Delay for a profiler
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
