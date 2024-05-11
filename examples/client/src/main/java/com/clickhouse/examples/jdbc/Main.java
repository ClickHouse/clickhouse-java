package com.clickhouse.examples.jdbc;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.format.BinaryStreamUtils;

public class Main {
    static void dropAndCreateTable(ClickHouseNode server, String table) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
            ClickHouseRequest<?> request = client.read(server);
            // or use future chaining
            request.query("drop table if exists " + table).execute().get();
            request.query("create table " + table + "(a String, b Nullable(String)) engine=MergeTree() order by a")
                    .execute().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ClickHouseException.forCancellation(e, server);
        } catch (ExecutionException e) {
            throw ClickHouseException.of(e, server);
        }
    }

    static long insert(ClickHouseNode server, String table) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
            ClickHouseRequest.Mutation request = client.read(server).write().table(table)
                    .format(ClickHouseFormat.RowBinary);
            ClickHouseConfig config = request.getConfig();
            CompletableFuture<ClickHouseResponse> future;
            // back-pressuring is not supported, you can adjust the first two arguments
            // `stream` must be closed to commit changes to a server
            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config, (Runnable) null)) {
                // in async mode, which is default, execution happens in a worker thread
                future = request.data(stream.getInputStream()).execute();

                // writing happens in main thread
                for (int i = 0; i < 10; i++) {
                    BinaryStreamUtils.writeString(stream, String.valueOf(i % 16));
                    BinaryStreamUtils.writeNonNull(stream);
                    BinaryStreamUtils.writeString(stream, UUID.randomUUID().toString());
                }
            }

            // response should be always closed
            try (ClickHouseResponse response = future.get()) {
                ClickHouseResponseSummary summary = response.getSummary();
                return summary.getWrittenRows();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ClickHouseException.forCancellation(e, server);
        } catch (ExecutionException | IOException e) {
            throw ClickHouseException.of(e, server);
        }
    }

    static int query(ClickHouseNode server, String table) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
                ClickHouseResponse response = client.read(server)
                        // prefer to use RowBinaryWithNamesAndTypes as it's fully supported
                        // see details at https://github.com/ClickHouse/clickhouse-java/issues/928
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select a, b from " + table).execute().get()) {
            int count = 0;
            // or use stream API via response.stream()
            for (ClickHouseRecord r : response.records()) {
                System.out.println(r.getValue(0).asString() + " | " + r.getValue("b").asString());
                count++;
            }
            return count;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ClickHouseException.forCancellation(e, server);
        } catch (ExecutionException e) {
            throw ClickHouseException.of(e, server);
        }
    }

    public static void main(String[] args) {
        if (args.length == 1 && args[0].equals("--help")) {
            System.out.println("Usage: java -cp ... com.clickhouse.examples.jdbc.Main");
            System.out.println("  -DchHost=... - ClickHouse server host");
            System.out.println("  -DchPort=... - ClickHouse server port");
            System.out.println("  -DchUser=... - ClickHouse server user");
            System.out.println("  -DchPassword=... - ClickHouse server password");
            System.exit(0);
        }

        // Describe how to connect to ClickHouse server.
        ClickHouseNode server = ClickHouseNode.builder()
                .host(System.getProperty("chHost", "localhost"))
                .port(ClickHouseProtocol.HTTP, Integer.getInteger("chPort", 8123))
                .database("system").credentials(ClickHouseCredentials.fromUserAndPassword(
                        System.getProperty("chUser", "default"), System.getProperty("chPassword", "")))
                .build();

        String table = "default.java_client_example_table";

        try {
            System.out.println("Reseting table " + table + " on " + server + "...");
            dropAndCreateTable(server, table);

            System.out.println("Inserted " + insert(server, table) + " records");

            System.out.println("Query returned " + query(server, table) + " records");

        } catch (ClickHouseException e) {
            e.printStackTrace();
        }
        System.out.println("Done!");
    }
}
