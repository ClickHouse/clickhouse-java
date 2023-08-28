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
            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config, (Runnable) null)) {
                // in async mode, which is default, execution happens in a worker thread
                future = request.data(stream.getInputStream()).execute();

                // writing happens in main thread
                for (int i = 0; i < 10_000; i++) {
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
                        .query("select * from " + table).execute().get()) {
            int count = 0;
            // or use stream API via response.stream()
            for (ClickHouseRecord r : response.records()) {
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
        ClickHouseNode server = ClickHouseNode.builder()
                .host(System.getProperty("chHost", "localhost"))
                .port(ClickHouseProtocol.HTTP, Integer.getInteger("chPort", 8123))
                // .port(ClickHouseProtocol.GRPC, Integer.getInteger("chPort", 9100))
                // .port(ClickHouseProtocol.TCP, Integer.getInteger("chPort", 9000))
                .database("system").credentials(ClickHouseCredentials.fromUserAndPassword(
                        System.getProperty("chUser", "default"), System.getProperty("chPassword", "")))
                .build();

        String table = "java_client_example_table";

        try {
            dropAndCreateTable(server, table);

            System.out.println("Insert: " + insert(server, table));

            System.out.println("Query: " + query(server, table));
        } catch (ClickHouseException e) {
            e.printStackTrace();
        }
    }
}
