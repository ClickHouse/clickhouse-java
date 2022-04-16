package com.clickhouse.examples.jdbc;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.data.BinaryStreamUtils;
import com.clickhouse.client.data.ClickHousePipedStream;

public class Main {
    static void dropAndCreateTable(ClickHouseNode server, String table) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
            ClickHouseRequest<?> request = client.connect(server);
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
            ClickHouseRequest.Mutation request = client.connect(server).write().table(table)
                    .format(ClickHouseFormat.RowBinary);
            ClickHouseConfig config = request.getConfig();
            CompletableFuture<ClickHouseResponse> future;
            // back-pressuring is not supported, you can adjust the first two arguments
            try (ClickHousePipedStream stream = new ClickHousePipedStream(config.getWriteBufferSize(),
                    config.getMaxQueuedBuffers(), config.getSocketTimeout())) {
                // in async mode, which is default, execution happens in a worker thread
                future = request.data(stream.getInput()).execute();

                // writing happens in main thread
                for (int i = 0; i < 1000000; i++) {
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
                ClickHouseResponse response = client.connect(server).query("select * from " + table).execute().get()) {
            int count = 0;
            // or use stream API via response.stream()
            for (ClickHouseRecord rec : response.records()) {
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
                .host(System.getProperty("chHost", "192.168.3.16"))
                .port(ClickHouseProtocol.GRPC, Integer.parseInt(System.getProperty("chPort", "9100")))
                .database("system").credentials(ClickHouseCredentials.fromUserAndPassword(
                        System.getProperty("chUser", "default"), System.getProperty("chPassword", "")))
                .build();

        String table = "grpc_example_table";

        try {
            dropAndCreateTable(server, table);

            insert(server, table);

            query(server, table);
        } catch (ClickHouseException e) {
            e.printStackTrace();
        }
    }
}
