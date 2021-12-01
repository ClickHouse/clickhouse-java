package com.clickhouse.benchmark.client;

import java.util.concurrent.Future;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import com.clickhouse.client.ClickHouseResponse;

public class Load extends ClientBenchmark {
    @Benchmark
    public void selectDateTime32Rows(Blackhole blackhole, ClientState state) throws Throwable {
        int rows = state.getSampleSize() + state.getRandomNumber();
        Future<ClickHouseResponse> future = state.newRequest().format(state.getFormat())
                .query("select toDateTime('2021-02-20 13:15:20') + number as d from system.numbers limit " + rows)
                .execute();
        state.consume(blackhole, future);
    }

    @Benchmark
    public void selectDateTime64Rows(Blackhole blackhole, ClientState state) throws Throwable {
        int rows = state.getSampleSize() + state.getRandomNumber();
        Future<ClickHouseResponse> future = state.newRequest().format(state.getFormat()).query(
                "select toDateTime64('2021-02-20 13:15:20.000000000', 9) + number as d from system.numbers limit "
                        + rows)
                .execute();
        state.consume(blackhole, future);
    }

    @Benchmark
    public void selectInt32Rows(Blackhole blackhole, ClientState state) throws Throwable {
        int rows = state.getSampleSize() + state.getRandomNumber();
        Future<ClickHouseResponse> future = state.newRequest().format(state.getFormat())
                .query("select toInt32(number) from system.numbers limit " + rows).execute();
        state.consume(blackhole, future);
    }

    @Benchmark
    public void selectStringRows(Blackhole blackhole, ClientState state) throws Throwable {
        int rows = state.getSampleSize() + state.getRandomNumber();
        Future<ClickHouseResponse> future = state.newRequest().format(state.getFormat())
                .query("select toString(number) as s from system.numbers limit " + rows).execute();
        state.consume(blackhole, future);
    }

    @Benchmark
    public void selectUInt64Rows(Blackhole blackhole, ClientState state) throws Throwable {
        int rows = state.getSampleSize() + state.getRandomNumber();
        Future<ClickHouseResponse> future = state.newRequest().format(state.getFormat())
                .query("select * from system.numbers limit " + rows).execute();
        state.consume(blackhole, future);
    }
}
