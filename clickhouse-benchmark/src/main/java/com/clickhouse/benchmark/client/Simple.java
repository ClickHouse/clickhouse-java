package com.clickhouse.benchmark.client;

import java.util.concurrent.Future;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import com.clickhouse.client.ClickHouseResponse;

public class Simple extends ClientBenchmark {
    @Benchmark
    public void insertOneRandomNumber(Blackhole blackhole, ClientState state) throws Throwable {
        Future<ClickHouseResponse> future = state.newRequest().format(state.getFormat())
                .query("insert into test_insert(i) values(" + state.getRandomSample() + ")").execute();
        state.consume(blackhole, future);
    }

    @Benchmark
    public void selectOneRandomNumber(Blackhole blackhole, ClientState state) throws Throwable {
        Future<ClickHouseResponse> future = state.newRequest().format(state.getFormat())
                .query("select " + state.getRandomSample() + " as n").execute();
        state.consume(blackhole, future);
    }
}
