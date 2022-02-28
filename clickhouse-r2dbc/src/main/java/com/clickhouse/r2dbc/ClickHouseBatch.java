package com.clickhouse.r2dbc;

import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseRequest;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

public class ClickHouseBatch implements Batch {

    private static final ClickHouseFormat PREFERRED_FORMAT = ClickHouseFormat.TabSeparatedWithNamesAndTypes;
    private ClickHouseRequest<?> request;
    List<String> sqlList = new ArrayList<>();

    public ClickHouseBatch(ClickHouseRequest request) {
        this.request = request;
    }

    @Override
    public Batch add(String sql) {
        sqlList.add(sql);
        return this;
    }

    @Override
    public Publisher<? extends Result> execute() {
        return Flux.fromStream(sqlList.stream().map(sql -> {
            request.query(sql).format(PREFERRED_FORMAT);
            return Mono.fromFuture(request::execute); }))
                .flatMap(Mono::flux)
                .map(ClickHouseResult::new);
    }
}
