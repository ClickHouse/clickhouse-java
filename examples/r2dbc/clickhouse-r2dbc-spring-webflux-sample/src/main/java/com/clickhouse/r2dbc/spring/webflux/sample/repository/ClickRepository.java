package com.clickhouse.r2dbc.spring.webflux.sample.repository;

import com.clickhouse.r2dbc.spring.webflux.sample.model.Click;
import com.clickhouse.r2dbc.spring.webflux.sample.model.ClickStats;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;



@Repository
public class ClickRepository {

    @Autowired
    ConnectionFactory connectionFactory;

    public Flux<ClickStats> getStatsByDomain(String domain){
        return Mono.from(connectionFactory.create())
                .flatMapMany(conn -> conn.createStatement("select domain, path,  toDate(cdate) as d, count(1) as count from clickdb.clicks where domain = :domain group by domain, path, d")
                        .bind("domain", domain)
                        .execute())
                .flatMap(result -> result.map((row, rowMetadata) -> new ClickStats(row
                        .get("domain", String.class), row.get("path", String.class), row.get("d", LocalDate.class),  row.get("count", Long.class))));
    }

    public Mono<Void> add(Click click){
        return Mono.from(connectionFactory.create())
                .flatMapMany(conn -> execute(click, conn)).then();
    }

    private Publisher<? extends Result> execute(Click click, Connection conn) {
        return conn.createStatement("insert into clickdb.clicks values (:domain, :path, :cdate, :count)")
        .bind("domain", click.getDomain())
        .bind("path", click.getPath())
                .bind("cdate", LocalDateTime.now())
                .bind("count", 1).execute();
    }

}
