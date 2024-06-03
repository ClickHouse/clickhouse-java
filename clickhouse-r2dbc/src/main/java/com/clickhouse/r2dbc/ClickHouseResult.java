package com.clickhouse.r2dbc;

import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class ClickHouseResult implements Result {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseResult.class);

    private final Flux<? extends Result.Segment> segments;

    ClickHouseResult(ClickHouseResponse response) {
        Flux<? extends RowSegment> rowSegments = Mono.just(response)
                .flatMapMany(resp -> Flux
                        .fromStream(StreamSupport.stream(resp.records().spliterator(), false)
                                .map(rec -> ClickHousePair.of(resp.getColumns(), rec))))
                    .map(pair -> new ClickHouseRow(pair.getRight(), pair.getLeft()))
                .map(RowSegment::new);
        Mono<? extends UpdateCount> updatedCount =  Mono.just(response).map(ClickHouseResponse::getSummary)
                .map(ClickHouseResponseSummary::getProgress)
                .map(ClickHouseResponseSummary.Progress::getWrittenRows)
                .map(UpdateCount::new);
        this.segments = Flux.concat(updatedCount, rowSegments).doOnComplete(response::close);
    }

    ClickHouseResult(Flux<? extends Result.Segment> rowSegments) {
        this.segments = rowSegments;
    }

    /**
     * Returns updated count(written rows from summary of {@link ClickHouseResponse}).Important! if writtenRows is greater than MAX_INT then it will return MAX_INT.
     *
     * @return updated count
     */
    @Override
    public Mono<Long> getRowsUpdated() {
        return this.segments.filter(segment -> segment instanceof UpdateCount)
                .cast(UpdateCount.class)
                .map(UpdateCount::value)
                .reduce(Long::sum);
    }

    @Override
    public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> biFunction) {
        return this.segments.filter(segment -> segment instanceof RowSegment).cast(RowSegment.class)
                .map(RowSegment::row).handle((row, sink) -> {
            try {
                sink.next(biFunction.apply(row, row.getMetadata()));
            } catch (Exception e) {
                log.error("Provided function caused exception:", e);
            }
        });
    }

    @Override
    public Result filter(Predicate<Segment> predicate) {
        return new ClickHouseResult(segments.filter(predicate));
    }

    @Override
    public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> function) {
        return segments.flatMap(segment -> {
            try {
                Publisher<? extends T> retValue = function.apply(segment);
                if (retValue == null) {
                    return Mono.error(new IllegalStateException("flatmap function returned null value"));
                }
                return retValue;
            } catch (Exception e) {
                log.error("Provided function caused exception:", e);
                return Mono.error(e);
            }
        });
    }


    class RowSegment implements Result.RowSegment {

        final ClickHouseRow row;

        RowSegment(ClickHouseRow row) {
            this.row = row;
        }

        @Override
        public Row row() {
            return row;
        }
    }

    class UpdateCount implements Result.UpdateCount {

        final long updateCount;

        UpdateCount(long updateCount) {
            this.updateCount = updateCount;
        }

        @Override
        public long value() {
            return updateCount;
        }
    }
}
