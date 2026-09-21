package com.clickhouse.examples.service;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.examples.model.SignalSlice;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Builds a row-oriented, one-second view of signal means by location.
 */
@Service
public class SignalSliceService {

    private static final String SELECT_SLICES = """
            SELECT
                location_id,
                toStartOfSecond(event_time) AS window_start,
                if(countIf(signal_type = 'TEMPERATURE') = 0, NULL,
                   avgIf(value, signal_type = 'TEMPERATURE')) AS temperature,
                if(countIf(signal_type = 'HUMIDITY') = 0, NULL,
                   avgIf(value, signal_type = 'HUMIDITY')) AS humidity,
                if(countIf(signal_type = 'PRESSURE') = 0, NULL,
                   avgIf(value, signal_type = 'PRESSURE')) AS pressure,
                if(countIf(signal_type = 'MOTION') = 0, NULL,
                   avgIf(value, signal_type = 'MOTION')) AS motion,
                if(countIf(signal_type = 'GAS') = 0, NULL,
                   avgIf(value, signal_type = 'GAS')) AS gas,
                if(countIf(signal_type = 'BATTERY') = 0, NULL,
                   avgIf(value, signal_type = 'BATTERY')) AS battery,
                if(countIf(signal_type = 'LIGHT') = 0, NULL,
                   avgIf(value, signal_type = 'LIGHT')) AS light
            FROM iot_signals
            WHERE event_time >= now64(3) - toIntervalMillisecond({lookbackMillis:UInt64})
              AND event_time < now64(3)
            %s
            GROUP BY location_id, window_start
            ORDER BY window_start DESC, location_id
            """;

    private final Client client;

    public SignalSliceService(Client client) {
        this.client = client;
    }

    public List<SignalSlice> findSlices(Duration lookback, UUID locationId) {
        Map<String, Object> parameters;
        String locationClause = "";
        if (locationId != null) {
            locationClause = "AND location_id = {locationId:UUID}";
            parameters = Map.of(
                    "lookbackMillis", lookback.toMillis(),
                    "locationId", locationId);
        } else {
            parameters = Map.of("lookbackMillis", lookback.toMillis());
        }

        return client.queryAll(SELECT_SLICES.formatted(locationClause), parameters).stream()
                .map(this::toSignalSlice)
                .toList();
    }

    private SignalSlice toSignalSlice(GenericRecord record) {
        return new SignalSlice(
                record.getUUID("location_id"),
                record.getInstant("window_start"),
                nullableDouble(record, "temperature"),
                nullableDouble(record, "humidity"),
                nullableDouble(record, "pressure"),
                nullableDouble(record, "motion"),
                nullableDouble(record, "gas"),
                nullableDouble(record, "battery"),
                nullableDouble(record, "light"));
    }

    private Double nullableDouble(GenericRecord record, String column) {
        return record.hasValue(column) ? record.getDouble(column) : null;
    }
}
