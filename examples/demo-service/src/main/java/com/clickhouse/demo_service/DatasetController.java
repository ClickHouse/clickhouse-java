package com.clickhouse.demo_service;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.demo_service.data.VirtualDatasetRecord;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Dataset API:
 * - /direct/dataset/0/?limit=N - uses client v2 directly to fetch N rows from a virtual dataset.
 *
 * <p>Example: {@code  curl -v http://localhost:8080/direct/dataset/0?limit=10}</p>
 */
@RestController
@RequestMapping("/")
@Log
public class DatasetController {

    private Client chDirectClient;

    public DatasetController(Client chDirectClient) {
        this.chDirectClient = chDirectClient;
    }

    private static final String DATASET_QUERY =
            "SELECT generateUUIDv4() as id, " +
            "toUInt32(number) as p1, " +
            "number,  " +
            "toFloat32(number/100000) as p2, " +
            "toFloat64(number/100000) as p3" +
                    " FROM system.numbers";


    @GetMapping("/direct/dataset/0")
    public List<VirtualDatasetRecord> directDatasetFetch(@RequestParam(name = "limit", required = false) Integer limit) {
        limit = limit == null ? 100 : limit;

        final String query = DATASET_QUERY + " LIMIT " + limit;
        try (Records records = chDirectClient.queryRecords(query).get(3000, TimeUnit.MILLISECONDS)) {
            ArrayList<VirtualDatasetRecord> result = new ArrayList<>();

            long start = System.nanoTime();
            for (GenericRecord record : records) {
                result.add(new VirtualDatasetRecord(
                        record.getUUID("id"),
                        record.getLong("p1"),
                        record.getBigInteger("number"),
                        record.getFloat("p2"),
                        record.getDouble("p3")
                ));
            }
            long duration = System.nanoTime() - start;
            log.info("Read " + result.size() + " records in " + TimeUnit.NANOSECONDS.toMillis(duration) + "ms. Client time " + records.getMetrics()
                    .getMetric(ClientMetrics.OP_DURATION).getLong() + " ms" + " server time " + (TimeUnit.NANOSECONDS.toMillis(records.getServerTime())) + " ms");
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch dataset", e);
        }
    }
}
