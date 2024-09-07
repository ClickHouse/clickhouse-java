package com.clickhouse.demo_service;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.demo_service.data.VirtualDatasetRecord;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.java.Log;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
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

    /**
     * Makes query to a {@code system.numbers} that can be used to generate a virtual dataset.
     * Size of the dataset is limited by the {@code limit} parameter.
     */
    private static final String DATASET_QUERY =
            "SELECT generateUUIDv4() as id, " +
            "toUInt32(number) as p1, " +
            "number,  " +
            "toFloat32(number/100000) as p2, " +
            "toFloat64(number/100000) as p3" +
                    " FROM system.numbers";

    /**
     * Common approach to fetch data from ClickHouse using client v2.
     *
     * @param limit
     * @return
     */
    @GetMapping("/direct/dataset/0")
    public List<VirtualDatasetRecord> directDatasetFetch(@RequestParam(name = "limit", required = false) Integer limit) {
        limit = limit == null ? 100 : limit;

        final String query = DATASET_QUERY + " LIMIT " + limit;
        try (QueryResponse response = chDirectClient.query(query).get(3000, TimeUnit.MILLISECONDS)) {
            ArrayList<VirtualDatasetRecord> result = new ArrayList<>();

            // iterable approach is more efficient for large datasets because it doesn't load all records into memory
            ClickHouseBinaryFormatReader reader = chDirectClient.newBinaryFormatReader(response);

            long start = System.nanoTime();
            while (reader.next() != null) {
                result.add(new VirtualDatasetRecord(
                        reader.getUUID("id"),
                        reader.getLong("p1"),
                        reader.getBigInteger("number"),
                        reader.getFloat("p2"),
                        reader.getDouble("p3")
                ));
            }
            long duration = System.nanoTime() - start;

            // report metrics (only for demonstration purposes)
            log.info(String.format("records: %d, read time: %d ms, client time: %d ms, server time: %d ms",
                    result.size(), TimeUnit.NANOSECONDS.toMillis(duration),
                    response.getMetrics().getMetric(ClientMetrics.OP_DURATION).getLong(),
                    TimeUnit.NANOSECONDS.toMillis(response.getServerTime())));

            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch dataset", e);
        }
    }

    private JsonMapper jsonMapper = new JsonMapper();

    /**
     * Current approach is to demonstrate how to 'stream' data from ClickHouse using JSONEachRow format.
     * This approach is faster than common one because it bypasses Spring internals and writes directly to http output stream.
     * @param httpResp
     * @param limit
     */
    @GetMapping("/direct/dataset/1")
    @ResponseBody
    public void directDataFetchJSONEachRow(HttpServletResponse httpResp, @RequestParam(name = "limit", required = false) Integer limit) {
        limit = limit == null ? 100 : limit;

        final String query = DATASET_QUERY + " LIMIT " + limit;
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.JSONEachRow);
        try (QueryResponse response = chDirectClient.query(query, settings).get(3000, TimeUnit.MILLISECONDS);
                // JSONEachRow format is a stream of JSON objects, so we need to parse them one by one
                MappingIterator<ObjectNode> jsonIter = jsonMapper.readerFor(ObjectNode.class)
                    .readValues(response.getInputStream())) {
            httpResp.setContentType("application/json");
            JsonGenerator jsonGen = jsonMapper.getFactory().createGenerator(httpResp.getOutputStream());

            jsonGen.writeStartArray();
            long start = System.nanoTime();
            int counter =0;
            while (jsonIter.hasNext()) {
                ObjectNode node = jsonIter.next();
                // here may be some processing logic
                node.put("ordNum", counter++);

            }
            jsonGen.writeEndArray();
            jsonGen.close();
            long duration = System.nanoTime() - start;

            // report metrics (only for demonstration purposes)
            log.info(String.format("records: %d, read time: %d ms, client time: %d ms, server time: %d ms",
                    counter, TimeUnit.NANOSECONDS.toMillis(duration),
                    response.getMetrics().getMetric(ClientMetrics.OP_DURATION).getLong(),
                    TimeUnit.NANOSECONDS.toMillis(response.getServerTime())));
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch dataset", e);
        }
    }
}
