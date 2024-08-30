package com.clickhouse.demo_service;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.demo_service.data.VirtualDatasetRecord;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.java.Log;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
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

    private final Client chDirectClient;

    private static final int MAX_LIMIT = 100_000;

    private BasicObjectsPool<ObjectsPreparedCollection<VirtualDatasetRecord>> pool;

    public DatasetController(Client chDirectClient) {
        this.chDirectClient = chDirectClient;
    }

    @PostConstruct
    public void setup() {
        chDirectClient.ping(3000); // helps to warm up the connection

        // Register class for deserialization
        TableSchema datasetQuerySchema = chDirectClient.getTableSchemaFromQuery(DATASET_QUERY, "virtual_table1");
        chDirectClient.register(VirtualDatasetRecord.class, datasetQuerySchema);
        log.info("Dataset schema: " + datasetQuerySchema.getColumns());

        pool = new BasicObjectsPool<>(new ConcurrentLinkedDeque<>(), 100) {
            @Override
            ObjectsPreparedCollection<VirtualDatasetRecord> create() {
                return new ObjectsPreparedCollection<>(new LinkedList<>(), MAX_LIMIT) {
                    @Override
                    VirtualDatasetRecord create() {
                        return new VirtualDatasetRecord();
                    }
                };
            }
        };
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
            ClickHouseBinaryFormatReader reader = Client.newBinaryFormatReader(response);

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

    /**
     * Using POJO deserialization to fetch data from ClickHouse. Also using a objects cache
     * to avoid objects creation on each iteration.
     *
     * @param limit
     * @return
     */
    @GetMapping("/direct/dataset/cached_objects")
    public CalculationResult directDatasetFetchCached(@RequestParam(name = "limit", required = false) Integer limit) {
        limit = limit == null ? 100 : limit;

        final String query = DATASET_QUERY + " LIMIT " + limit;
        List<VirtualDatasetRecord> result = null;
        ObjectsPreparedCollection<VirtualDatasetRecord> objectsPool = this.pool.lease(); // take object from the pool
        try  {
            long start = System.nanoTime();

            result = chDirectClient.queryAll(query, VirtualDatasetRecord.class, objectsPool);
            long duration = System.nanoTime() - start;
            log.info("records: " + result.size() + ", read time: " + TimeUnit.NANOSECONDS.toMillis(duration) + " ms");
            long p1Sum = 0;
            for (VirtualDatasetRecord record : result) {
                p1Sum += record.getP1();
            }
            objectsPool.reset(); // reset pool to for next use
            return new CalculationResult(p1Sum);
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch dataset", e);
        } finally {
            this.pool.release(objectsPool);
        }
    }
}
