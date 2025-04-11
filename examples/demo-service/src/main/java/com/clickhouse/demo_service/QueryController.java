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
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Class demonstrates using ClickHouse client directly from a service.
 * It avoids JDBC overhead and much easier to use.
 * Data may be streamed from database directly to the service response.
 */
@RestController
@RequestMapping("/dataset")
@Log
public class QueryController {

    private final Client chDirectClient;

    private static final int MAX_LIMIT = 100_000;

    private BasicObjectsPool<ObjectsPreparedCollection<VirtualDatasetRecord>> pool;

    public QueryController(Client chDirectClient) {
        this.chDirectClient = chDirectClient;
    }

    public TableSchema datasetQuerySchema;

    @PostConstruct
    public void setup() {
        chDirectClient.ping(3000); // helps to warm up the connection

        // Register class for deserialization
        datasetQuerySchema = chDirectClient.getTableSchemaFromQuery(DATASET_QUERY);
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
     * Fetches data from a DB using row binary reader. VirtualDatasetRecord objects are created on each iteration and
     * filled with data from the reader. Gives full control on how data is processed and stored.
     * If this method returns a lot of data it may cause application slowdown.
     *
     * @param limit
     * @return
     */
    @GetMapping("/reader")
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
            log.info(String.format("records: %d, read time: %d ms (%d bytes), client time: %d ms, server time: %d ms",
                    result.size(), TimeUnit.NANOSECONDS.toMillis(duration), response.getReadBytes(),
                    response.getMetrics().getMetric(ClientMetrics.OP_DURATION).getLong(),
                    TimeUnit.NANOSECONDS.toMillis(response.getServerTime())));

            return result.stream().findFirst().stream().collect(Collectors.toCollection(ArrayList::new));
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch dataset", e);
        }
    }

    private JsonMapper jsonMapper = new JsonMapper();

    /**
     * Reads data in JSONEachRow format, parses it into JSON library object (can be used for further processing) and
     * writes it back to the response.
     * This helps to reduce effort of writing data to the response.
     *
     * @param httpResp
     * @param limit
     */
    @GetMapping("/json_each_row_in_and_out")
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
     * Using POJO deserialization to fetch data from ClickHouse.
     * If cache is enabled, objects are reused from the pool otherwise new objects are created on each iteration.
     *
     * @param limit
     * @return
     */
    @GetMapping("/read_to_pojo")
    public CalculationResult directDatasetReadToPojo(@RequestParam(name = "limit", required = false) Integer limit,
                                                     @RequestParam(name = "cache", required = false) Boolean cache) {
        limit = limit == null ? 100 : limit;
        cache = cache != null && cache;
        return readToPOJO(limit, cache);
    }

    private CalculationResult readToPOJO(int limit, boolean cache) {
        final String query = DATASET_QUERY + " LIMIT " + limit;
        List<VirtualDatasetRecord> result = null;
        Supplier<VirtualDatasetRecord> objectsPool = cache ? this.pool.lease()
                : VirtualDatasetRecord::new;
        try  {
            long start = System.nanoTime();

            result = chDirectClient.queryAll(query, VirtualDatasetRecord.class, datasetQuerySchema, objectsPool);
            long duration = System.nanoTime() - start;
            log.info("records: " + result.size() + ", read time: " + TimeUnit.NANOSECONDS.toMillis(duration) + " ms");
            long p1Sum = 0;
            for (VirtualDatasetRecord record : result) {
                p1Sum += record.getP1();
            }
            if (cache) {
                ((ObjectsPreparedCollection<VirtualDatasetRecord>) objectsPool).reset();
            }
            return new CalculationResult(p1Sum);
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch dataset", e);
        } finally {
            if (cache) {
                this.pool.release((ObjectsPreparedCollection<VirtualDatasetRecord>) objectsPool);
            }
        }
    }
}
