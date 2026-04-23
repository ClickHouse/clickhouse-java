package com.clickhouse.examples.arrow_format;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;
import java.util.stream.IntStream;

/**
 *
 * Arrow requires access to direct memory and "unsafe" JDK API. Next JVM parameters should be set to allow this.
 * For more info read Apache Arrow manual.
 * {@code
 * --add-opens=java.base/java.nio=ALL-UNNAMED
 * --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
 * --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
 * }
 *
 */
public class ReadWriteArrow implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ReadWriteArrow.class);

    private static final int ROOT_ALLOCATOR_LIMIT = 10 * 1024 * 1024;

    RootAllocator rootAllocator = new RootAllocator(ROOT_ALLOCATOR_LIMIT);

    ReadWriteArrow() {

    }

    private  void loadData(Client client) {
        LOG.info("Loading data to table using arrow");

        // memory allocator to store values on local machine. Read more https://arrow.apache.org/java/current/memory.html


        // Create value holders. Each column is Vector. In current example we want to send measures along with timestamps
        // We allocate all needed space just for simplicity.
        final int numValuesInBatch = 10_000;
        TimeStampVector  tsVector = new TimeStampMilliVector("ts", rootAllocator);

        Decimal256Vector val1Vector = new Decimal256Vector("val1", rootAllocator, 76, 39);

        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(tsVector, val1Vector);

        final String table = "arrow_example";
        executeUpdate("CREATE TABLE IF NOT EXISTS " + table + "(ts DateTime64, val1 Decimal(76,39)) Engine = MergeTree Order By()", client);
        executeUpdate("TRUNCATE " + table, client);

        LOG.info("Generating data");
        final long startTimestamp = System.currentTimeMillis();


        IntStream.range(0, numValuesInBatch)
                .forEachOrdered(index -> {
                    try {
                        tsVector.setSafe(index, startTimestamp + index);
                        val1Vector.setSafe(index, randomBigDecimal(76, 39));
                    } catch (Exception e) {
                        LOG.error("Failed at " + index, e);
                        throw new RuntimeException(e);
                    }
                });

        tsVector.setValueCount(numValuesInBatch);
        val1Vector.setValueCount(numValuesInBatch);

        // If you see Because of Code: 33. DB::Exception: Error while reading batch of Arrow data: IOError: Array length did not match record batch length: While executing ArrowBlockInputFormat.
        // May be missing `vectorSchemaRoot.setRowCount`
        vectorSchemaRoot.setRowCount(numValuesInBatch);

        InsertSettings insertSettings = new InsertSettings();
        insertSettings.compressClientRequest(true);
        try (InsertResponse response = client.insert(table, out -> {
            // use DataWriter to avoid tmp storage.
            try (ArrowWriter arrowWriter = new ArrowStreamWriter(vectorSchemaRoot, /* provider = */ null, out)) {
                arrowWriter.start();
                arrowWriter.writeBatch();
                arrowWriter.end();

            } catch (Exception e) {
                LOG.error("Failed writing data to output stream", e);
            }
        }, ClickHouseFormat.ArrowStream, insertSettings).get()) {
            LOG.info("Data inserted {}", response.getWrittenRows());
        } catch (Exception e) {
            LOG.error("Failed to write data to DB", e);
        } finally {
            vectorSchemaRoot.close(); // free memory
        }
    }

    private void readData(Client client) {


        // Prepare data
        final String table = "arrow_read_example";
        final String tableCopy = table + "_copy";
        executeUpdate("CREATE TABLE IF NOT EXISTS " + table + "(ts DateTime(3), val1 Decimal(76,62)) Engine = MergeTree Order By()", client);
        executeUpdate("CREATE TABLE IF NOT EXISTS " + tableCopy + "(ts DateTime(3), val1 Decimal(76,62)) Engine = MergeTree Order By()", client);
        executeUpdate("TRUNCATE " + table, client);
        executeUpdate("TRUNCATE " + tableCopy, client);

        long tsStart = System.currentTimeMillis();
        int nRows = 10;

        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
        sqlBuilder.append(table).append(" VALUES ");
        IntStream.range(0, nRows)
                .forEachOrdered(index -> {
                    sqlBuilder.append('(')
                            .append(tsStart + index)
                            .append(',')
                            .append(randomBigDecimal(76, 62))
                            .append(')').append(',');
                });

        sqlBuilder.setLength(sqlBuilder.length() - 1);
        executeUpdate(sqlBuilder.toString(), client);


        // Init Arrow Vectors
        // memory allocator to store values on local machine. Read more https://arrow.apache.org/java/current/memory.html
        try (QueryResponse resp = client.query("SELECT * FROM " + table + " LIMIT "  +nRows +" FORMAT ArrowStream").get()) {

            // It is important to close reader to release memory.
            // The vectorSchemaRoot and rootAllocator should be closed when it is not used anymore, too
            try (ArrowReader arrowReader = new ArrowStreamReader(resp.getInputStream(), rootAllocator)) {
                VectorSchemaRoot vectorSchemaRoot = arrowReader.getVectorSchemaRoot();
                FieldVector tsVector = vectorSchemaRoot.getVector("ts");
                FieldVector val1Vector = vectorSchemaRoot.getVector("val1");

                while (arrowReader.loadNextBatch()) {
                    LOG.info("tsVector[{}], val1Vector[{}]", tsVector.getValueCount(), val1Vector.getValueCount());
                    // We assume both vectors have same number of values

                    // Copy data to another table using ArrowStream input format
                    InsertSettings insertSettings = new InsertSettings();
                    try (InsertResponse insert = client.insert(tableCopy, outputStream -> {
                        try (ArrowWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, outputStream)) {
                            writer.start();
                            writer.writeBatch();
                            writer.end();
                        }

                    }, ClickHouseFormat.ArrowStream, insertSettings).get()) {
                        LOG.info("Inserted in {} ns", insert.getServerTime());
                    }
                }

            }
        } catch (Exception e) {
            LOG.error("Failed to query", e);
        }
    }

    private static final Random RND = new Random();

    private static void executeUpdate(String sql, Client client) {
        try (CommandResponse r = client.execute(sql).get()) {} catch (
                Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static BigDecimal randomBigDecimal(int precision, int scale) {
        if (precision <= 0) throw new IllegalArgumentException("precision must be > 0");
        if (scale < 0 || scale > precision) throw new IllegalArgumentException("scale must be in [0, precision]");
        // first digit 1..9 so precision is exact
        StringBuilder digits = new StringBuilder();
        digits.append(1 + RND.nextInt(9));
        for (int i = 1; i < precision; i++) {
            digits.append(RND.nextInt(10));
        }
        BigInteger unscaled = new BigInteger(digits.toString());
        return new BigDecimal(unscaled, scale);
    }



    public static void main(String ...args) {
        final String endpoint = System.getProperty("chEndpoint", "http://localhost:8123");
        final String user = System.getProperty("chUser", "default");
        final String password = System.getProperty("chPassword", "");
        final String database = System.getProperty("chDatabase", "default");


        try (ReadWriteArrow app = new ReadWriteArrow();
             Client chClient = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(password)
                .setDefaultDatabase(database)
                .build()) {

            app.loadData(chClient);
            app.readData(chClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        rootAllocator.close();
    }
}
