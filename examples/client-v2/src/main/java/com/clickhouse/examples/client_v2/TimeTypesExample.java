package com.clickhouse.examples.client_v2;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.QueryResponse;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryFormatSerializer;
import com.clickhouse.data.ClickHouseFormat;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.LocalTime;
import java.util.List;

@Slf4j
public class TimeTypesExample {
    private final String endpoint;
    private final String user;
    private final String password;
    private final String database;

    public TimeTypesExample(String endpoint, String user, String password, String database) {
        this.endpoint = endpoint;
        this.user = user;
        this.password = password;
        this.database = database;
    }

    public void demonstrateTimeTypes() {
        log.info("=== Time Types Example ===");

        // Create table with Time and Time64 columns
        createTable();

        // Insert data using binary format
        insertTimeData();

        // Read data back
        readTimeData();

        log.info("Time types example completed");
    }

    private void createTable() {
        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s.time_types_example (" +
            "id UInt32, " +
            "time_col Time, " +
            "time64_col Time64(3), " +
            "description String" +
            ") ENGINE = MergeTree() ORDER BY id",
            database
        );

        try (ClickHouseClient client = ClickHouseClient.builder()
                .nodeSelector(ClickHouseNode.of(endpoint))
                .build();
             ClickHouseRequest request = client.read(createTableSql)
                     .format(ClickHouseFormat.RowBinary)) {

            request.executeAndWait();
            log.info("Table 'time_types_example' created successfully");
        } catch (Exception e) {
            log.error("Failed to create table", e);
        }
    }

    private void insertTimeData() {
        log.info("Inserting time data...");

        try (ClickHouseClient client = ClickHouseClient.builder()
                .nodeSelector(ClickHouseNode.of(endpoint))
                .build()) {

            // Prepare data
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            RowBinaryFormatSerializer serializer = new RowBinaryFormatSerializer(outputStream);

            // Sample data
            LocalTime[] times = {
                LocalTime.of(0, 0, 0),           // 00:00:00
                LocalTime.of(12, 0, 0),          // 12:00:00
                LocalTime.of(12, 34, 56),        // 12:34:56
                LocalTime.of(23, 59, 59),        // 23:59:59
                LocalTime.of(12, 34, 56, 123_456_789) // 12:34:56.123456789
            };

            String[] descriptions = {
                "Start of day",
                "Noon",
                "Specific time",
                "End of day",
                "Time with nanoseconds"
            };

            for (int i = 0; i < times.length; i++) {
                serializer.writeUnsignedInt32(i + 1); // id
                serializer.writeUnsignedInt32(times[i].toSecondOfDay()); // time_col (seconds since midnight)
                serializer.writeUnsignedInt64(times[i].toNanoOfDay()); // time64_col (nanoseconds since midnight)
                serializer.writeString(descriptions[i]); // description
            }

            // Insert data
            String insertSql = String.format(
                "INSERT INTO %s.time_types_example (id, time_col, time64_col, description) VALUES",
                database
            );

            try (ClickHouseRequest request = client.write(insertSql)
                    .format(ClickHouseFormat.RowBinary);
                 ClickHouseResponse response = request.data(new ByteArrayInputStream(outputStream.toByteArray())).executeAndWait()) {

                ClickHouseResponseSummary summary = response.getSummary();
                log.info("Inserted {} rows", summary.getWrittenRows());
            }

        } catch (Exception e) {
            log.error("Failed to insert time data", e);
        }
    }

    private void readTimeData() {
        log.info("Reading time data...");

        String query = String.format(
            "SELECT id, time_col, time64_col, description FROM %s.time_types_example ORDER BY id",
            database
        );

        try (Client client = Client.builder()
                .endpoint(endpoint)
                .username(user)
                .password(password)
                .database(database)
                .build()) {

            QueryResponse response = client.query(query).get();
            ClickHouseBinaryFormatReader reader = response.getBinaryReader();

            while (reader.hasNext()) {
                var record = reader.next();
                int id = record.getInt("id");
                LocalTime timeCol = record.getTime("time_col");
                LocalTime time64Col = record.getTime("time64_col");
                String description = record.getString("description");

                log.info("Row {}: Time={}, Time64={}, Description={}", 
                    id, timeCol, time64Col, description);
            }

        } catch (Exception e) {
            log.error("Failed to read time data", e);
        }
    }

    public void demonstrateTimeQueries() {
        log.info("=== Time Queries Example ===");

        String[] queries = {
            // Query to find times after noon
            "SELECT id, time_col, description FROM time_types_example WHERE time_col > 43200 ORDER BY time_col",
            
            // Query to find times with specific hour
            "SELECT id, time_col, description FROM time_types_example WHERE toHour(time_col) = 12 ORDER BY time_col",
            
            // Query to format time in different ways
            "SELECT id, time_col, toString(time_col) as time_str, description FROM time_types_example ORDER BY id",
            
            // Query to show time64 with precision
            "SELECT id, time64_col, toString(time64_col) as time64_str, description FROM time_types_example ORDER BY id"
        };

        try (Client client = Client.builder()
                .endpoint(endpoint)
                .username(user)
                .password(password)
                .database(database)
                .build()) {

            for (int i = 0; i < queries.length; i++) {
                log.info("Query {}: {}", i + 1, queries[i]);
                
                QueryResponse response = client.query(queries[i]).get();
                ClickHouseBinaryFormatReader reader = response.getBinaryReader();

                while (reader.hasNext()) {
                    var record = reader.next();
                    log.info("  Result: {}", record);
                }
                log.info("");
            }

        } catch (Exception e) {
            log.error("Failed to execute time queries", e);
        }
    }
} 