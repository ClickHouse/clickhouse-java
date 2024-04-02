package com.clickhouse.client.api.query;

import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.data_formats.DataFormat;
import com.clickhouse.data.ClickHouseRecord;

import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Response class provides interface to input stream of response data.
 * <br/>
 * It is used to read data from ClickHouse server.
 * It is used to get response metadata like errors, warnings, etc.
 *
 * This class is for the following user cases:
 * <ul>
 *     <li>Full read. User does conversion from record to custom object</li>
 *     <li>Full read. No conversion to custom object. List of generic records is returned. </li>
 *     <li>Iterative read. One record is returned at a time</li>
 * </ul>
 *
 *
 */
public class QueryResponse<TOutDataFormat extends DataFormat> {

    private final Future<ClickHouseResponse> responseRef;

    public QueryResponse(Future<ClickHouseResponse> responseRef) {
        this.responseRef = responseRef;
    }

    public boolean isDone() {
        return responseRef.isDone();
    }

    /**
     * Read full dataset from response. It is blocking operation.
     * @param consumer a function to convert record to custom object and storing somewhere
     */
    public void readFull(Consumer<ClickHouseRecord> consumer) {
        try (ClickHouseResponse response = responseRef.get()) {
            response.records().forEach(consumer); // very simple implementation
        } catch (Exception e) {
            throw new RuntimeException(e); // TODO: handle exception
        }
    }

    /**
     * Read full dataset from response. It is blocking operation.
     *
     * @return a list of records
     */
    public Iterable<ClickHouseRecord> readFull() {
        try (ClickHouseResponse response = responseRef.get()) {
            return response.records(); // very simple implementation
        } catch (Exception e) {
            throw new RuntimeException(e); // TODO: handle exception
        }
    }


    /**
     * Read one record from response. It is blocking operation.
     *
     * @return a record
     */
    public ClickHouseRecord readOne() {
        if (recordsIterator == null) {
            try (ClickHouseResponse response = responseRef.get()) {
                recordsIterator = response.records().iterator(); // very simple implementation
            } catch (Exception e) {
                throw new RuntimeException(e); // TODO: handle exception
            }
        }

        return recordsIterator.next();
    }

    private Iterator<ClickHouseRecord> recordsIterator;
}
