package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.data_formats.ClickHouseFormatReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.datatypes.readers.ReaderExpectations.Expected;
import com.clickhouse.client.testdata.DataTypeGenerator;
import com.clickhouse.client.testdata.GenerateTestDataSet;
import com.clickhouse.client.testdata.TestDataSet;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import org.testng.Assert;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Reads the data set of every type in {@link GenerateTestDataSet#getStorableTypes()} with a format reader constructed
 * by {@link #createReader(QueryResponse, TableSchema)} and verifies every accessor of {@link ClickHouseFormatReader}
 * for every row, by column index and by name, against {@link ReaderExpectations}. Deviations listed in
 * {@link ReaderKnownIssues} are allowed; known issues that do not happen anymore fail
 * {@link #testKnownIssuesAreCurrent()}.
 */
public abstract class AbstractBinaryReaderDataSetTest extends AbstractAccessorDataSetTest {

    /**
     * @return format requested from ClickHouse for queries in this test
     */
    protected abstract ClickHouseFormat getFormat();

    /**
     * Constructs the reader under test for the given query response and table schema.
     */
    protected abstract ClickHouseFormatReader createReader(QueryResponse response, TableSchema schema)
            throws Exception;

    @Override
    protected String getSubjectName() {
        return getFormat().name() + " reader";
    }

    @Override
    protected List<KnownIssue> getKnownIssues() {
        return ReaderKnownIssues.ALL;
    }

    @Override
    protected boolean issueApplies(KnownIssue issue) {
        return issue.appliesTo(getFormat());
    }

    @Override
    protected boolean issueMatches(KnownIssue issue, String type, DeviationSummary deviation) {
        return issue.matches(getFormat(), type, deviation.accessor, deviation.deviation, deviation.nullValue,
                deviation.thrown);
    }

    @Override
    protected Map<String, DeviationSummary> readAndVerify(TestDataSet dataSet, String table,
            Map<String, String> settings) throws Exception {
        Map<String, DeviationSummary> deviations = new LinkedHashMap<>();
        QuerySettings querySettings = new QuerySettings().setFormat(getFormat());
        settings.forEach(querySettings::serverSetting);
        TableSchema tableSchema = getClient().getTableSchema(table);
        DataTypeGenerator generator = dataSet.getColumns().get(VALUE_COLUMN);
        List<Object[]> rows = dataSet.getRows();
        int rowCount = 0;
        try (QueryResponse response = getClient().query("SELECT * FROM " + table + " ORDER BY id", querySettings)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
             ClickHouseFormatReader reader = createReader(response, tableSchema)) {
            ReaderExpectations expectations = null;
            while (reader.hasNext()) {
                Map<String, Object> record = reader.next();
                if (expectations == null) {
                    ClickHouseColumn column = reader.getSchema().getColumnByName(VALUE_COLUMN);
                    expectations = new ReaderExpectations(column, generator);
                }
                Assert.assertTrue(rowCount < rows.size(), "More rows than in the data set: " + rows.size());
                Object[] row = rows.get(rowCount++);
                Assert.assertEquals(reader.getInteger(1), row[0], "Rows are read out of order");
                verifyRow(reader, record, expectations, row, deviations);
            }
        } catch (Exception e) {
            deviations.put("read", readFailure(rowCount, e));
            return deviations;
        }
        Assert.assertEquals(rowCount, rows.size(), "Number of rows read");
        return deviations;
    }

    private void verifyRow(ClickHouseFormatReader reader, Map<String, Object> record,
            ReaderExpectations expectations, Object[] row, Map<String, DeviationSummary> deviations) {
        Object value = row[1];
        Object raw = reader.readValue(VALUE_INDEX);
        Assert.assertSame(reader.readValue(VALUE_COLUMN), raw, "readValue by name and by index");
        Assert.assertSame(record.get(VALUE_COLUMN), raw, "record returned by next() and readValue");
        Assert.assertEquals(reader.hasValue(VALUE_INDEX), value != null, "hasValue by index for row " + row[0]);
        Assert.assertEquals(reader.hasValue(VALUE_COLUMN), value != null, "hasValue by name for row " + row[0]);

        for (ReaderAccessor accessor : ReaderAccessor.values()) {
            Expected expected = expectations.expected(accessor, value);
            verify(accessor, expected, row, "index", () -> accessor.get(reader, VALUE_INDEX), deviations);
            verify(accessor, expected, row, "name", () -> accessor.get(reader, VALUE_COLUMN), deviations);
        }
    }
}
