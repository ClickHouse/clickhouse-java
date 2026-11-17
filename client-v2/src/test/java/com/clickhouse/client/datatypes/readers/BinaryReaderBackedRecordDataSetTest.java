package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.data_formats.internal.BinaryReaderBackedRecord;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
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
 * Reads the data set of every type in {@link GenerateTestDataSet#getStorableTypes()} via
 * {@link com.clickhouse.client.api.Client#queryRecords(String, QuerySettings)} (producing
 * {@link BinaryReaderBackedRecord}) and verifies every accessor of {@link GenericRecord} by column index and by name
 * against {@link ReaderExpectations}.
 */
public class BinaryReaderBackedRecordDataSetTest extends AbstractAccessorDataSetTest {

    @Override
    protected String getSubjectName() {
        return "BinaryReaderBackedRecord";
    }

    @Override
    protected List<KnownIssue> getKnownIssues() {
        return ReaderKnownIssues.ALL;
    }

    @Override
    protected boolean issueApplies(KnownIssue issue) {
        return issue.appliesTo(ClickHouseFormat.RowBinaryWithNamesAndTypes);
    }

    @Override
    protected boolean issueMatches(KnownIssue issue, String type, DeviationSummary deviation) {
        return issue.matches(ClickHouseFormat.RowBinaryWithNamesAndTypes, type, deviation.accessor,
                deviation.deviation, deviation.nullValue, deviation.thrown);
    }

    @Override
    protected Map<String, DeviationSummary> readAndVerify(TestDataSet dataSet, String table,
            Map<String, String> settings) throws Exception {
        Map<String, DeviationSummary> deviations = new LinkedHashMap<>();
        QuerySettings querySettings = new QuerySettings();
        settings.forEach(querySettings::serverSetting);
        DataTypeGenerator generator = dataSet.getColumns().get(VALUE_COLUMN);
        List<Object[]> rows = dataSet.getRows();
        int rowCount = 0;
        try (Records records = getClient().queryRecords("SELECT * FROM " + table + " ORDER BY id", querySettings)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            Assert.assertEquals(records.isEmpty(), rows.isEmpty(), "isEmpty()");
            ReaderExpectations expectations = null;
            for (GenericRecord record : records) {
                Assert.assertTrue(record instanceof BinaryReaderBackedRecord,
                        "Expected BinaryReaderBackedRecord but got " + record.getClass());
                if (expectations == null) {
                    ClickHouseColumn column = record.getSchema().getColumnByName(VALUE_COLUMN);
                    expectations = new ReaderExpectations(column, generator);
                }
                Assert.assertTrue(rowCount < rows.size(), "More rows than in the data set: " + rows.size());
                Object[] row = rows.get(rowCount++);
                Assert.assertEquals(record.getInteger(1), row[0], "Rows are read out of order");
                verifyRow(record, expectations, row, deviations);
            }
        } catch (Exception e) {
            deviations.put("read", readFailure(rowCount, e));
            return deviations;
        }
        Assert.assertEquals(rowCount, rows.size(), "Number of rows read");
        return deviations;
    }

    private static void verifyRow(GenericRecord record, ReaderExpectations expectations, Object[] row,
            Map<String, DeviationSummary> deviations) {
        Object value = row[1];
        Object raw = record.getObject(VALUE_INDEX);
        Assert.assertSame(record.getObject(VALUE_COLUMN), raw, "getObject by name and by index");
        Assert.assertEquals(record.hasValue(VALUE_INDEX), value != null, "hasValue by index for row " + row[0]);
        Assert.assertEquals(record.hasValue(VALUE_COLUMN), value != null, "hasValue by name for row " + row[0]);
        if (value != null) {
            Assert.assertSame(record.getValues().get(VALUE_COLUMN), raw, "getValues() and getObject");
        }
        verifyRecordAccessors(record, expectations, row, deviations);
    }
}
