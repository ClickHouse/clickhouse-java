package com.clickhouse.client.datatypes;

import com.clickhouse.client.api.metadata.DefaultColumnToMethodMatchingStrategy;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.serde.POJOSerDe;
import com.clickhouse.client.datatypes.DataSetPojoGenerator.DataSetPojo;
import com.clickhouse.client.datatypes.readers.ReaderValues;
import com.clickhouse.client.testdata.GenerateTestDataSet;
import com.clickhouse.client.testdata.GenerateTestDataSet.DataSetEntry;
import com.clickhouse.client.testdata.TestDataSet;
import com.clickhouse.data.ClickHouseColumn;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class DataSetPojoGeneratorTest {

    @DataProvider
    public Object[][] storableEntries() {
        List<DataSetEntry> entries = GenerateTestDataSet.readDataSetIndex();
        return entries.stream()
                .filter(DataSetEntry::isStorable)
                .map(e -> new Object[] {e})
                .toArray(Object[][]::new);
    }

    @Test(groups = {"unit"}, dataProvider = "storableEntries")
    public void testGeneratePojoForDataSetEntry(DataSetEntry entry) {
        TestDataSet dataSet = TestDataSet.of(GenerateTestDataSet.parseType(entry.getType()));
        DataSetPojo pojo = DataSetPojoGenerator.forDataSet(entry);

        TableSchema schema = new TableSchema(entry.getName(), null, null, Arrays.asList(
                ClickHouseColumn.of("id", "Int32"),
                ClickHouseColumn.of("value", entry.getClickHouseType())));
        POJOSerDe serDe = new POJOSerDe(DefaultColumnToMethodMatchingStrategy.INSTANCE);
        serDe.registerClass(pojo.getPojoClass(), schema);
        Assert.assertEquals(serDe.getFieldSerializers(pojo.getPojoClass(), schema).size(), 2);
        Assert.assertEquals(serDe.getFieldDeserializers(pojo.getPojoClass(), schema).size(), 2);

        List<Object> instances = pojo.toPojoList(dataSet);
        List<Object[]> rows = dataSet.getRows();
        Assert.assertEquals(instances.size(), rows.size());
        for (int i = 0; i < rows.size(); i++) {
            Object instance = instances.get(i);
            Assert.assertEquals(pojo.getId(instance), ((Integer) rows.get(i)[0]).intValue());
            Assert.assertTrue(ReaderValues.deepEquals(
                    ReaderValues.normalize(rows.get(i)[1]),
                    ReaderValues.normalize(pojo.getValue(instance))));
        }

        Class<?> primitiveType = DataSetPojoGenerator.primitiveJavaTypeFor(entry);
        if (primitiveType != null) {
            DataSetPojo primitivePojo = DataSetPojoGenerator.forDataSet(entry, int.class, primitiveType);
            serDe.registerClass(primitivePojo.getPojoClass(), schema);
            Assert.assertEquals(serDe.getFieldSerializers(primitivePojo.getPojoClass(), schema).size(), 2);
            Assert.assertEquals(serDe.getFieldDeserializers(primitivePojo.getPojoClass(), schema).size(), 2);
            Object instance = primitivePojo.newInstance((Integer) rows.get(0)[0], rows.get(0)[1]);
            Assert.assertTrue(ReaderValues.deepEquals(
                    ReaderValues.normalize(rows.get(0)[1]),
                    ReaderValues.normalize(primitivePojo.getValue(instance))));
        }
    }
}
