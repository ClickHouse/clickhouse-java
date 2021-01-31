package ru.yandex.clickhouse.response;


import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseResultBuilderTest {

    @Test
    public void testBuild() throws Exception {
        ClickHouseResultSet resultSet = ClickHouseResultBuilder.builder(2)
            .names("string", "int")
            .types("String", "UInt32")
            .addRow("ololo", 1000)
            .addRow("o\tlo\nlo", 1000)
            .addRow(null, null)
            .build();
        List<ClickHouseColumnInfo> columns = resultSet.getColumns();

        Assert.assertEquals("string", columns.get(0).getColumnName());
        Assert.assertEquals("int", columns.get(1).getColumnName());

        Assert.assertEquals("String", columns.get(0).getOriginalTypeName());
        Assert.assertEquals("UInt32", columns.get(1).getOriginalTypeName());

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("ololo", resultSet.getString(1));
        Assert.assertEquals(1000, resultSet.getInt(2));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("o\tlo\nlo", resultSet.getString(1));
        Assert.assertEquals(1000, resultSet.getInt(2));

        Assert.assertTrue(resultSet.next());
        Assert.assertNull(resultSet.getString(1));
        Assert.assertEquals(0, resultSet.getInt(2));

        Assert.assertFalse(resultSet.next());

    }
}