package ru.yandex.clickhouse.response;


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

        Assert.assertEquals("string", resultSet.getColumnNames()[0]);
        Assert.assertEquals("int", resultSet.getColumnNames()[1]);

        Assert.assertEquals("String", resultSet.getTypes()[0]);
        Assert.assertEquals("UInt32", resultSet.getTypes()[1]);

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