package com.clickhouse.jdbc.types;

import com.clickhouse.data.ClickHouseColumn;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

@Test(groups = {"unit"})
public class ArrayTest {

    @Test
    public void testToStringForArrayOfNamedTuples() throws SQLException {
        ClickHouseColumn column = ClickHouseColumn.of("value", "Array(Tuple(id UUID))");
        UUID id = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        Array array = new Array(column, new Object[] {new Object[] {id}});

        assertEquals(array.toString(), "[[550e8400-e29b-41d4-a716-446655440000]]");
    }
}
