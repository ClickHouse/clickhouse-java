package com.clickhouse.jdbc;

import org.testng.annotations.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ArrayTupleIntegrationTest extends JdbcIntegrationTest {

    @Test(groups = {"integration"})
    public void testArrayOfNamedAndUnnamedTuplesToString() throws Exception {
        String uuid = "550e8400-e29b-41d4-a716-446655440000";
        String expected = "[[" + uuid + "]]";
        String query = "SELECT [('" + uuid + "')::Tuple(id UUID)] AS named, "
                + "[('" + uuid + "')::Tuple(UUID)] AS unnamed";

        try (Connection connection = getJdbcConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            assertTrue(resultSet.next());

            assertArrayValue(resultSet.getObject("named"), expected);
            assertArrayValue(resultSet.getObject("unnamed"), expected);

            assertFalse(resultSet.next());
        }
    }

    private static void assertArrayValue(Object value, String expected) throws SQLException {
        assertTrue(value instanceof Array);
        Array array = (Array) value;
        assertEquals(value.toString(), expected);
        assertEquals(Arrays.deepToString((Object[]) array.getArray()), expected);
    }
}
