package ru.yandex.clickhouse.integration;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;

/**
 * Here it is assumed the connection to a ClickHouse instance with flights example data it available at localhost:8123
 * For ClickHouse quickstart and example dataset see <a href="https://clickhouse.yandex/tutorial.html">https://clickhouse.yandex/tutorial.html</a>
 */
public class ArrayTest {

    private ClickHouseDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
    }

    @Test(enabled = false)
    public void testStringArray() throws SQLException {
        String[] array = {"a'','sadf',aa", "", ",", "юникод,'юникод'", ",2134,saldfk"};
        String arrayString = array.length == 0 ? "" : "'" + Joiner.on("','").join(Iterables.transform(Arrays.asList(array), new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s.replace("'", "\\'");
            }
        })) + "'";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            assertEquals(rs.getArray(1).getBaseType(), Types.VARCHAR);
                    String[] stringArray = (String[]) rs.getArray(1).getArray();
                    assertEquals(stringArray.length, array.length);
                    for (int i = 0; i < stringArray.length; i++) {
                        assertEquals(stringArray[i], array[i]);
                    }
        }
        statement.close();
    }

    @Test(enabled = false)
    public void testLongArray() throws SQLException {
        Long[] array = {-12345678987654321L, 23325235235L, -12321342L};
        String arrayString = array.length == 0 ? "" : "toInt64(" + Joiner.on("),toInt64(").join(array) + ")";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            assertEquals(rs.getArray(1).getBaseType(), Types.BIGINT);
            long[] longArray = (long[]) rs.getArray(1).getArray();
            assertEquals(longArray.length, array.length);
            for (int i = 0; i < longArray.length; i++) {
                assertEquals(longArray[i], array[i].longValue());
            }
        }
        statement.close();
    }

}
