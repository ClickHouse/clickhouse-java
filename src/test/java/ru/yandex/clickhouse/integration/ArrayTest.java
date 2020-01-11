package ru.yandex.clickhouse.integration;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

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
        dataSource = ClickHouseContainerForTest.newDataSource();
        connection = dataSource.getConnection();
    }

    @Test
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

    @Test
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

    @Test
    public void testDecimalArray() throws SQLException {
        BigDecimal[] array = {BigDecimal.valueOf(-12.345678987654321), BigDecimal.valueOf(23.325235235), BigDecimal.valueOf(-12.321342)};
        String arrayString = array.length == 0 ? "" : "toDecimal64(" + Joiner.on(", 15),toDecimal64(").join(array) + ", 15)";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            assertEquals(rs.getArray(1).getBaseType(), Types.DECIMAL);
            BigDecimal[] deciamlArray = (BigDecimal[]) rs.getArray(1).getArray();
            assertEquals(deciamlArray.length, array.length);
            for (int i = 0; i < deciamlArray.length; i++) {
                assertEquals(0, deciamlArray[i].compareTo(array[i]));
            }
        }
        statement.close();
    }

    @Test
    public void testInsertUIntArray() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.unsigned_array");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.unsigned_array"
                        + " (ua32 Array(UInt32), ua64 Array(UInt64), f64 Array(Float64), a32 Array(Int32)) ENGINE = TinyLog"
        );

        String insertSql = "INSERT INTO test.unsigned_array (ua32, ua64, f64, a32) VALUES (?, ?, ?, ?)";

        PreparedStatement statement = connection.prepareStatement(insertSql);

        statement.setArray(1, new ClickHouseArray(ClickHouseDataType.UInt64, new long[]{4294967286L, 4294967287L}));
        statement.setArray(2, new ClickHouseArray(ClickHouseDataType.UInt64, new BigInteger[]{new BigInteger("18446744073709551606"), new BigInteger("18446744073709551607")}));
        statement.setArray(3, new ClickHouseArray(ClickHouseDataType.Float64, new double[]{1.23, 4.56}));
        statement.setArray(4, new ClickHouseArray(ClickHouseDataType.Int32, new int[]{-2147483648, 2147483647}));
        statement.execute();

        statement = connection.prepareStatement(insertSql);

        statement.setObject(1, new ArrayList<Object>(Arrays.asList(4294967286L, 4294967287L)));
        statement.setObject(2, new ArrayList<Object>(Arrays.asList(
                new BigInteger("18446744073709551606"),
                new BigInteger("18446744073709551607"))));
        statement.setObject(3, new ArrayList<Object>(Arrays.asList(1.23, 4.56)));
        statement.setObject(4, Arrays.asList(-2147483648, 2147483647));
        statement.execute();

        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select ua32, ua64, f64, a32 from test.unsigned_array");
        for (int i = 0; i < 2; ++i) {
            rs.next();
            Array bigUInt32 = rs.getArray(1);
            Assert.assertEquals(bigUInt32.getBaseType(), Types.BIGINT); //
            Assert.assertEquals(bigUInt32.getArray().getClass(), long[].class);
            Assert.assertEquals(((long[]) bigUInt32.getArray())[0], 4294967286L);
            Assert.assertEquals(((long[]) bigUInt32.getArray())[1], 4294967287L);
            Array bigUInt64 = rs.getArray(2);
            Assert.assertEquals(bigUInt64.getBaseType(), Types.BIGINT);
            Assert.assertEquals(bigUInt64.getArray().getClass(), BigInteger[].class);
            Assert.assertEquals(((BigInteger[]) bigUInt64.getArray())[0], new BigInteger("18446744073709551606"));
            Assert.assertEquals(((BigInteger[]) bigUInt64.getArray())[1], new BigInteger("18446744073709551607"));
            Array float64 = rs.getArray(3);
            Assert.assertEquals(float64.getBaseType(), Types.DOUBLE);
            Assert.assertEquals(float64.getArray().getClass(), double[].class);
            Assert.assertEquals(((double[]) float64.getArray())[0], 1.23, 0.0000001);
            Assert.assertEquals(((double[]) float64.getArray())[1], 4.56, 0.0000001);
            Array int32 = rs.getArray(4);
            Assert.assertEquals(int32.getBaseType(), Types.INTEGER); //
            Assert.assertEquals(int32.getArray().getClass(), int[].class);
            Assert.assertEquals(((int[]) int32.getArray())[0], -2147483648);
            Assert.assertEquals(((int[]) int32.getArray())[1], 2147483647);
        }
    }

    @Test
    public void testInsertStringArray() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.string_array");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.string_array (foo Array(String)) ENGINE = TinyLog");

        String insertSQL = "INSERT INTO test.string_array (foo) VALUES (?)";
        PreparedStatement statement = connection.prepareStatement(insertSQL);
        statement.setArray(1, connection.createArrayOf(
            String.class.getCanonicalName(),
            new String[]{"23", "42"}));
        statement.executeUpdate();

        ResultSet r = connection.createStatement().executeQuery(
            "SELECT foo FROM test.string_array");
        r.next();
        String[] s = (String[]) r.getArray(1).getArray();
        Assert.assertEquals(s[0], "23");
        Assert.assertEquals(s[1], "42");
    }

    @Test
    public void testInsertStringArrayViaUnwrap() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.string_array");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.string_array (foo Array(String)) ENGINE = TinyLog");

        String insertSQL = "INSERT INTO test.string_array (foo) VALUES (?)";
        ClickHousePreparedStatement statement = connection.prepareStatement(insertSQL)
            .unwrap(ClickHousePreparedStatement.class);
        statement.setArray(1, new String[] {"23", "42"});
        statement.executeUpdate();

        ResultSet r = connection.createStatement().executeQuery(
            "SELECT foo FROM test.string_array");
        r.next();
        String[] s = (String[]) r.getArray(1).getArray();
        Assert.assertEquals(s[0], "23");
        Assert.assertEquals(s[1], "42");
    }
}
