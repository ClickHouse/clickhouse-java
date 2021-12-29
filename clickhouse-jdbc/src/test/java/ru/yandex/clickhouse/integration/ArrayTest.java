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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.JdbcIntegrationTest;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

/**
 * Here it is assumed the connection to a ClickHouse instance with flights example data it available at localhost:8123
 * For ClickHouse quickstart and example dataset see <a href="https://clickhouse.yandex/tutorial.html">https://clickhouse.yandex/tutorial.html</a>
 */
public class ArrayTest extends JdbcIntegrationTest {
    private Connection connection;

    @BeforeClass(groups = "integration")
    public void setUp() throws Exception {
        connection = newConnection();
    }

    @AfterClass(groups = "integration")
    public void tearDown() throws Exception {
        closeConnection(connection);
    }

    @Test(groups = "integration")
    public void testStringArray() throws SQLException {
        String[] array = {"a'','sadf',aa", "", ",", "юникод,'юникод'", ",2134,saldfk"};

        StringBuilder sb = new StringBuilder();
        for (String s : array) {
            sb.append("','").append(s.replace("'", "\\'"));
        }

        if (sb.length() > 0) {
            sb.deleteCharAt(0).deleteCharAt(0).append('\'');
        }

        String arrayString = sb.toString();

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            Assert.assertEquals(rs.getArray(1).getBaseType(), Types.VARCHAR);
                    String[] stringArray = (String[]) rs.getArray(1).getArray();
                    Assert.assertEquals(stringArray.length, array.length);
                    for (int i = 0; i < stringArray.length; i++) {
                        Assert.assertEquals(stringArray[i], array[i]);
                    }
        }
        statement.close();
    }

    @Test(groups = "integration")
    public void testLongArray() throws SQLException {
        Long[] array = {-12345678987654321L, 23325235235L, -12321342L};
        StringBuilder sb = new StringBuilder();
        for (long l : array) {
            sb.append("),toInt64(").append(l);
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(0).deleteCharAt(0).append(')');
        }
        String arrayString = sb.toString();

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            Assert.assertEquals(rs.getArray(1).getBaseType(), Types.BIGINT);
            long[] longArray = (long[]) rs.getArray(1).getArray();
            Assert.assertEquals(longArray.length, array.length);
            for (int i = 0; i < longArray.length; i++) {
                Assert.assertEquals(longArray[i], array[i].longValue());
            }
        }
        statement.close();
    }

    @Test(groups = "integration")
    public void testDecimalArray() throws SQLException {
        BigDecimal[] array = {BigDecimal.valueOf(-12.345678987654321), BigDecimal.valueOf(23.325235235), BigDecimal.valueOf(-12.321342)};
        StringBuilder sb = new StringBuilder();
        for (BigDecimal d : array) {
            sb.append(", 15),toDecimal64(").append(d);
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(0).delete(0, sb.indexOf(",") + 1).append(", 15)");
        }
        String arrayString = sb.toString();

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            Assert.assertEquals(rs.getArray(1).getBaseType(), Types.DECIMAL);
            BigDecimal[] deciamlArray = (BigDecimal[]) rs.getArray(1).getArray();
            Assert.assertEquals(deciamlArray.length, array.length);
            for (int i = 0; i < deciamlArray.length; i++) {
                Assert.assertEquals(0, deciamlArray[i].compareTo(array[i]));
            }
        }
        statement.close();
    }

    @Test(groups = "integration")
    public void testInsertUIntArray() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS unsigned_array");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS unsigned_array"
                        + " (ua32 Array(UInt32), ua64 Array(UInt64), f64 Array(Float64), a32 Array(Int32)) ENGINE = TinyLog"
        );

        String insertSql = "INSERT INTO unsigned_array (ua32, ua64, f64, a32) VALUES (?, ?, ?, ?)";

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
        ResultSet rs = select.executeQuery("select ua32, ua64, f64, a32 from unsigned_array");
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

    @Test(groups = "integration")
    public void testInsertStringArray() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS string_array");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS string_array (foo Array(String)) ENGINE = TinyLog");

        String insertSQL = "INSERT INTO string_array (foo) VALUES (?)";
        PreparedStatement statement = connection.prepareStatement(insertSQL);
        statement.setArray(1, connection.createArrayOf(
            String.class.getCanonicalName(),
            new String[]{"23", "42"}));
        statement.executeUpdate();

        ResultSet r = connection.createStatement().executeQuery(
            "SELECT foo FROM string_array");
        r.next();
        String[] s = (String[]) r.getArray(1).getArray();
        Assert.assertEquals(s[0], "23");
        Assert.assertEquals(s[1], "42");
    }

    @Test(groups = "integration")
    public void testInsertStringArrayViaUnwrap() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS string_array");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS string_array (foo Array(String)) ENGINE = TinyLog");

        String insertSQL = "INSERT INTO string_array (foo) VALUES (?)";
        ClickHousePreparedStatement statement = connection.prepareStatement(insertSQL)
            .unwrap(ClickHousePreparedStatement.class);
        statement.setArray(1, new String[] {"23", "42"});
        statement.executeUpdate();

        ResultSet r = connection.createStatement().executeQuery(
            "SELECT foo FROM string_array");
        r.next();
        String[] s = (String[]) r.getArray(1).getArray();
        Assert.assertEquals(s[0], "23");
        Assert.assertEquals(s[1], "42");
    }

    // @Test(groups = "integration")
    public void testInsertByteArray() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS int8_array");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS int8_array (foo Array(Int8)) ENGINE = Memory");

        String insertSQL = "INSERT INTO int8_array (foo) VALUES (?)";
        PreparedStatement statement = connection.prepareStatement(insertSQL);
        statement.setArray(1, new ClickHouseArray(ClickHouseDataType.Int8, new byte[]{12,34}));
        statement.executeUpdate();

        ResultSet r = connection.createStatement().executeQuery(
            "SELECT foo FROM int8_array");
        r.next();
        int[] bytes = (int[]) r.getArray(1).getArray();
        Assert.assertEquals(bytes[0], 12);
        Assert.assertEquals(bytes[1], 34);
    }
}
