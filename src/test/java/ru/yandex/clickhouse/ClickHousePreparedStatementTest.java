package ru.yandex.clickhouse;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.TimeZone;

import org.apache.http.impl.client.CloseableHttpClient;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.settings.ClickHouseProperties;

import static org.testng.Assert.assertEquals;

public class ClickHousePreparedStatementTest {

    private static final String SQL_STATEMENT= "INSERT INTO foo (bar) VALUES (";

    @Test
    public void testSetBytesNull() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setBytes(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetBytesNormal() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setBytes(1, "foo".getBytes("UTF-8"));
        assertParamMatches(s, "'\\x66\\x6F\\x6F'");
    }

    @Test
    public void testSetBytesEmpty() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setBytes(1, "".getBytes("UTF-8"));
        assertParamMatches(s, "''");
    }

    @Test
    public void testSetNull() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setNull(1, Types.ARRAY);
        assertParamMatches(s, "null");
        s.setNull(1, Types.CHAR);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetBooleanTrue() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setBoolean(1, true);
        assertParamMatches(s, "1");
    }

    @Test
    public void testSetBooleanFalse() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setBoolean(1, false);
        assertParamMatches(s, "0");
    }

    @Test
    public void testSetByte() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setByte(1, (byte) -127);
        assertParamMatches(s, "-127");
    }

    @Test
    public void testSetShort() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setShort(1, (short) 42);
        assertParamMatches(s, "42");
    }

    @Test
    public void testSetInt() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setInt(1, 0);
        assertParamMatches(s, "0");
    }

    @Test
    public void testSetLong() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setLong(1, 1337L);
        assertParamMatches(s, "1337");
    }

    @Test
    public void testSetFloat() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setFloat(1, -23.42f);
        assertParamMatches(s, "-23.42");
    }

    @Test
    public void testSetDouble() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setDouble(1, Double.MIN_VALUE);
        assertParamMatches(s, "4.9E-324"); // will result in 0 in Float64
                                           // but parsing is OK
    }

    @Test
    public void testSetBigDecimalNull() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setBigDecimal(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetBigDecimalNormal() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setBigDecimal(1, BigDecimal.valueOf(-0.2342));
        assertParamMatches(s, "-0.2342");
    }

    @Test
    public void testSetStringNull() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setString(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetStringSimple() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setString(1, "foo");
        assertParamMatches(s, "'foo'");
    }

    @Test
    public void testSetStringEvil() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setString(1, "\"'\\x32");
        assertParamMatches(s, "'\"\\'\\\\x32'");
    }

    @Test
    public void testSetDateNull() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setDate(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetDateNormal() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setDate(1, new Date(1557168043000L));
        assertParamMatches(s, "'2019-05-06'");
    }

    @Test
    public void testSetDateOtherTimeZone() throws Exception {
        ClickHousePreparedStatement s = createStatement(
            TimeZone.getTimeZone("Asia/Tokyo"),
            new ClickHouseProperties());
        s.setDate(1, new Date(1557168043000L));
        assertParamMatches(s, "'2019-05-06'");
    }

    @Test
    public void testSetDateOtherTimeZoneServerTime() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUseServerTimeZoneForDates(true);
        ClickHousePreparedStatement s = createStatement(
            TimeZone.getTimeZone("Asia/Tokyo"),
            props);
        s.setDate(1, new Date(1557168043000L));
        assertParamMatches(s, "'2019-05-07'");
    }

    @Test
    public void testSetTimeNull() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setTime(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetTimeNormal() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setTime(1, new Time(1557168043000L));
        assertParamMatches(s, "'2019-05-06 21:40:43'");
    }

    @Test
    public void testSetTimeNormalOtherTimeZone() throws Exception {
        ClickHousePreparedStatement s = createStatement(
            TimeZone.getTimeZone("America/Los_Angeles"),
            new ClickHouseProperties());
        s.setTime(1, new Time(1557168043000L));
        assertParamMatches(s, "'2019-05-06 11:40:43'");
    }

    @Test
    public void testSetTimestampNull() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setTimestamp(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetTimestampNormal() throws Exception {
        ClickHousePreparedStatement s = createStatement();
        s.setTimestamp(1, new Timestamp(1557168043000L));
        assertParamMatches(s, "'2019-05-06 21:40:43'");
    }

    @Test
    public void testSetTimestampNormalOtherTimeZone() throws Exception {
        ClickHousePreparedStatement s = createStatement(
            TimeZone.getTimeZone("America/Los_Angeles"),
            new ClickHouseProperties());
        s.setTimestamp(1, new Timestamp(1557168043000L));
        assertParamMatches(s, "'2019-05-06 11:40:43'");
    }

    private static void assertParamMatches(ClickHousePreparedStatement stmt, String expected) {
        assertEquals(stmt.asSql(), SQL_STATEMENT + expected + ")");
    }

    private static ClickHousePreparedStatement createStatement() throws Exception {
        return createStatement(
            TimeZone.getTimeZone("Europe/Moscow"),
            new ClickHouseProperties());
    }

    private static ClickHousePreparedStatement createStatement(TimeZone timezone,
        ClickHouseProperties props) throws Exception
    {
        return new ClickHousePreparedStatementImpl(
            Mockito.mock(CloseableHttpClient.class),
            Mockito.mock(ClickHouseConnection.class),
            props,
            "INSERT INTO foo (bar) VALUES (?)",
            timezone,
            ResultSet.TYPE_FORWARD_ONLY);
    }

}
