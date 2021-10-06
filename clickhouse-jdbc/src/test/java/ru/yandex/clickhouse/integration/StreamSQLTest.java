package ru.yandex.clickhouse.integration;


import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.zip.GZIPOutputStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.JdbcIntegrationTest;
import ru.yandex.clickhouse.domain.ClickHouseCompression;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.util.ClickHouseVersionNumberUtil;

public class StreamSQLTest extends JdbcIntegrationTest {
    private static final DateTimeFormatter DATE_TIME_FORMATTER_TZ = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd['T'][ ]HH:mm:ss[.SSS][XXX]");

    private ClickHouseConnection connection;

    private Timestamp utcToServerTimezone(String datetime) {
        return Timestamp.from(LocalDateTime.parse(datetime, DATE_TIME_FORMATTER_TZ)
            .atZone(ZoneId.of("UTC")).withZoneSameInstant(connection.getTimeZone().toZoneId()).toInstant());
    }

    @BeforeClass(groups = "integration")
    public void setUp() throws Exception {
        connection = newConnection();
    }

    @AfterClass(groups = "integration")
    public void tearDown() throws Exception {
        closeConnection(connection);
    }

    @Test(groups = "integration")
    public void simpleCSVInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS csv_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE csv_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));

        connection.createStatement()
                .write()
                .sql("insert into csv_stream_sql format CSV")
                .data(inputStream)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM csv_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    private InputStream getTSVStream(final int rowsCount) {
        return new InputStream() {
            private int si = 0;
            private String s = "";
            private int i = 0;

            private boolean genNextString() {
                if (i >= rowsCount) return false;
                si = 0;
                s = String.format("%d\txxxx%d\n", 1, i);
                i++;
                return true;
            }

            public int read() {
                if (si >= s.length()) {
                    if ( ! genNextString() ) {
                        return -1;
                    }
                }
                return s.charAt( si++ );
            }
        };
    }

    @Test(groups = "integration")
    public void multiRowTSVInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS tsv_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE tsv_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        final int rowsCount = 100000;

        connection.createStatement().
                write()
                .sql("insert into tsv_stream_sql format TSV")
                .data(getTSVStream(rowsCount), ClickHouseFormat.TSV)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM tsv_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), rowsCount);
        Assert.assertEquals(rs.getInt("sum"), rowsCount);
        Assert.assertEquals(rs.getInt("uniq"), rowsCount);
    }

    private InputStream gzStream( InputStream is ) throws IOException
    {
        final int bufferSize = 16384;
        byte[] data = new byte[bufferSize];
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(os);
        BufferedInputStream es = new BufferedInputStream(is, bufferSize);
        int count;
        while ( ( count = es.read( data, 0, bufferSize) ) != -1 )
            gzipOutputStream.write( data, 0, count );
        es.close();
        gzipOutputStream.close();

        return new ByteArrayInputStream( os.toByteArray() );
    }

    @Test(groups = "integration")
    public void multiRowTSVInsertCompressed() throws SQLException, IOException {
        connection.createStatement().execute("DROP TABLE IF EXISTS tsv_compressed_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE tsv_compressed_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        final int rowsCount = 100000;

        InputStream gz = gzStream(getTSVStream(rowsCount));
        connection.createStatement()
                .write()
                .sql("insert into tsv_compressed_stream_sql format TSV")
                .data(gz, ClickHouseFormat.TSV, ClickHouseCompression.gzip)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM tsv_compressed_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), rowsCount);
        Assert.assertEquals(rs.getInt("sum"), rowsCount);
        Assert.assertEquals(rs.getInt("uniq"), rowsCount);
    }

    @Test(groups = "integration")
    public void multiRowTSVInsertNotCompressed() throws SQLException, IOException {
        connection.createStatement().execute("DROP TABLE IF EXISTS tsv_not_compressed_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE tsv_not_compressed_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        final int rowsCount = 100000;

        InputStream in = getTSVStream(rowsCount);
        connection.createStatement()
                .write()
                .sql("insert into tsv_not_compressed_stream_sql format TSV")
                .data(in, ClickHouseFormat.TSV, ClickHouseCompression.none)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM tsv_not_compressed_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), rowsCount);
        Assert.assertEquals(rs.getInt("sum"), rowsCount);
        Assert.assertEquals(rs.getInt("uniq"), rowsCount);
    }


    @Test(groups = "integration")
    public void JSONEachRowInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS json_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE json_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "{\"value\":5,\"string_value\":\"6\"}\n{\"value\":1,\"string_value\":\"6\"}";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));

        connection.createStatement().
                write()
                .sql("insert into json_stream_sql")
                .data(inputStream, ClickHouseFormat.JSONEachRow)
                .data(inputStream)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM json_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test(groups = "integration")
    public void JSONEachRowCompressedInsert() throws SQLException, IOException {
        connection.createStatement().execute("DROP TABLE IF EXISTS json_comressed_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE json_comressed_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "{\"value\":5,\"string_value\":\"6\"}\n{\"value\":1,\"string_value\":\"6\"}";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));

        connection.createStatement().
                write()
                .sql("insert into json_comressed_stream_sql")
                .data(inputStream, ClickHouseFormat.JSONEachRow)
                .data(gzStream(inputStream))
                .dataCompression(ClickHouseCompression.gzip)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM json_comressed_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test(groups = "integration")
    public void CSVInsertCompressedIntoTable() throws SQLException, IOException {
        connection.createStatement().execute("DROP TABLE IF EXISTS csv_stream_compressed");
        connection.createStatement().execute(
                "CREATE TABLE csv_stream_compressed (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));

        connection.createStatement()
                .write()
                .table(dbName + ".csv_stream_compressed")
                .format(ClickHouseFormat.CSV)
                .dataCompression(ClickHouseCompression.gzip)
                .data(gzStream(inputStream))
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM csv_stream_compressed");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test(groups = "integration")
    public void ORCInsertCompressedIntoTable() throws SQLException {
        // clickhouse-client -q "select number int, toString(number) str, 1/number flt, toDecimal64( 1/(number+1) , 9) dcml,
        // toDateTime('2020-01-01 00:00:00') + number time from numbers(100) format ORC"|gzip > test_sample.orc.gz

        String version = connection.getServerVersion();
        if (ClickHouseVersionNumberUtil.compare(version, "20.8") < 0) {
            return;
        }

        connection.createStatement().execute("DROP TABLE IF EXISTS orc_stream_compressed");
        connection.createStatement().execute(
                "CREATE TABLE orc_stream_compressed (int Int64, str String, flt Float64, " +
                     "dcml Decimal64(9), time DateTime) ENGINE = Log();"
        );

        InputStream inputStream = StreamSQLTest.class.getResourceAsStream("/data_samples/test_sample.orc.gz");

        connection.createStatement().
                write()
                .table(dbName + ".orc_stream_compressed")
                .format(ClickHouseFormat.ORC)
                .dataCompression(ClickHouseCompression.gzip)
                .data(inputStream)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, " +
                        "sum(int) sum_int, " +
                        "round(sum(flt),2) AS sum_flt, " +
                        "uniqExact(str) uniq_str, " +
                        "max(dcml) max_dcml, " +
                        "min(time) min_time, " +
                        "max(time) max_time " +
                        "FROM orc_stream_compressed");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 100);
        Assert.assertEquals(rs.getLong("sum_int"), 4950);
        Assert.assertEquals(rs.getFloat("sum_flt"), Float.POSITIVE_INFINITY);
        Assert.assertEquals(rs.getLong("uniq_str"), 100);
        Assert.assertEquals(rs.getBigDecimal("max_dcml").setScale(9), new BigDecimal("1.000000000"));
        Assert.assertEquals(rs.getTimestamp("min_time"), utcToServerTimezone("2020-01-01 00:00:00"));
        Assert.assertEquals(rs.getTimestamp("max_time"), utcToServerTimezone("2020-01-01 00:01:39"));
    }

    @Test(groups = "integration")
    public void ORCInsertCompressedIntoTable1() throws SQLException {
        // clickhouse-client -q "select number int, toString(number) str, 1/number flt, toDecimal64( 1/(number+1) , 9) dcml,
        // toDateTime('2020-01-01 00:00:00') + number time from numbers(100) format ORC"|gzip > test_sample.orc.gz

        String version = connection.getServerVersion();
        if (version.compareTo("20.8") < 0) {
            return;
        }

        connection.createStatement().execute("DROP TABLE IF EXISTS orc1_stream_compressed");
        connection.createStatement().execute(
                "CREATE TABLE orc1_stream_compressed (int Int64, str String, flt Float64, " +
                        "dcml Decimal64(9), time DateTime) ENGINE = Log();"
        );

        InputStream inputStream = StreamSQLTest.class.getResourceAsStream("/data_samples/test_sample.orc.gz");

        connection.createStatement().
                write()
                .sql("insert into orc1_stream_compressed format ORC")
                .dataCompression(ClickHouseCompression.gzip)
                .data(inputStream)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "select * from orc1_stream_compressed where int=42");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("int"), 42);
        Assert.assertEquals(rs.getString("str"), "42");
        Assert.assertTrue( Math.abs(rs.getFloat("flt") - 0.023809524) < 0.0001);
        Assert.assertTrue( Math.abs(rs.getFloat("dcml") - 0.023255813) < 0.0001);
        Assert.assertEquals(rs.getTimestamp("time"), utcToServerTimezone("2020-01-01 00:00:42"));
    }

    @Test(groups = "integration")
    public void ParquetInsertCompressedIntoTable() throws SQLException {
        // clickhouse-client -q "select number int, toString(number) str, 1/number flt, toDecimal64( 1/(number+1) , 9) dcml,
        // toDateTime('2020-01-01 00:00:00') + number time from numbers(100) format Parquet"|gzip > test_sample.parquet.gz

        String version = connection.getServerVersion();
        if (version.compareTo("20.8") < 0) {
            return;
        }

        connection.createStatement().execute("DROP TABLE IF EXISTS parquet_stream_compressed");
        connection.createStatement().execute(
                "CREATE TABLE parquet_stream_compressed (int Int64, str String, flt Float64, " +
                        "dcml Decimal64(9), time DateTime) ENGINE = Log();"
        );

        InputStream inputStream = StreamSQLTest.class.getResourceAsStream("/data_samples/test_sample.parquet.gz");

        connection.createStatement().
                write()
                .table(dbName + ".parquet_stream_compressed")
                .format(ClickHouseFormat.Parquet)
                .dataCompression(ClickHouseCompression.gzip)
                .data(inputStream)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, " +
                        "sum(int) sum_int, " +
                        "round(sum(flt),2) AS sum_flt, " +
                        "uniqExact(str) uniq_str, " +
                        "max(dcml) max_dcml, " +
                        "min(time) min_time, " +
                        "max(time) max_time " +
                        "FROM parquet_stream_compressed");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 100);
        Assert.assertEquals(rs.getLong("sum_int"), 4950);
        Assert.assertEquals(rs.getFloat("sum_flt"), Float.POSITIVE_INFINITY);
        Assert.assertEquals(rs.getLong("uniq_str"), 100);
        Assert.assertEquals(rs.getBigDecimal("max_dcml").setScale(9), new BigDecimal("1.000000000"));
        Assert.assertEquals(rs.getTimestamp("min_time"), utcToServerTimezone("2020-01-01 00:00:00"));
        Assert.assertEquals(rs.getTimestamp("max_time"), utcToServerTimezone("2020-01-01 00:01:39"));
    }

    @Test(groups = "integration")
    public void ParquetInsertCompressedIntoTable1() throws SQLException {
        // clickhouse-client -q "select number int, toString(number) str, 1/number flt, toDecimal64( 1/(number+1) , 9) dcml,
        // toDateTime('2020-01-01 00:00:00') + number time from numbers(100) format Parquet"|gzip > test_sample.parquet.gz

        String version = connection.getServerVersion();
        if (version.compareTo("20.8") < 0) {
            return;
        }

        connection.createStatement().execute("DROP TABLE IF EXISTS parquet1_stream_compressed");
        connection.createStatement().execute(
                "CREATE TABLE parquet1_stream_compressed (int Int64, str String, flt Float64, " +
                        "dcml Decimal64(9), time DateTime) ENGINE = Log();"
        );

        InputStream inputStream = StreamSQLTest.class.getResourceAsStream("/data_samples/test_sample.parquet.gz");

        connection.createStatement().
                write()
                .sql("insert into parquet1_stream_compressed format Parquet")
                .dataCompression(ClickHouseCompression.gzip)
                .data(inputStream)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "select * from parquet1_stream_compressed where int=42");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("int"), 42);
        Assert.assertEquals(rs.getString("str"), "42");
        Assert.assertTrue( Math.abs(rs.getFloat("flt") - 0.023809524) < 0.0001);
        Assert.assertTrue( Math.abs(rs.getFloat("dcml") - 0.023255813) < 0.0001);
        Assert.assertEquals(rs.getTimestamp("time"), utcToServerTimezone("2020-01-01 00:00:42"));
    }

}
