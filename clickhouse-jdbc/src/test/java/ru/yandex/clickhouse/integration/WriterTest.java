package ru.yandex.clickhouse.integration;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.internal.Utils;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class WriterTest {

    private ClickHouseConnection connection;
    private ClickHouseStatement statement;

    @BeforeMethod
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setDecompress(true);
        properties.setCompress(true);
        connection = ClickHouseContainerForTest.newDataSource(properties).getConnection();
        statement = connection.createStatement();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
        connection.createStatement().execute("DROP TABLE IF EXISTS test.writer");
        connection.createStatement().execute("CREATE TABLE test.writer (id Int32, name String) ENGINE = Log");
        connection.createStatement().execute("TRUNCATE TABLE test.writer");
    }

    @Test
    public void testCSV() throws Exception {
        String data = "10;Фёдор\n20;Слава";

        statement.write()
                .table("test.writer")
                .format(ClickHouseFormat.CSV)
                .option("format_csv_delimiter", ";")
                .data(new ByteArrayInputStream(data.getBytes("UTF-8")))
                .send();
        assertTableRowCount(2);
    }

    @Test
    public void testTSV() throws Exception {
        File tempFile = Utils.createTempFile("");
        FileOutputStream fos = new FileOutputStream(tempFile);
        for (int i = 0; i < 1000; i++) {
            fos.write((i + "\tИмя " + i + "\n").getBytes("UTF-8"));
        }
        fos.close();

        statement
                .write()
                .table("test.writer")
                .format(ClickHouseFormat.TabSeparated)
                .data(tempFile)
                .send();

        assertTableRowCount(1000);

        ResultSet rs = statement.executeQuery("SELECT count() FROM test.writer WHERE name = concat('Имя ', toString(id))");
        rs.next();
        assertEquals(rs.getInt(1), 1000);
    }

    @Test
    public void testRowBinary() throws Exception {
        statement.write().send("INSERT INTO test.writer", new ClickHouseStreamCallback() {
            @Override
            public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                for (int i = 0; i < 10; i++) {
                    stream.writeInt32(i);
                    stream.writeString("Имя " + i);
                }
            }
        }, ClickHouseFormat.RowBinary);

        assertTableRowCount(10);
        ResultSet rs = statement.executeQuery("SELECT count() FROM test.writer WHERE name = concat('Имя ', toString(id))");
        rs.next();
        assertEquals(rs.getInt(1), 10);
    }

    @Test
    public void testNative() throws Exception {
        statement.write().send("INSERT INTO test.writer", new ClickHouseStreamCallback() {
            @Override
            public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {

                int numberOfRows = 1000;
                stream.writeUnsignedLeb128(2); // 2 columns
                stream.writeUnsignedLeb128(numberOfRows);

                stream.writeString("id");
                stream.writeString("Int32");

                for (int i = 0; i < numberOfRows; i++) {
                    stream.writeInt32(i);
                }

                stream.writeString("name");
                stream.writeString("String");

                for (int i = 0; i < numberOfRows; i++) {
                    stream.writeString("Имя " + i);
                }
            }
        }, ClickHouseFormat.Native);

        assertTableRowCount(1000);
        ResultSet rs = statement.executeQuery("SELECT count() FROM test.writer WHERE name = concat('Имя ', toString(id))");
        rs.next();
        assertEquals(rs.getInt(1), 1000);
    }

    private void assertTableRowCount(int expected) throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT count() from test.writer");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), expected);
    }
}
