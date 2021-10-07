package ru.yandex.clickhouse.integration;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.JdbcIntegrationTest;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class WriterTest extends JdbcIntegrationTest {
    private ClickHouseConnection connection;

    @BeforeClass(groups = "integration")
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setDecompress(true);
        properties.setCompress(true);
        connection = newConnection(properties);
    }

    @AfterClass(groups = "integration")
    public void tearDown() throws Exception {
        closeConnection(connection);
    }

    @BeforeMethod(groups = "integration")
    public void createTable() throws Exception {
        try (ClickHouseStatement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS writer");
            statement.execute("CREATE TABLE writer (id Int32, name String) ENGINE = Log");
            statement.execute("TRUNCATE TABLE writer");
        }
    }

    @Test(groups = "integration")
    public void testCSV() throws Exception {
        String data = "10;Фёдор\n20;Слава";

        try (ClickHouseStatement statement = connection.createStatement()) {
            statement.write().table(dbName + ".writer").format(ClickHouseFormat.CSV).option("format_csv_delimiter", ";")
                    .data(new ByteArrayInputStream(data.getBytes("UTF-8"))).send();
            assertTableRowCount(2);
        }
    }

    @Test(groups = "integration")
    public void testTSV() throws Exception {
        File tempFile = File.createTempFile("tmp-", ".tsv");
        tempFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(tempFile);
        for (int i = 0; i < 1000; i++) {
            fos.write((i + "\tИмя " + i + "\n").getBytes("UTF-8"));
        }
        fos.close();

        try (ClickHouseStatement statement = connection.createStatement()) {
            statement.write().table(dbName + ".writer").format(ClickHouseFormat.TabSeparated).data(tempFile).send();

            assertTableRowCount(1000);

            ResultSet rs = statement
                    .executeQuery("SELECT count() FROM writer WHERE name = concat('Имя ', toString(id))");
            rs.next();
            assertEquals(rs.getInt(1), 1000);
        }
    }

    @Test(groups = "integration")
    public void testRowBinary() throws Exception {
        try (ClickHouseStatement statement = connection.createStatement()) {
            statement.write().send("INSERT INTO writer", new ClickHouseStreamCallback() {
                @Override
                public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                    for (int i = 0; i < 10; i++) {
                        stream.writeInt32(i);
                        stream.writeString("Имя " + i);
                    }
                }
            }, ClickHouseFormat.RowBinary);

            assertTableRowCount(10);
            ResultSet rs = statement
                    .executeQuery("SELECT count() FROM writer WHERE name = concat('Имя ', toString(id))");
            rs.next();
            assertEquals(rs.getInt(1), 10);
        }
    }

    @Test(groups = "integration")
    public void testNative() throws Exception {
        try (ClickHouseStatement statement = connection.createStatement()) {
            statement.write().send("INSERT INTO writer", new ClickHouseStreamCallback() {
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
            ResultSet rs = statement
                    .executeQuery("SELECT count() FROM writer WHERE name = concat('Имя ', toString(id))");
            rs.next();
            assertEquals(rs.getInt(1), 1000);
        }
    }

    private void assertTableRowCount(int expected) throws SQLException {
        try (ClickHouseStatement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT count() from writer");) {
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), expected);
        }
    }
}
