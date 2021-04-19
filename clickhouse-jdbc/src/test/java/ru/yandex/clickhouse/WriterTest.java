package ru.yandex.clickhouse;

import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.ByteArrayInputStream;
import java.sql.SQLException;

public class WriterTest {

    private ClickHouseStatementImpl statement;

    @BeforeTest
    public void setUp() throws SQLException {
        statement = Mockito.mock(ClickHouseStatementImpl.class);
        Mockito.when(statement.write()).thenReturn(new Writer(statement));
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = ".*No input data.*")
    public void testNonConfigured() throws SQLException {
        statement.write().send();
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "Format can not be null")
    public void testNullFormatGiven() {
        statement.write().format(null);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Wrong binary format.*")
    public void testWrongBinaryFormat() throws SQLException {
        statement.write().send("INSERT", (ClickHouseStreamCallback)null, ClickHouseFormat.CSV);
    }

    @Test
    public void testWhitePath() throws SQLException {
        statement
                .write()
                .format(ClickHouseFormat.CSV)
                .table("my_table")
                .data(new ByteArrayInputStream(new byte[1]))
                .send();
    }

    @Test
    public void testSendToTable() throws SQLException {
        statement.write().sendToTable("table", new ByteArrayInputStream(new byte[1]), ClickHouseFormat.CSV);
    }
}