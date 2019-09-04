package ru.yandex.clickhouse;

import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.domain.ClickHouseFormat;

import java.io.ByteArrayInputStream;
import java.sql.SQLException;

public class WriterTest {

    private ClickHouseStatementImpl statement;

    @BeforeTest
    public void setUp() throws SQLException {
        statement = Mockito.mock(ClickHouseStatementImpl.class);
        Mockito.when(statement.write()).thenReturn(new Writer(statement));
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = ".*Neither table nor SQL.*")
    public void testNonConfigured() throws SQLException {
        statement.write().send();
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "Format can not be null")
    public void testNullFormatGiven() {
        statement.write().format(null);
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