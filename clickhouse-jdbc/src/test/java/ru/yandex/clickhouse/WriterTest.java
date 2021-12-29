package ru.yandex.clickhouse;

import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.ByteArrayInputStream;
import java.sql.SQLException;

public class WriterTest {

    private ClickHouseStatementImpl statement;

    @BeforeClass(groups = "unit")
    public void setUp() throws SQLException {
        statement = Mockito.mock(ClickHouseStatementImpl.class);
        Mockito.when(statement.write()).thenReturn(new Writer(statement));
    }

    @Test(groups = "unit", expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = ".*No input data.*")
    public void testNonConfigured() throws SQLException {
        statement.write().send();
    }

    @Test(groups = "unit", expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "Format can not be null")
    public void testNullFormatGiven() {
        statement.write().format(null);
    }

    @Test(groups = "unit", expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Wrong binary format.*")
    public void testWrongBinaryFormat() throws SQLException {
        statement.write().send("INSERT", (ClickHouseStreamCallback)null, ClickHouseFormat.CSV);
    }

    @Test(groups = "unit")
    public void testWhitePath() throws SQLException {
        statement
                .write()
                .format(ClickHouseFormat.CSV)
                .table("my_table")
                .data(new ByteArrayInputStream(new byte[1]))
                .send();
    }

    @Test(groups = "unit")
    public void testSendToTable() throws SQLException {
        statement.write().sendToTable("table", new ByteArrayInputStream(new byte[1]), ClickHouseFormat.CSV);
    }
}