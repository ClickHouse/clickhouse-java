package com.clickhouse.client.api.internal;

import com.clickhouse.data.ClickHouseFormat;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;

public class ClientUtilsTest {

    @Test(groups = {"unit"})
    public void testQuietCloseSwallowsExceptionAndLogs() throws IOException {
        Logger log = Mockito.mock(Logger.class);
        IOException failure = new IOException("close failed");
        Closeable closeable = Mockito.mock(Closeable.class);
        Mockito.doThrow(failure).when(closeable).close();

        // Should not propagate the exception thrown by close()
        ClientUtils.quietClose(closeable, log);

        Mockito.verify(closeable).close();
        Mockito.verify(log).warn(Mockito.contains("Failed to close object"), Mockito.eq(failure));
    }

    @Test(groups = {"unit"})
    public void testQuietCloseClosesSuccessfully() throws IOException {
        Logger log = Mockito.mock(Logger.class);
        Closeable closeable = Mockito.mock(Closeable.class);

        ClientUtils.quietClose(closeable, log);

        Mockito.verify(closeable).close();
        Mockito.verifyNoInteractions(log);
    }

    @Test(groups = {"unit"})
    public void testQuietCloseWithNull() {
        Logger log = Mockito.mock(Logger.class);

        // Should be a no-op and not throw on a null closeable
        ClientUtils.quietClose(null, log);

        Mockito.verifyNoInteractions(log);
        Assert.assertTrue(true);
    }

    @Test(groups = {"unit"}, dataProvider = "trailingFormatData")
    public void testExtractTrailingFormat(String sqlQuery, ClickHouseFormat expectedFormat) {
        Assert.assertEquals(ClientUtils.extractTrailingFormat(sqlQuery), expectedFormat);
    }

    @DataProvider(name = "trailingFormatData")
    public static Object[][] trailingFormatData() {
        return new Object[][]{
                {"SELECT 1 FORMAT JSONEachRow", ClickHouseFormat.JSONEachRow},
                {"SELECT 1 format TabSeparated", ClickHouseFormat.TabSeparated},
                {"SELECT 1\n  FORMAT\tCSV \n ", ClickHouseFormat.CSV},
                {"SELECT 1 FORMAT CSV;", ClickHouseFormat.CSV},
                {"SELECT 1 SETTINGS max_block_size = 10 FORMAT Pretty", ClickHouseFormat.Pretty},
                {"SELECT 1 FORMAT CSV SETTINGS format_csv_delimiter = '|'", ClickHouseFormat.CSV},
                {"SELECT 1 FORMAT CSV\r\n", ClickHouseFormat.CSV},
                {"SELECT 1 FORMAT CSV # as csv", ClickHouseFormat.CSV},
                {"SELECT 1 FORMAT CSV -- as csv", ClickHouseFormat.CSV},
                {"SELECT 1 FORMAT CSV /* as csv */", ClickHouseFormat.CSV},
                {"SELECT /* FORMAT TSKV */ 1 FORMAT CSV", ClickHouseFormat.CSV},
                // the name of the format is read without regard to case, as the server reads it
                {"SELECT 1 FORMAT jsoneachrow", ClickHouseFormat.JSONEachRow},
                {"SELECT 1 FORMAT JsOnEaChRoW", ClickHouseFormat.JSONEachRow},
                {"SELECT 1 FORMAT TABSEPARATEDWITHNAMES", ClickHouseFormat.TabSeparatedWithNames},
                // statements that name no format
                {"SELECT 1", null},
                {"SELECT 1 FORMAT", null},
                {"", null},
                {null, null},
                // a format name the client does not know is left to the server
                {"SELECT 1 FORMAT NoSuchFormat", null},
                {"SELECT 1 FORMAT nosuchformat", null},
                // FORMAT that is not a clause of the statement
                {"SELECT 'x FORMAT CSV'", null},
                {"SELECT 'it''s x FORMAT CSV'", null},
                {"SELECT 'it\\'s x FORMAT CSV'", null},
                {"SELECT 1 AS \"x FORMAT CSV\"", null},
                {"SELECT 1 AS `x FORMAT CSV`", null},
                {"SELECT 1 -- FORMAT CSV", null},
                {"SELECT 1 /* FORMAT CSV */", null},
                {"INSERT INTO t FORMAT CSV\n1,2\n", null},
                {"INSERT INTO t FORMAT CSV\n'a',2\n", null},
                {"SELECT formatDateTime(d, '%F') FROM t", null},
                {"SELECT 1 AS format FROM t LIMIT 1", null},
        };
    }
}
