package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.ServerException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class ExceptionUtilsTest {
    @DataProvider
    public Object[][] serverExceptions() {
        return new Object[][] {
            {ServerException.TABLE_NOT_FOUND, "42S02"},
            {ServerException.ErrorCodes.UNKNOWN_SETTING.getCode(), ExceptionUtils.SQL_STATE_DATA_EXCEPTION}
        };
    }

    @Test(dataProvider = "serverExceptions")
    public void shouldMapServerExceptionToSqlState(int errorCode, String expectedSqlState) {
        ServerException cause = new ServerException(errorCode, "Server error", 400, "query-id");

        SQLException exception = ExceptionUtils.toSqlState(cause);

        assertEquals(exception.getSQLState(), expectedSqlState);
        assertEquals(exception.getErrorCode(), errorCode);
        assertSame(exception.getCause(), cause);
    }
}
