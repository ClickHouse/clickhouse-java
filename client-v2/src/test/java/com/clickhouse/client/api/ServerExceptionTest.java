package com.clickhouse.client.api;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ServerExceptionTest {

    @DataProvider(name = "retryableCodes")
    public static Object[][] retryableCodes() {
        return new Object[][] {
                // Execution timeout means the server already spent the whole time budget on the query,
                // so retrying it can only spend it again.
                {159, "TIMEOUT_EXCEEDED", false},
                {60, "UNKNOWN_TABLE", false},
                {62, "SYNTAX_ERROR", false},
                {241, "MEMORY_LIMIT_EXCEEDED", true},
                {209, "SOCKET_TIMEOUT", true},
                {210, "NETWORK_ERROR", true},
                {999, "KEEPER_EXCEPTION", true},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "retryableCodes")
    public void testIsRetryable(int code, String codeName, boolean expected) {
        ServerException exception = new ServerException(code, "DB::Exception: " + codeName, 500, "query-id");

        Assert.assertEquals(exception.isRetryable(), expected,
                "Unexpected retryability for code " + code + " (" + codeName + ")");
        Assert.assertEquals(exception.getCode(), code);
    }
}
