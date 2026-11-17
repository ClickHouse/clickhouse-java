package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.DataTransferException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.SocketTimeoutException;

public class StatementImplTest {

    @Test
    public void detectsNestedSocketTimeout() {
        SocketTimeoutException timeout = new SocketTimeoutException("read timed out");
        DataTransferException transfer = new DataTransferException("incomplete compressed block", timeout);
        ClientException readFailure = new ClientException("failed to read value", transfer);

        Assert.assertTrue(StatementImpl.hasCause(readFailure, SocketTimeoutException.class));
    }

    @Test
    public void ignoresOtherNestedFailures() {
        ClientException readFailure = new ClientException(
                "failed to read value", new DataTransferException("corrupted compressed block"));

        Assert.assertFalse(StatementImpl.hasCause(readFailure, SocketTimeoutException.class));
    }
}
