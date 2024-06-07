package com.clickhouse.client;

import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutionException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseExceptionTest {
    @Test(groups = { "unit" })
    public void testConstructorWithCause() {
        ClickHouseException e = new ClickHouseException(-1, (Throwable) null, null);
        Assert.assertEquals(e.getErrorCode(), -1);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error -1");

        ClickHouseNode server = ClickHouseNode.builder().build();
        e = new ClickHouseException(233, (Throwable) null, server);
        Assert.assertEquals(e.getErrorCode(), 233);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error 233");
        Assert.assertEquals(e.getServer(), server);

        Throwable cause = new IllegalArgumentException();
        e = new ClickHouseException(123, cause, server);
        Assert.assertEquals(e.getErrorCode(), 123);
        Assert.assertEquals(e.getCause(), cause);
        Assert.assertEquals(e.getMessage(), "Unknown error 123");
        Assert.assertEquals(e.getServer(), server);

        cause = new IllegalArgumentException("Some error");
        e = new ClickHouseException(111, cause, server);
        Assert.assertEquals(e.getErrorCode(), 111);
        Assert.assertEquals(e.getCause(), cause);
        Assert.assertEquals(e.getMessage(), "Some error");
        Assert.assertEquals(e.getServer(), server);
    }

    @Test(groups = { "unit" })
    public void testConstructorWithoutCause() {
        ClickHouseException e = new ClickHouseException(-1, (String) null, (ClickHouseNode) null);
        Assert.assertEquals(e.getErrorCode(), -1);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error -1");

        ClickHouseNode server = ClickHouseNode.builder().build();
        e = new ClickHouseException(233, (String) null, server);
        Assert.assertEquals(e.getErrorCode(), 233);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error 233");
        Assert.assertEquals(e.getServer(), server);

        e = new ClickHouseException(123, "", server);
        Assert.assertEquals(e.getErrorCode(), 123);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error 123");
        Assert.assertEquals(e.getServer(), server);

        e = new ClickHouseException(111, "Some error", server);
        Assert.assertEquals(e.getErrorCode(), 111);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Some error");
        Assert.assertEquals(e.getServer(), server);
    }

    @Test(groups = { "unit" })
    public void testHandleException() {
        ClickHouseNode server = ClickHouseNode.builder().build();
        Throwable cause = new RuntimeException();
        ClickHouseException e = ClickHouseException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), ClickHouseException.ERROR_UNKNOWN);
        Assert.assertEquals(e.getCause(), cause);
        Assert.assertEquals(e.getMessage(),
                "Unknown error " + ClickHouseException.ERROR_UNKNOWN);

        e = ClickHouseException.of("Some error", server);
        Assert.assertEquals(e.getErrorCode(), ClickHouseException.ERROR_UNKNOWN);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Some error");

        Assert.assertEquals(e, ClickHouseException.of(e, server));

        cause = new ExecutionException(null);
        e = ClickHouseException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), ClickHouseException.ERROR_UNKNOWN);
        Assert.assertEquals(e.getCause(), cause);
        Assert.assertEquals(e.getMessage(),
                "Unknown error " + ClickHouseException.ERROR_UNKNOWN);

        e = ClickHouseException.of((ExecutionException) null, server);
        Assert.assertEquals(e.getErrorCode(), ClickHouseException.ERROR_UNKNOWN);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(),
                "Unknown error " + ClickHouseException.ERROR_UNKNOWN);

        cause = new ExecutionException(new ClickHouseException(-100, (Throwable) null, server));
        e = ClickHouseException.of(cause, server);
        Assert.assertEquals(e, cause.getCause());
        Assert.assertEquals(e.getErrorCode(), -100);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error -100");

        cause = new ExecutionException(new IllegalArgumentException());
        e = ClickHouseException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), ClickHouseException.ERROR_UNKNOWN);
        Assert.assertEquals(e.getCause(), cause.getCause());
        Assert.assertEquals(e.getMessage(),
                "Unknown error " + ClickHouseException.ERROR_UNKNOWN);

        cause = new ExecutionException(new IllegalArgumentException("Code: 12345. Something goes wrong..."));
        e = ClickHouseException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), 12345);
        Assert.assertEquals(e.getCause(), cause.getCause());
        Assert.assertEquals(e.getMessage(), cause.getCause().getMessage());

        cause = new ExecutionException(new IllegalArgumentException("Code:12345. Something goes wrong..."));
        e = ClickHouseException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), 12345);
        Assert.assertEquals(e.getCause(), cause.getCause());
        Assert.assertEquals(e.getMessage(), cause.getCause().getMessage());

        cause = new ExecutionException(new IllegalArgumentException("Poco::Exception. Code: 1000, "));
        e = ClickHouseException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), 1000);
        Assert.assertEquals(e.getCause(), cause.getCause());
        Assert.assertEquals(e.getMessage(), cause.getCause().getMessage());

        cause = new ExecutionException(new IllegalArgumentException("Code:       123"));
        e = ClickHouseException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), 123);
        Assert.assertEquals(e.getCause(), cause.getCause());
        Assert.assertEquals(e.getMessage(), cause.getCause().getMessage());

        cause = new ExecutionException(new IllegalArgumentException("Code: ab123"));
        e = ClickHouseException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), 1002);
        Assert.assertEquals(e.getCause(), cause.getCause());
        Assert.assertEquals(e.getMessage(), cause.getCause().getMessage());

    }

    @Test(groups = { "unit" })
    public void testNetworkException() {
        Assert.assertTrue(ClickHouseException.isConnectTimedOut(new SocketTimeoutException("Connect timed out")));
        Assert.assertTrue(ClickHouseException.isConnectTimedOut(new SocketTimeoutException("connect timed out")));
        Assert.assertTrue(ClickHouseException.isConnectTimedOut(new SocketTimeoutException("Connect timed out ")));
    }
}
