package com.clickhouse.client.api.internal;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.Assert;
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
}
