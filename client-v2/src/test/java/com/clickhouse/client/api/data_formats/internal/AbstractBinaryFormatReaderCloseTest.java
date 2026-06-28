package com.clickhouse.client.api.data_formats.internal;

import org.apache.hc.core5.http.ConnectionClosedException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

/**
 * Verifies that the reader's close path recognises Apache HC's
 * ConnectionClosedException (the "Premature end of chunk coded message body"
 * surface that fires when the server tore the connection down before writing
 * the terminating zero-length chunk) and is willing to swallow it on close.
 * Matched by class-name suffix so the recogniser works against both the
 * directly-referenced and the shaded copy of the HC class.
 */
@Test(groups = {"unit"})
public class AbstractBinaryFormatReaderCloseTest {

    @DataProvider(name = "cases")
    public Object[][] cases() {
        return new Object[][] {
                { new ConnectionClosedException("Premature end of chunk coded message body: closing chunk expected"), true },
                { new IOException("close failed", new ConnectionClosedException("closing chunk expected")), true },
                { new IOException("disk full"), false },
                { null, false },
        };
    }

    @Test(dataProvider = "cases")
    public void recognisesHcConnectionClosed(Throwable t, boolean expected) {
        assertEquals(AbstractBinaryFormatReader.isConnectionClosedException(t), expected);
    }
}
