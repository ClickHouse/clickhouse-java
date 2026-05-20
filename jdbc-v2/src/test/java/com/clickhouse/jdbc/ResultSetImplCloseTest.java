package com.clickhouse.jdbc;

import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for ResultSetImpl.isStreamDrainException — the classifier that
 * decides whether a close-time exception is a benign chunked-stream drain
 * failure (to swallow) or a real error (to propagate).
 *
 * Background: see issue #2361. When the server's send_timeout fires mid-write,
 * Apache HC's ChunkedInputStream.close() trips
 * `ConnectionClosedException: Premature end of chunk coded message body:
 * closing chunk expected`. That happens during ResultSetImpl.close(), AFTER
 * the application has finished iterating. Propagating it punishes well-behaved
 * try-with-resources callers for a server-side socket race they cannot affect.
 */
public class ResultSetImplCloseTest {

    @Test
    public void prematureEndOfChunkIsDrainException() {
        // The canonical surface from issue #2361.
        Exception e = new IOException("Premature end of chunk coded message body: closing chunk expected");
        assertTrue(ResultSetImpl.isStreamDrainException(e));
    }

    @Test
    public void closingChunkExpectedIsDrainException() {
        // Forward-compat for variations of the same message.
        Exception e = new IOException("...closing chunk expected");
        assertTrue(ResultSetImpl.isStreamDrainException(e));
    }

    @Test
    public void wrappedCausesAreUnwrapped() {
        // Stack from the actual JDBC trace:
        //   SQLException -> ClientException -> ConnectionClosedException
        Throwable root = new FakeConnectionClosedException("Premature end of chunk coded message body: closing chunk expected");
        RuntimeException mid = new RuntimeException("Failed to close response", root);
        Exception top = new Exception("wrapped", mid);
        assertTrue(ResultSetImpl.isStreamDrainException(top));
    }

    @Test
    public void classNameMatchEvenWithoutMessage() {
        // Defensive: if a future HC version drops the descriptive message but
        // keeps the exception class, we still want to classify it as drain noise.
        Throwable e = new FakeConnectionClosedException(null);
        assertTrue(ResultSetImpl.isStreamDrainException(e));
    }

    @Test
    public void unrelatedExceptionsArePropagated() {
        assertFalse(ResultSetImpl.isStreamDrainException(new IOException("disk full")));
        assertFalse(ResultSetImpl.isStreamDrainException(new IllegalStateException("bad state")));
        assertFalse(ResultSetImpl.isStreamDrainException(new RuntimeException("anything else")));
    }

    @Test
    public void nullThrowableHandled() {
        assertFalse(ResultSetImpl.isStreamDrainException(null));
    }

    /** Mimics org.apache.hc.core5.http.ConnectionClosedException for the class-name match. */
    private static final class FakeConnectionClosedException extends IOException {
        FakeConnectionClosedException(String msg) { super(msg); }
    }
}
